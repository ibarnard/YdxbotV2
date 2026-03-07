import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import history_analysis
import policy_engine
import zq_multiuser as zm
from user_manager import UserContext, clear_registered_user_contexts


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="复盘用户", user_id=9901):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": user_id},
        },
    )
    return UserContext(str(user_dir))


def _make_analytics():
    rounds = {f"r{i}": {"round_key": f"r{i}"} for i in range(1, 10)}
    decisions = {key: {"round_key": key} for key in ["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"]}
    executions_by_key = {
        "r1": {"round_key": "r1", "action_type": "bet", "preset_name": "yc100", "bet_amount": 100000},
        "r2": {"round_key": "r2", "action_type": "bet", "preset_name": "yc50", "bet_amount": 50000},
        "r3": {"round_key": "r3", "action_type": "bet", "preset_name": "yc50", "bet_amount": 50000},
        "r4": {"round_key": "r4", "action_type": "observe", "preset_name": "", "bet_amount": 0},
        "r5": {"round_key": "r5", "action_type": "blocked", "preset_name": "", "bet_amount": 0},
        "r6": {"round_key": "r6", "action_type": "bet", "preset_name": "yc100", "bet_amount": 100000},
        "r8": {"round_key": "r8", "action_type": "bet", "preset_name": "yc50", "bet_amount": 50000},
    }
    settlements_by_key = {
        "r1": {"round_key": "r1", "is_win": 0, "profit": -18000},
        "r2": {"round_key": "r2", "is_win": 0, "profit": -12000},
        "r3": {"round_key": "r3", "is_win": 1, "profit": 2000},
        "r6": {"round_key": "r6", "is_win": 0, "profit": -8000},
    }
    return {
        "rounds_by_key": rounds,
        "regimes_by_key": {key: {"regime_label": history_analysis.REGIME_CHAOS} for key in rounds},
        "decisions_by_key": decisions,
        "executions_by_key": executions_by_key,
        "settlements_by_key": settlements_by_key,
        "risks_by_round": {
            "r4": [{"layer_code": "strategy"}],
            "r5": [],
        },
        "executions": list(executions_by_key.values()),
    }


def test_build_fp_brief_and_gap_brief(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9901)
    ctx.state.runtime["current_fk1_action_text"] = "观望"

    monkeypatch.setattr(
        history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "regime_label": history_analysis.REGIME_CHAOS,
            "recent_temperature": {"level": "cold"},
            "similar_cases": {
                "similar_count": 23,
                "evidence_strength": "weak",
                "recommended_tier_cap": "mid",
            },
        },
    )
    monkeypatch.setattr(
        history_analysis,
        "build_policy_evidence_package",
        lambda user_ctx, analysis_snapshot=None: {
            "overview_24h": {
                "settled_count": 18,
                "win_rate": 0.444,
                "pnl_total": -9000,
                "max_drawdown": 15000,
                "observe_count": 8,
                "blocked_count": 2,
            }
        },
    )
    monkeypatch.setattr(history_analysis, "_recent_analytics_context", lambda user_ctx, hours=24: _make_analytics())
    monkeypatch.setattr(
        history_analysis,
        "_review_learning_status",
        lambda user_ctx: {"active_gray": {"candidate_version": "c2"}},
    )
    monkeypatch.setattr(
        policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: {"policy_version": "v4", "policy_mode": "gray"},
    )

    brief = history_analysis.build_fp_brief(ctx)
    gap = history_analysis.build_fp_gap_brief(ctx)

    assert "🧾 复盘摘要（24h）" in brief
    assert "当前策略：v4 (gray) | 学习 gray c2" in brief
    assert "24h：样本 18 | 胜率 44.4% | 盈亏 -9,000 | 回撤 15,000" in brief
    assert "链路：决策 8/9 | 执行 7/8 | 结算 4/5" in brief
    assert "当前建议：观望" in brief

    assert "🧩 复盘缺口" in gap
    assert "覆盖：盘面 9/9 | 决策 8/9 | 执行 7/8 | 结算 4/5" in gap
    assert "有盘面无决策：1 | 样例 r9" in gap
    assert "有决策无执行：1 | 样例 r7" in gap
    assert "有下注无结算：1 | 样例 r8" in gap
    assert "有阻断无风控记录：1 | 样例 r5" in gap


def test_build_fp_action_report_suggests_manual_checks(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9902)

    monkeypatch.setattr(
        history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "regime_label": history_analysis.REGIME_CHAOS,
            "recent_temperature": {"level": "cold"},
            "similar_cases": {
                "similar_count": 23,
                "evidence_strength": "weak",
                "recommended_tier_cap": "mid",
            },
        },
    )
    monkeypatch.setattr(
        history_analysis,
        "build_policy_evidence_package",
        lambda user_ctx, analysis_snapshot=None: {
            "overview_24h": {
                "settled_count": 18,
                "win_rate": 0.444,
                "pnl_total": -9000,
                "max_drawdown": 15000,
                "observe_count": 8,
                "blocked_count": 2,
            }
        },
    )
    monkeypatch.setattr(history_analysis, "_recent_analytics_context", lambda user_ctx, hours=24: _make_analytics())
    monkeypatch.setattr(
        history_analysis,
        "_review_learning_status",
        lambda user_ctx: {"active_gray": None, "active_shadow": None, "promoted": None},
    )
    monkeypatch.setattr(
        policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: {"policy_version": "v4", "policy_mode": "gray"},
    )

    report = history_analysis.build_fp_action_report(ctx)

    assert "🧭 人工动作建议" in report
    assert "当前策略：v4 (gray)" in report
    assert "先修链路再动策略" in report
    assert "高档位承压" in report
    assert "历史建议偏保守" in report
    assert "观望占比偏高" in report


def test_process_user_command_fp_brief_gap_action_routes(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9903)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.history_analysis, "build_fp_brief", lambda user_ctx: "BRIEF")
    monkeypatch.setattr(zm.history_analysis, "build_fp_gap_brief", lambda user_ctx: "GAPS")
    monkeypatch.setattr(zm.history_analysis, "build_fp_action_report", lambda user_ctx: "ACTION")

    asyncio.run(zm.process_user_command(None, SimpleNamespace(raw_text="fp brief", chat_id=1, id=1), ctx, {}))
    asyncio.run(zm.process_user_command(None, SimpleNamespace(raw_text="fp gaps", chat_id=1, id=2), ctx, {}))
    asyncio.run(zm.process_user_command(None, SimpleNamespace(raw_text="fp action", chat_id=1, id=3), ctx, {}))

    assert sent_messages == ["BRIEF", "GAPS", "ACTION"]
