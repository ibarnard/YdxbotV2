import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import self_learning_engine
import zq_multiuser as zm
from user_manager import UserContext, clear_registered_user_contexts


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="驾驶舱用户", user_id=9951):
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


def test_format_dashboard_builds_admin_cockpit(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9951)
    ctx.state.history = [1, 0] * 20
    ctx.state.runtime.update(
        {
            "bet_on": True,
            "mode": 1,
            "current_model_id": "gpt-test",
            "current_preset_name": "yc10",
            "current_dynamic_base_tier": "yc10",
            "current_dynamic_tier": "yc20",
            "current_round": 128,
            "current_round_key": "rk_9951",
            "current_fk1_action_text": "观望",
            "current_fk1_tier_cap": "mid",
            "account_balance": 930000,
            "balance_status": "success",
            "gambling_fund": 880000,
            "period_profit": 3500,
            "earnings": 100000,
            "lose_count": 2,
            "bet_sequence_count": 3,
            "pending_bet_id": "bet_9951_pending",
            "policy_active_version": "v4",
            "policy_active_mode": "gray",
            "task_current_name": "午盘任务",
            "task_current_progress_bets": 3,
            "task_current_target_bets": 8,
            "task_current_trigger_mode": "auto",
            "package_current_name": "午盘包",
            "total": 20,
            "win_total": 11,
            "initial_amount": 500,
            "lose_stop": 13,
            "explode": 5,
            "stop": 3,
            "lose_once": 3.0,
            "lose_twice": 2.1,
            "lose_three": 2.05,
            "lose_four": 2.0,
        }
    )
    ctx.state.bet_sequence_log = [
        {
            "bet_id": "bet_9951_settled",
            "sequence": 2,
            "direction": "small",
            "amount": 20000,
            "profit": -8000,
            "status": "settled",
            "settled_at": "2026-03-07 12:00:00",
        },
        {
            "bet_id": "bet_9951_pending",
            "sequence": 3,
            "direction": "big",
            "amount": 50000,
            "profit": 0,
            "status": "placed",
            "placed_at": "2026-03-07 12:01:00",
        },
    ]

    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk_9951",
            "current_round_no": 128,
            "regime_label": zm.history_analysis.REGIME_CONTINUATION,
            "recent_temperature": {"level": "cold"},
            "similar_cases": {"similar_count": 21},
        },
    )
    monkeypatch.setattr(
        zm.history_analysis,
        "build_policy_evidence_package",
        lambda user_ctx, analysis_snapshot=None: {
            "overview_24h": {
                "settled_count": 18,
                "win_rate": 0.5,
                "pnl_total": -5000,
                "max_drawdown": 6200,
                "observe_count": 2,
                "blocked_count": 1,
            }
        },
    )
    monkeypatch.setattr(
        zm,
        "get_current_repo_info",
        lambda: {"current_tag": "v0.1.1", "nearest_tag": "v0.1.1", "short_commit": "abcd1234"},
    )

    self_learning_engine._write_learning_center(
        ctx,
        {
            "version": 1,
            "learning_id": "learn_9951",
            "sequence": 2,
            "active_gray_candidate_id": "lc_9951_002",
            "active_shadow_candidate_id": "",
            "promoted_candidate_id": "",
            "candidates": [
                {
                    "candidate_id": "lc_9951_002",
                    "candidate_version": "c2",
                    "status": self_learning_engine.LEARNING_STATUS_GRAY,
                }
            ],
        },
    )

    message = zm.format_dashboard(ctx)

    assert "📍 Admin 驾驶舱" in message
    assert "📊 近 40 盘结果（由近及远）" in message
    assert "脚本：运行中 | 模式：预测 | 模型：gpt-test" in message
    assert "盘面：第 128 盘 | 延续盘 | 温度 偏冷" in message
    assert "手况：待结算 | 第 3 手 | 大 | 50,000 | bet_9951_pending" in message
    assert "上手：输 -8,000 | 第 2 手 | 小 | 2026-03-07 12:00:00" in message
    assert "局面：任务局 | 3/8 手 | 盈亏 +3,500 | 连输 2" in message
    assert "轮次：午盘包 / 午盘任务 | 3/8 手 | auto" in message
    assert "策略：yc10 -> yc20 | policy v4 (gray) | learn gray c2" in message
    assert "24h：样本 18 | 胜率 50.0% | 盈亏 -5,000 | 回撤 6,200 | 观望 2 | 阻断 1" in message


def test_get_bet_status_text_shows_pause_reason_and_remaining_rounds():
    rt = {
        "manual_pause": False,
        "switch": True,
        "bet_on": False,
        "stop_count": 3,
        "pause_countdown_active": True,
        "pause_countdown_total_rounds": 2,
        "pause_countdown_last_remaining": 2,
        "pause_countdown_reason": "深度风控暂停（3连输档）",
    }

    status = zm.get_bet_status_text(rt)
    assert status == "自动暂停（剩2局，深度风控暂停（3连输档））"


def test_process_user_command_dashboard_alias_refreshes_cockpit(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9952)
    refreshed = []

    def fake_create_task(coro):
        coro.close()
        return None

    async def fake_refresh(client, user_ctx, global_config):
        refreshed.append(user_ctx.user_id)
        return SimpleNamespace(chat_id=1, id=2)

    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(zm, "_refresh_admin_dashboard", fake_refresh)

    asyncio.run(
        zm.process_user_command(
            None,
            SimpleNamespace(raw_text="dashboard", chat_id=1, id=1),
            ctx,
            {},
        )
    )

    assert refreshed == [ctx.user_id]
