import asyncio
import json
import sqlite3
from pathlib import Path

import self_learning_engine
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="学习测试", user_id=9801):
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


def _sample_policy_context():
    return {
        "policy_id": "pol_demo_main",
        "policy_version": "v2",
        "policy_mode": "gray",
        "policy_summary": "当前策略摘要",
        "prompt_fragment": "当前策略基线",
        "evidence_package": {
            "current_regime": "混乱盘",
            "scores": {"trend": 28, "chaos": 84, "reversal": 35},
            "similar_cases": {
                "similar_count": 46,
                "evidence_strength": "strong",
                "weighted_signal_hit_rate": 0.49,
                "recommended_tier_cap": "yc5",
                "source": "analytics",
                "tiers": {
                    "low": {"count": 12, "avg_pnl": 1200, "win_rate": 0.56},
                    "mid": {"count": 8, "avg_pnl": -500, "win_rate": 0.48},
                    "high": {"count": 6, "avg_pnl": -12000, "win_rate": 0.33},
                },
            },
            "recent_temperature": {"level": "cold", "settled_10": 10, "win_rate_10": 0.4, "drawdown_10": 18000},
            "overview_24h": {
                "settled_count": 36,
                "win_rate": 0.48,
                "pnl_total": -18000,
                "max_drawdown": 52000,
                "observe_count": 10,
                "blocked_count": 7,
            },
            "regime_24h": {"best_regime": "延续盘", "worst_regime": "混乱盘", "sample_rounds": 68},
            "tier_24h": {
                "low": {"count": 10, "avg_pnl": 1500, "win_rate": 0.55},
                "mid": {"count": 6, "avg_pnl": 800, "win_rate": 0.52},
                "high": {"count": 4, "avg_pnl": -10000, "win_rate": 0.25},
            },
        },
    }


def _table_count(db_path: Path, table_name: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def test_generate_learning_candidates_persist_center_and_analytics(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9802)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )

    result = self_learning_engine.generate_candidates_from_evidence(ctx)

    assert result["ok"] is True
    assert result["created_count"] >= 2
    assert ctx.state.runtime["learning_candidate_count"] >= 2
    center_path = Path(ctx.user_dir) / "learning_center.json"
    payload = json.loads(center_path.read_text(encoding="utf-8"))
    assert payload["sequence"] >= 2
    assert len(payload["candidates"]) >= 2

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _table_count(db_path, "learning_candidates") >= 2

    result_again = self_learning_engine.generate_candidates_from_evidence(ctx)
    assert result_again["created_count"] == 0
    assert _table_count(db_path, "learning_candidates") >= 2


def test_build_learning_texts(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9803)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )
    self_learning_engine.generate_candidates_from_evidence(ctx)

    overview = self_learning_engine.build_learning_overview_text(ctx)
    listing = self_learning_engine.build_learning_list_text(ctx)
    detail = self_learning_engine.build_learning_detail_text(ctx, "c1")

    assert "受控自学习中心" in overview
    assert "候选总数" in overview
    assert "学习候选列表" in listing
    assert "混乱/偏冷观望加强" in listing or "高档位收紧" in listing
    assert "学习候选详情" in detail
    assert "基于策略" in detail


def test_process_user_command_learn(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9804)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn gen", "chat_id": 1, "id": 1})(),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn list", "chat_id": 1, "id": 2})(),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn show c1", "chat_id": 1, "id": 3})(),
            ctx,
            {},
        )
    )

    assert any("已生成学习候选" in text for text in sent_messages)
    assert any("学习候选列表" in text for text in sent_messages)
    assert any("学习候选详情" in text for text in sent_messages)
