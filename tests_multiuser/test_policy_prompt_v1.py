import asyncio
import json
import sqlite3
from pathlib import Path

import history_analysis
import policy_engine
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="策略测试", user_id=9501):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": user_id},
            "ai": {"api_keys": ["test-key"]},
        },
    )
    return UserContext(str(user_dir))


def _db_row(db_path: Path, sql: str):
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(sql).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _seed_policy_analytics(ctx):
    db_path = Path(ctx.user_dir) / "analytics.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        history_analysis._ensure_analytics_schema(conn)
        dataset = [
            ("rk_pol_1", history_analysis.REGIME_CONTINUATION, "big", "yc5", 3200, 1),
            ("rk_pol_2", history_analysis.REGIME_CONTINUATION, "big", "yc5", 2800, 1),
            ("rk_pol_3", history_analysis.REGIME_EXHAUSTION, "small", "yc50", -18000, 0),
            ("rk_pol_4", history_analysis.REGIME_CHAOS, "small", "yc100", -26000, 0),
        ]
        for idx, (round_key, regime, result_side, preset_name, profit, is_win) in enumerate(dataset):
            ts = f"2026-03-07 0{idx + 1}:00:00"
            result_num = 1 if result_side == "big" else 0
            conn.execute(
                """
                INSERT INTO rounds (
                    round_key, user_id, account_name, history_index, issue_no, result_num, result_side,
                    captured_at, board_5, board_10, board_20, board_40, current_round_no, current_hand_no
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_key,
                    ctx.user_id,
                    ctx.config.name,
                    idx,
                    "",
                    result_num,
                    result_side,
                    ts,
                    "大大小小大",
                    "大大小小大大大小小大",
                    "大大小小大大大小小大大大小小大",
                    "大大小小大大大小小大大大小小大大大小小大大大小小大",
                    idx + 1,
                    1,
                ),
            )
            conn.execute(
                """
                INSERT INTO regime_features (
                    round_key, feature_version, w5_switch_rate, w10_switch_rate, w20_switch_rate, w40_switch_rate,
                    w5_big_ratio, w10_big_ratio, w40_big_ratio, tail_streak_len, tail_streak_side,
                    gap_big_small, trend_score, chaos_score, reversal_score, regime_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_key,
                    "v1",
                    0.25,
                    0.35,
                    0.40,
                    0.45,
                    0.55,
                    0.55,
                    0.55,
                    2,
                    result_side,
                    4,
                    66 if regime == history_analysis.REGIME_CONTINUATION else 42,
                    30 if regime == history_analysis.REGIME_CONTINUATION else 80,
                    25 if regime == history_analysis.REGIME_CONTINUATION else 28,
                    regime,
                ),
            )
            conn.execute(
                """
                INSERT INTO execution_records (
                    execution_id, round_key, decision_id, bet_id, action_type, action_text, blocked_by,
                    preset_name, bet_amount, bet_hand_index, current_round_no, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"exec_{idx}",
                    round_key,
                    f"dec_{idx}",
                    f"bet_{idx}",
                    "bet",
                    "下注",
                    "",
                    preset_name,
                    5000 if preset_name == "yc5" else (50000 if preset_name == "yc50" else 100000),
                    1,
                    idx + 1,
                    "",
                    ts,
                ),
            )
            conn.execute(
                """
                INSERT INTO settlements (
                    settle_id, round_key, decision_id, bet_id, settled_at, history_index, result_num,
                    result_side, is_win, profit, fund_before, fund_after, balance_before, balance_after,
                    lose_count_before, lose_count_after, streak_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"stl_{idx}",
                    round_key,
                    f"dec_{idx}",
                    f"bet_{idx}",
                    ts,
                    idx,
                    result_num,
                    result_side,
                    is_win,
                    profit,
                    1_000_000,
                    1_000_000 + profit,
                    2_000_000,
                    2_000_000 + profit,
                    0,
                    1 if not is_win else 0,
                    "normal",
                ),
            )
        conn.commit()
    finally:
        conn.close()


def test_policy_sync_and_rollback(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9502)
    ctx.state.history = [1, 1, 1, 0, 1, 1, 1, 0] * 10
    ctx.state.runtime["current_round"] = 8
    _seed_policy_analytics(ctx)

    result = policy_engine.sync_policy_from_evidence(ctx)

    assert result["ok"] is True
    assert result["changed"] is True
    assert ctx.state.runtime["policy_active_version"] == "v2"
    assert ctx.state.runtime["policy_active_mode"] == "gray"

    rollback = policy_engine.rollback_policy(ctx)
    assert rollback["ok"] is True
    assert ctx.state.runtime["policy_active_version"] == "v1"


def test_record_decision_audit_persists_policy_fields(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9503)
    ctx.state.history = [1] * 60
    ctx.state.runtime["current_round_key"] = "rk:test:59:3"

    history_analysis.record_decision_audit(
        ctx,
        {
            "decision_id": "dec_policy_1",
            "timestamp": "2026-03-07 10:00:00",
            "mode": "M-SMP",
            "model_id": "mock-model",
            "prediction_source": "model",
            "pattern_tag": "TREND",
            "input_payload": {"foo": "bar"},
            "output": {"prediction": 1, "confidence": 81, "reason": "测试原因"},
            "policy_id": "pol_9503_main",
            "policy_version": "v2",
            "policy_mode": "gray",
            "policy_summary": "延续盘 | strong | 限档 yc5",
        },
    )

    row = _db_row(Path(ctx.user_dir) / "analytics.db", "SELECT policy_id, policy_version, policy_mode, policy_summary FROM decisions LIMIT 1")
    assert row["policy_id"] == "pol_9503_main"
    assert row["policy_version"] == "v2"
    assert row["policy_mode"] == "gray"
    assert "限档 yc5" in row["policy_summary"]


def test_predict_next_bet_v10_includes_policy_context(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9504)
    ctx.state.history = [1, 1, 1, 0, 1, 1, 1, 0] * 10
    ctx.state.runtime["current_round"] = 6
    _seed_policy_analytics(ctx)
    policy_engine.sync_policy_from_evidence(ctx)

    captured = {}

    class FakeModelManager:
        async def call_model(self, model_id, messages, temperature=0.1, max_tokens=500):
            captured["messages"] = messages
            return {
                "success": True,
                "content": '{"logic":"顺势","reasoning":"策略回写已生效","confidence":77,"prediction":1}',
            }

    monkeypatch.setattr(ctx, "get_model_manager", lambda: FakeModelManager())

    prediction = asyncio.run(zm.predict_next_bet_v10(ctx, {}, current_round=6))

    assert prediction == 1
    prompt_text = captured["messages"][1]["content"]
    assert "[Policy Overlay]" in prompt_text
    assert "policy_version" not in prompt_text
    assert "当前策略版本" in prompt_text
    assert "结构化证据包" in prompt_text

    row = _db_row(Path(ctx.user_dir) / "analytics.db", "SELECT policy_version, policy_mode FROM decisions ORDER BY decision_time DESC LIMIT 1")
    assert row["policy_version"] == "v2"
    assert row["policy_mode"] == "gray"


def test_process_user_command_policy_sync(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9505)
    ctx.state.history = [1] * 60
    _seed_policy_analytics(ctx)

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    event = type(
        "E",
        (),
        {
            "raw_text": "policy sync",
            "chat_id": 1,
            "id": 1,
        },
    )()

    asyncio.run(zm.process_user_command(None, event, ctx, {}))

    assert sent_messages
    assert "策略版本" in sent_messages[-1]
    assert ctx.state.runtime["policy_active_version"] == "v2"
