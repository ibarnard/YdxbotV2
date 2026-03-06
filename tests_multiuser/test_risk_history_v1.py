import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import history_analysis
import risk_control
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="测试账号", user_id=9001):
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


def _build_sample_history():
    return [1, 1, 1, 0, 1, 1, 1, 0] * 10


def _append_settled_entries(ctx):
    ctx.state.bet_sequence_log = [
        {
            "bet_id": "b1",
            "amount": 5000,
            "profit": 3200,
            "status": "settled",
            "settled_at": "2026-03-06 10:00:00",
            "settle_history_index": 19,
        },
        {
            "bet_id": "b2",
            "amount": 5000,
            "profit": 2800,
            "status": "settled",
            "settled_at": "2026-03-06 11:00:00",
            "settle_history_index": 39,
        },
        {
            "bet_id": "b3",
            "amount": 50000,
            "profit": -18000,
            "status": "settled",
            "settled_at": "2026-03-06 12:00:00",
            "settle_history_index": 59,
        },
    ]


def _table_count(db_path: Path, table_name: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def _seed_fp_analytics(ctx):
    db_path = Path(ctx.user_dir) / "analytics.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        history_analysis._ensure_analytics_schema(conn)
        now = datetime.now()
        dataset = [
            {
                "round_key": "rk_fp_1",
                "regime": history_analysis.REGIME_CONTINUATION,
                "result_side": "big",
                "direction": "big",
                "is_observe": 0,
                "execution_type": "bet",
                "blocked_by": "",
                "preset_name": "yc10",
                "bet_amount": 10000,
                "bet_hand_index": 1,
                "profit": 3000,
            },
            {
                "round_key": "rk_fp_2",
                "regime": history_analysis.REGIME_EXHAUSTION,
                "result_side": "small",
                "direction": "small",
                "is_observe": 0,
                "execution_type": "bet",
                "blocked_by": "",
                "preset_name": "yc5",
                "bet_amount": 5000,
                "bet_hand_index": 2,
                "profit": 1200,
                "risk_action": "cap",
                "risk_tier_cap": "yc5",
            },
            {
                "round_key": "rk_fp_3",
                "regime": history_analysis.REGIME_REVERSAL,
                "result_side": "big",
                "direction": "small",
                "is_observe": 0,
                "execution_type": "bet",
                "blocked_by": "",
                "preset_name": "yc1",
                "bet_amount": 1000,
                "bet_hand_index": 3,
                "profit": -800,
            },
            {
                "round_key": "rk_fp_4",
                "regime": history_analysis.REGIME_RANGE,
                "result_side": "small",
                "direction": "small",
                "is_observe": 0,
                "execution_type": "blocked",
                "blocked_by": "fk2",
                "preset_name": "",
                "bet_amount": 0,
                "bet_hand_index": 4,
            },
            {
                "round_key": "rk_fp_5",
                "regime": history_analysis.REGIME_CHAOS,
                "result_side": "big",
                "direction": "observe",
                "is_observe": 1,
                "execution_type": "observe",
                "blocked_by": "",
                "preset_name": "",
                "bet_amount": 0,
                "bet_hand_index": 1,
            },
            {
                "round_key": "rk_fp_6",
                "regime": history_analysis.REGIME_CHAOS,
                "result_side": "small",
                "direction": "big",
                "is_observe": 0,
                "execution_type": "blocked",
                "blocked_by": "fk1",
                "preset_name": "",
                "bet_amount": 0,
                "bet_hand_index": 2,
            },
            {
                "round_key": "rk_fp_7",
                "regime": history_analysis.REGIME_CONTINUATION,
                "result_side": "big",
                "direction": "big",
                "is_observe": 0,
                "execution_type": "blocked",
                "blocked_by": "fund",
                "preset_name": "",
                "bet_amount": 0,
                "bet_hand_index": 1,
            },
            {
                "round_key": "rk_fp_8",
                "regime": history_analysis.REGIME_EXHAUSTION,
                "result_side": "small",
                "direction": "small",
                "is_observe": 0,
                "execution_type": "blocked",
                "blocked_by": "fk3",
                "preset_name": "",
                "bet_amount": 0,
                "bet_hand_index": 5,
            },
            {
                "round_key": "rk_fp_9",
                "regime": history_analysis.REGIME_RANGE,
                "result_side": "big",
                "direction": "big",
                "is_observe": 0,
                "execution_type": "bet",
                "blocked_by": "",
                "preset_name": "yc50",
                "bet_amount": 50000,
                "bet_hand_index": 5,
                "profit": -15000,
            },
        ]

        for index, item in enumerate(dataset):
            ts = (now - timedelta(hours=index)).strftime("%Y-%m-%d %H:%M:%S")
            result_num = 1 if item["result_side"] == "big" else 0
            decision_id = f"dec_fp_{index + 1}"
            bet_id = f"bet_fp_{index + 1}"
            conn.execute(
                """
                INSERT INTO rounds (
                    round_key, user_id, account_name, history_index, issue_no, result_num, result_side,
                    captured_at, board_5, board_10, board_20, board_40, current_round_no, current_hand_no
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["round_key"],
                    ctx.user_id,
                    ctx.config.name,
                    index,
                    "",
                    result_num,
                    item["result_side"],
                    ts,
                    "大大大大小",
                    "大大大小大小大大",
                    "大大小大小大大大小小大大",
                    "大大小大小大大大小小大大小大小大大大小小大大大小大小大大",
                    index + 1,
                    item["bet_hand_index"],
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
                    item["round_key"],
                    "v1",
                    0.25,
                    0.35,
                    0.40,
                    0.45,
                    0.6,
                    0.55,
                    0.55,
                    2,
                    item["result_side"],
                    4,
                    65,
                    35,
                    30,
                    item["regime"],
                ),
            )
            conn.execute(
                """
                INSERT INTO decisions (
                    decision_id, round_key, decision_time, mode, model_id, prediction, direction_code,
                    direction_text, confidence, source, pattern_tag, reason_text, input_payload_json,
                    output_json, is_observe, is_fallback
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    item["round_key"],
                    ts,
                    "M-SMP",
                    "model-x",
                    -1 if item["direction"] == "observe" else (1 if item["direction"] == "big" else 0),
                    item["direction"],
                    "观望" if item["direction"] == "observe" else ("大" if item["direction"] == "big" else "小"),
                    68,
                    "model",
                    "TREND",
                    "seed",
                    "{}",
                    "{}",
                    item["is_observe"],
                    0,
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
                    f"exec_fp_{index + 1}",
                    item["round_key"],
                    decision_id,
                    bet_id if item["execution_type"] == "bet" else "",
                    item["execution_type"],
                    item["execution_type"],
                    item["blocked_by"],
                    item["preset_name"],
                    item["bet_amount"],
                    item["bet_hand_index"],
                    index + 1,
                    "",
                    ts,
                ),
            )
            if item.get("risk_action"):
                conn.execute(
                    """
                    INSERT INTO risk_records (
                        risk_record_id, round_key, decision_id, phase, layer_code, layer_text, enabled,
                        action, tier_cap, pause_rounds, recheck_after, reason_code, reason_text,
                        metrics_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"risk_fp_{index + 1}",
                        item["round_key"],
                        decision_id,
                        "pre_bet",
                        "fk1",
                        "盘面风控",
                        1,
                        item["risk_action"],
                        item.get("risk_tier_cap", ""),
                        0,
                        0,
                        "fk1_cap",
                        "seed",
                        "{}",
                        ts,
                    ),
                )
            if "profit" in item:
                conn.execute(
                    """
                    INSERT INTO settlements (
                        settle_id, round_key, decision_id, bet_id, settled_at, history_index, result_num, result_side,
                        is_win, profit, fund_before, fund_after, balance_before, balance_after,
                        lose_count_before, lose_count_after, streak_label
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"stl_fp_{index + 1}",
                        item["round_key"],
                        decision_id,
                        bet_id,
                        ts,
                        index,
                        result_num,
                        item["result_side"],
                        1 if int(item["profit"]) > 0 else 0,
                        int(item["profit"]),
                        500000,
                        500000 + int(item["profit"]),
                        1000000,
                        1000000 + int(item["profit"]),
                        0,
                        0 if int(item["profit"]) > 0 else 1,
                        "seed",
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def test_build_startup_focus_reminder_uses_fk_labels(tmp_path):
    ctx = _make_user_context(tmp_path)
    rt = ctx.state.runtime
    rt["fk1_enabled"] = True
    rt["fk2_enabled"] = False
    rt["fk3_enabled"] = True
    rt["fk1_default_enabled"] = True
    rt["fk2_default_enabled"] = False
    rt["fk3_default_enabled"] = True
    rt["current_preset_name"] = "yc20"

    message = zm.build_startup_focus_reminder(ctx)

    assert "fk1 盘面" in message
    assert "fk2 入场" in message
    assert "fk3 连输" in message
    assert "st <预设名>" in message


def test_fk_command_toggles_new_switches(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path)
    rt = ctx.state.runtime
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "delete_later", fake_delete_later)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fk 2 off", chat_id=1, id=1),
            ctx,
            {},
        )
    )
    assert rt["fk2_enabled"] is False
    assert rt["fk2_default_enabled"] is False
    assert rt["risk_deep_enabled"] is False

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fk 3 off", chat_id=1, id=2),
            ctx,
            {},
        )
    )
    assert rt["fk3_enabled"] is False
    assert rt["fk3_default_enabled"] is False

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fk 1 on", chat_id=1, id=3),
            ctx,
            {},
        )
    )
    assert rt["fk1_enabled"] is True
    assert rt["fk1_default_enabled"] is True
    assert rt["risk_base_enabled"] is True
    assert any("fk2 入场风控" in message for message in sent_messages)


def test_history_analysis_builds_snapshot_and_fp_messages(tmp_path):
    ctx = _make_user_context(tmp_path)
    ctx.state.history = _build_sample_history()
    _append_settled_entries(ctx)

    snapshot = history_analysis.build_current_analysis_snapshot(ctx)
    overview = history_analysis.build_fp_overview(ctx)
    evidence = history_analysis.build_fp_current_evidence(ctx)

    assert snapshot["round_key"].startswith("rk:")
    assert snapshot["regime_label"] in {
        history_analysis.REGIME_CONTINUATION,
        history_analysis.REGIME_EXHAUSTION,
        history_analysis.REGIME_REVERSAL,
        history_analysis.REGIME_RANGE,
        history_analysis.REGIME_CHAOS,
    }
    assert "24小时复盘总览" in overview
    assert "当前盘面证据" in evidence


def test_evaluate_fk1_caps_when_high_tier_risk():
    snapshot = {
        "regime_label": "延续盘",
        "similar_cases": {
            "similar_count": 48,
            "evidence_strength": "strong",
            "weighted_signal_hit_rate": 0.56,
            "tiers": {
                "low": {"avg_pnl": 2400, "win_rate": 0.58},
                "high": {"avg_pnl": -12800, "win_rate": 0.49},
            },
        },
        "recent_temperature": {"level": "normal"},
    }
    rt = {"fk1_enabled": True, "fk2_enabled": True, "fk3_enabled": True}

    result = risk_control.evaluate_fk1(snapshot, rt)

    assert result["action"] == "cap"
    assert result["tier_cap"] == "yc5"
    assert "高档位回撤偏大" in result["reason_text"]


def test_evaluate_fk1_observes_when_temperature_is_very_cold():
    snapshot = {
        "regime_label": "延续盘",
        "similar_cases": {
            "similar_count": 52,
            "evidence_strength": "strong",
            "weighted_signal_hit_rate": 0.58,
            "tiers": {
                "low": {"avg_pnl": 2000, "win_rate": 0.57},
                "high": {"avg_pnl": 3600, "win_rate": 0.56},
            },
        },
        "recent_temperature": {"level": "very_cold"},
    }
    rt = {"fk1_enabled": True, "fk2_enabled": True, "fk3_enabled": True}

    result = risk_control.evaluate_fk1(snapshot, rt)

    assert result["action"] == "observe"
    assert result["tier_cap"] == ""
    assert "近期实盘很冷" in result["reason_text"]


def test_record_decision_audit_writes_to_analytics_db(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9003)
    ctx.state.history = [1] * 40
    ctx.state.runtime["current_round_key"] = "rk:test:39:1"

    history_analysis.record_decision_audit(
        ctx,
        {
            "decision_id": "dec_test_1",
            "timestamp": "2026-03-06 12:00:00",
            "mode": "M-SMP",
            "model_id": "model-x",
            "prediction_source": "model",
            "pattern_tag": "TREND",
            "input_payload": {"foo": "bar"},
            "output": {"prediction": 1, "confidence": 70, "reason": "趋势明确"},
        },
    )

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert db_path.exists()
    assert _table_count(db_path, "decisions") == 1


def test_process_bet_on_fk1_observe_blocks_without_real_bet(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 50
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 2
    rt["current_preset_name"] = "yc20"
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False

    replay_events = []

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 68
        user_ctx.state.runtime["last_predict_tag"] = "TREND"
        user_ctx.state.runtime["last_predict_info"] = "趋势明确"
        return 1

    async def fake_notice(*args, **kwargs):
        return None

    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict_next_bet_v10)
    monkeypatch.setattr(zm, "_send_transient_admin_notice", fake_notice)
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:test:49:2",
            "regime_label": "混乱盘",
            "features": {},
            "similar_cases": {"similar_count": 41, "evidence_strength": "strong", "tiers": {}},
            "recent_temperature": {"level": "normal"},
        },
    )
    monkeypatch.setattr(
        zm.risk_control,
        "evaluate_fk1",
        lambda snapshot, runtime: {
            "action": "observe",
            "action_text": "盘面风控建议观望，本局不下注",
            "tier_cap": "",
            "reason_text": "当前为混乱盘",
            "regime_label": "混乱盘",
        },
    )
    monkeypatch.setattr(zm, "append_replay_event", lambda user_ctx, event_type, payload: replay_events.append((event_type, payload)))

    event = SimpleNamespace(
        reply_markup=True,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(50))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert rt["last_execution_action"] == "blocked"
    assert rt["last_blocked_by"] == "fk1"
    assert rt["bet"] is False
    assert db_path.exists()
    assert _table_count(db_path, "rounds") == 1
    assert _table_count(db_path, "regime_features") == 1
    assert _table_count(db_path, "risk_records") == 1
    assert _table_count(db_path, "execution_records") == 1
    assert any(event_type == "risk_action" and payload.get("layer") == "fk1" for event_type, payload in replay_events)


def test_process_bet_on_fk1_cap_clamps_real_bet_amount(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9002)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 60
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 3
    rt["current_preset_name"] = "yc20"
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False
    rt["gambling_fund"] = 1_000_000

    sent_messages = []
    replay_events = []
    clicked_buttons = []

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 72
        user_ctx.state.runtime["last_predict_tag"] = "TREND"
        user_ctx.state.runtime["last_predict_info"] = "趋势延续"
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    async def fake_notice(*args, **kwargs):
        return None

    async def fake_click(client, event, user_ctx, button_data):
        clicked_buttons.append(button_data)
        return None

    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict_next_bet_v10)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "delete_later", fake_delete_later)
    monkeypatch.setattr(zm, "_send_transient_admin_notice", fake_notice)
    monkeypatch.setattr(zm, "_click_bet_button_with_recover", fake_click)
    monkeypatch.setattr(
        zm,
        "_read_timing_config",
        lambda global_config: {
            "prompt_wait_sec": 0,
            "predict_timeout_sec": 1,
            "click_interval_sec": 0,
            "click_timeout_sec": 1,
        },
    )
    monkeypatch.setattr(zm, "calculate_bet_amount", lambda runtime: 50000)
    monkeypatch.setattr(zm, "generate_mobile_bet_report", lambda *args, **kwargs: "BET_REPORT")
    monkeypatch.setattr(zm, "append_replay_event", lambda user_ctx, event_type, payload: replay_events.append((event_type, payload)))
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:test:59:3",
            "regime_label": "衰竭盘",
            "features": {},
            "similar_cases": {"similar_count": 45, "evidence_strength": "strong", "tiers": {}},
            "recent_temperature": {"level": "normal"},
        },
    )
    monkeypatch.setattr(
        zm.risk_control,
        "evaluate_fk1",
        lambda snapshot, runtime: {
            "action": "cap",
            "action_text": "盘面风控限档，最高 yc5",
            "tier_cap": "yc5",
            "reason_text": "当前为衰竭盘，相似历史高档位回撤偏大",
            "regime_label": "衰竭盘",
        },
    )
    monkeypatch.setattr(zm.constants, "find_combination", lambda amount, buttons: [amount])
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {5000: "BIG5000"})

    event = SimpleNamespace(
        reply_markup=True,
        chat_id=1,
        id=1,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(60))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert rt["bet"] is True
    assert rt["bet_amount"] == 5000
    assert rt["current_fk1_tier_cap"] == "yc5"
    assert rt["last_execution_action"] == "bet"
    assert db_path.exists()
    assert _table_count(db_path, "rounds") == 1
    assert _table_count(db_path, "regime_features") == 1
    assert _table_count(db_path, "risk_records") == 1
    assert _table_count(db_path, "execution_records") == 1
    assert clicked_buttons == ["BIG5000"]
    assert ctx.state.bet_sequence_log[-1]["amount"] == 5000
    assert ctx.state.bet_sequence_log[-1]["fk1_tier_cap"] == "yc5"
    assert any(event_type == "bet_placed" and payload.get("fk1_tier_cap") == "yc5" for event_type, payload in replay_events)


def test_fp_reports_read_from_analytics_db(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9010)
    _seed_fp_analytics(ctx)

    regime_report = history_analysis.build_fp_regime_report(ctx)
    tier_report = history_analysis.build_fp_tier_report(ctx)
    hand_report = history_analysis.build_fp_hand_report(ctx)
    block_report = history_analysis.build_fp_block_report(ctx)
    linkage_report = history_analysis.build_fp_linkage_report(ctx)

    assert "按盘面复盘（24h）" in regime_report
    assert "延续盘：样本 2" in regime_report
    assert "按档位复盘（24h）" in tier_report
    assert "yc50：使用 1" in tier_report
    assert "按手位复盘（24h）" in hand_report
    assert "第5手+" in hand_report
    assert "观望/阻断复盘（24h）" in block_report
    assert "策略观望：1 次" in block_report
    assert "资金风控：1 次" in block_report
    assert "链路覆盖（24h）" in linkage_report
    assert "决策链路覆盖：9/9" in linkage_report
    assert "真实结算覆盖：4/4" in linkage_report
    assert "缺口5 有阻断无风控记录：4 局" in linkage_report


def test_current_analysis_snapshot_prefers_analytics_history(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9012)
    ctx.state.history = [1] * 40
    _seed_fp_analytics(ctx)

    snapshot = history_analysis.build_current_analysis_snapshot(ctx)

    assert snapshot["regime_label"] == history_analysis.REGIME_CONTINUATION
    assert snapshot["similar_cases"]["similar_count"] > 0


def test_fp_command_routes_to_extended_reports(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9011)
    _seed_fp_analytics(ctx)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fp 1", chat_id=1, id=1),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fp 4", chat_id=1, id=2),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="fp 6", chat_id=1, id=3),
            ctx,
            {},
        )
    )

    assert any("按盘面复盘（24h）" in message for message in sent_messages)
    assert any("观望/阻断复盘（24h）" in message for message in sent_messages)
    assert any("链路覆盖（24h）" in message for message in sent_messages)

def test_fp_current_evidence_contains_action_and_source(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9014)
    ctx.state.history = [1] * 40
    _seed_fp_analytics(ctx)

    evidence = history_analysis.build_fp_current_evidence(ctx)
    linkage = history_analysis.build_fp_linkage_report(ctx)

    assert "当前建议：" in evidence
    assert "来源 analytics.db" in evidence
    assert "策略观望记录覆盖：" in linkage
    assert "补充 有观望无策略记录：1 局" in linkage


def test_process_bet_on_strategy_observe_writes_strategy_risk_record(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9015)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 40
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 1
    rt["current_preset_name"] = "yc10"

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model_skip"
        user_ctx.state.runtime["last_predict_confidence"] = 65
        user_ctx.state.runtime["last_predict_tag"] = "CHAOS"
        user_ctx.state.runtime["last_predict_info"] = "当前证据不足"
        return -1

    async def fake_notice(*args, **kwargs):
        return None

    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict_next_bet_v10)
    monkeypatch.setattr(zm, "_send_transient_admin_notice", fake_notice)
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:strategy:39:1",
            "regime_label": history_analysis.REGIME_CHAOS,
            "features": {},
            "similar_cases": {"similar_count": 18, "evidence_strength": "weak", "tiers": {}, "source": "analytics"},
            "recent_temperature": {"level": "normal"},
        },
    )

    event = SimpleNamespace(
        reply_markup=True,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(40))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert db_path.exists()
    assert rt["last_execution_action"] == "strategy_observe"
    assert _table_count(db_path, "risk_records") == 1
    assert _table_count(db_path, "execution_records") == 1

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT layer_code, action, reason_code FROM risk_records").fetchone()
        assert row == ("strategy", "observe", "strategy_observe")
    finally:
        conn.close()
