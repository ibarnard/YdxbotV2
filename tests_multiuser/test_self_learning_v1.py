import asyncio
import json
import sqlite3
from pathlib import Path

import history_analysis
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


def _table_row(db_path: Path, sql: str):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(sql).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _seed_learning_eval_analytics(ctx):
    db_path = Path(ctx.user_dir) / "analytics.db"
    conn = sqlite3.connect(str(db_path))
    try:
        history_analysis._ensure_analytics_schema(conn)
        dataset = [
            ("rk_eval_1", history_analysis.REGIME_CHAOS, "big", "yc50", -10000, 0, "big"),
            ("rk_eval_2", history_analysis.REGIME_CHAOS, "small", "yc50", -12000, 0, "small"),
            ("rk_eval_3", history_analysis.REGIME_REVERSAL, "big", "yc20", -6000, 0, "big"),
            ("rk_eval_4", history_analysis.REGIME_CONTINUATION, "big", "yc5", 4000, 1, "big"),
            ("rk_eval_5", history_analysis.REGIME_CONTINUATION, "small", "yc5", 3200, 1, "small"),
            ("rk_eval_6", history_analysis.REGIME_RANGE, "big", "yc10", -2000, 0, "big"),
        ]
        for idx, (round_key, regime, result_side, preset_name, profit, is_win, direction_code) in enumerate(dataset):
            ts = f"2026-03-07 0{idx + 1}:10:00"
            result_num = 1 if result_side == "big" else 0
            bet_amount = {"yc5": 5000, "yc10": 10000, "yc20": 20000, "yc50": 50000}.get(preset_name, 5000)
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
                    "大大小小大大大小小大大大小小大大大小小大",
                    "大大小小大大大小小大大大小小大大大小小大大大小小大大大小小大大大小小大",
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
                    0.4,
                    0.45,
                    0.55,
                    0.55,
                    0.55,
                    2,
                    result_side,
                    4,
                    70 if regime == history_analysis.REGIME_CONTINUATION else 35,
                    82 if regime == history_analysis.REGIME_CHAOS else 42,
                    72 if regime == history_analysis.REGIME_REVERSAL else 20,
                    regime,
                ),
            )
            conn.execute(
                """
                INSERT INTO decisions (
                    decision_id, round_key, decision_time, mode, model_id, prediction, direction_code,
                    direction_text, confidence, source, pattern_tag, reason_text, input_payload_json,
                    output_json, is_observe, is_fallback, policy_id, policy_version, policy_mode, policy_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"dec_eval_{idx}",
                    round_key,
                    ts,
                    "M-SMP",
                    "mock-model",
                    1 if direction_code == "big" else 0,
                    direction_code,
                    "大" if direction_code == "big" else "小",
                    78,
                    "model",
                    "TREND",
                    "测试决策",
                    "{}",
                    "{}",
                    0,
                    0,
                    "pol_demo_main",
                    "v2",
                    "gray",
                    "测试策略",
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
                    f"exec_eval_{idx}",
                    round_key,
                    f"dec_eval_{idx}",
                    f"bet_eval_{idx}",
                    "bet",
                    "下注",
                    "",
                    preset_name,
                    bet_amount,
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
                    f"stl_eval_{idx}",
                    round_key,
                    f"dec_eval_{idx}",
                    f"bet_eval_{idx}",
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


def test_evaluate_learning_candidate_offline(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9805)
    _seed_learning_eval_analytics(ctx)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )
    generated = self_learning_engine.generate_candidates_from_evidence(ctx)
    assert generated["created_count"] >= 1

    result = self_learning_engine.evaluate_candidate_offline(ctx, "c1")

    assert result["ok"] is True
    assert result["evaluation"]["candidate_version"] == "c1"
    assert result["metrics"]["sample_size"] >= 6
    assert "离线评估" in result["message"]

    center_path = Path(ctx.user_dir) / "learning_center.json"
    payload = json.loads(center_path.read_text(encoding="utf-8"))
    candidate = payload["candidates"][0]
    assert candidate["status"] == self_learning_engine.LEARNING_STATUS_EVALUATED
    assert candidate["last_evaluation_status"] in {
        self_learning_engine.LEARNING_EVAL_PASS,
        self_learning_engine.LEARNING_EVAL_WATCH,
        self_learning_engine.LEARNING_EVAL_FAIL,
    }

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _table_count(db_path, "learning_evaluations") >= 1
    row = _table_row(db_path, "SELECT candidate_version, status, sample_size FROM learning_evaluations LIMIT 1")
    assert row["candidate_version"] == "c1"
    assert int(row["sample_size"]) >= 6


def test_process_user_command_learn(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9804)
    _seed_learning_eval_analytics(ctx)
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
    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn eval c1", "chat_id": 1, "id": 4})(),
            ctx,
            {},
        )
    )

    assert any("已生成学习候选" in text for text in sent_messages)
    assert any("学习候选列表" in text for text in sent_messages)
    assert any("学习候选详情" in text for text in sent_messages)
    assert any("学习候选离线评估" in text for text in sent_messages)


def test_activate_shadow_and_record_learning_shadow(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9806)
    _seed_learning_eval_analytics(ctx)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )
    generated = self_learning_engine.generate_candidates_from_evidence(ctx)
    assert generated["created_count"] >= 1

    center = self_learning_engine.load_learning_center(ctx)
    candidate = center["candidates"][0]
    candidate["status"] = self_learning_engine.LEARNING_STATUS_EVALUATED
    candidate["last_evaluation_status"] = self_learning_engine.LEARNING_EVAL_WATCH
    candidate["last_score_total"] = 55.0
    self_learning_engine._write_learning_center(ctx, center)

    activated = self_learning_engine.activate_candidate_shadow(ctx, candidate["candidate_version"])
    assert activated["ok"] is True

    recorded = self_learning_engine.record_active_shadow_round(ctx, "rk_eval_1", 1, "大")
    assert recorded["ok"] is True
    assert recorded["recorded"] is True
    assert recorded["shadow"]["diff_type"] in {
        "same",
        "observe_vs_bet",
        "tier_more_conservative",
        "tier_more_aggressive",
        "direction_diff",
    }

    center_after = self_learning_engine.load_learning_center(ctx)
    candidate_after = center_after["candidates"][0]
    assert center_after["active_shadow_candidate_id"] == candidate["candidate_id"]
    assert candidate_after["status"] == self_learning_engine.LEARNING_STATUS_SHADOW
    assert int(candidate_after["last_shadow_sample_size"]) >= 1
    assert candidate_after["last_shadow_status"] in {
        self_learning_engine.LEARNING_SHADOW_PASS,
        self_learning_engine.LEARNING_SHADOW_WATCH,
        self_learning_engine.LEARNING_SHADOW_FAIL,
    }

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _table_count(db_path, "learning_shadows") >= 1
    row = _table_row(db_path, "SELECT candidate_version, diff_type FROM learning_shadows LIMIT 1")
    assert row["candidate_version"] == candidate["candidate_version"]
    assert row["diff_type"]

    shadow_text = self_learning_engine.build_learning_shadow_text(ctx)
    assert "学习候选影子验证" in shadow_text
    assert candidate["candidate_version"] in shadow_text


def test_process_user_command_learn_shadow(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9807)
    _seed_learning_eval_analytics(ctx)
    monkeypatch.setattr(
        self_learning_engine.policy_engine,
        "build_policy_prompt_context",
        lambda user_ctx, analysis_snapshot=None: _sample_policy_context(),
    )
    generated = self_learning_engine.generate_candidates_from_evidence(ctx)
    assert generated["created_count"] >= 1

    center = self_learning_engine.load_learning_center(ctx)
    center["candidates"][0]["status"] = self_learning_engine.LEARNING_STATUS_EVALUATED
    center["candidates"][0]["last_evaluation_status"] = self_learning_engine.LEARNING_EVAL_WATCH
    center["candidates"][0]["last_score_total"] = 54.0
    self_learning_engine._write_learning_center(ctx, center)

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn shadow c1 on", "chat_id": 1, "id": 1})(),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn shadow", "chat_id": 1, "id": 2})(),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            type("E", (), {"raw_text": "learn shadow off", "chat_id": 1, "id": 3})(),
            ctx,
            {},
        )
    )

    assert any("已开启影子验证" in text for text in sent_messages)
    assert any("学习候选影子验证" in text for text in sent_messages)
    assert any("已关闭影子验证" in text for text in sent_messages)
