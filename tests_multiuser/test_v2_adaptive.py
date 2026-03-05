import json
import os
import sqlite3
from pathlib import Path

import adaptive_analytics as analytics
import adaptive_tasks
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _make_user(tmp_path: Path, user_name: str = "xu", user_id: int = 10001) -> UserContext:
    user_dir = tmp_path / "users" / user_name
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": user_name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": user_id},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    return UserContext(str(user_dir))


def test_user_context_initializes_tasks_json_and_runtime(tmp_path):
    ctx = _make_user(tmp_path)
    tasks_path = Path(ctx.user_dir) / "tasks.json"
    assert tasks_path.exists()
    assert ctx.get_tasks() == []

    rt = ctx.state.runtime
    assert rt.get("current_task_run_id", "") == ""
    assert rt.get("task_auto_enabled", True) is True
    assert rt.get("run_mode", "normal") == "normal"
    assert rt.get("task_mode_state", "idle") == "idle"


def test_adaptive_task_create_and_manual_run(tmp_path):
    ctx = _make_user(tmp_path, user_name="task_user", user_id=10002)
    ctx.state.history = [0, 1] * 30

    created = adaptive_tasks.create_task(ctx, "daily_mix", cron_minutes=15, mode="hybrid")
    assert created["ok"] is True
    assert len(adaptive_tasks.list_tasks(ctx)) == 1

    started = adaptive_tasks.start_task(ctx, "daily_mix", trigger_type="manual")
    assert started["ok"] is True

    rt = ctx.state.runtime
    assert rt.get("current_task_name") == "daily_mix"
    assert rt.get("current_task_run_id", "") != ""
    assert rt.get("current_preset_name", "") != ""


def test_adaptive_task_on_settle_triggers_risk_pause_without_ending_task(tmp_path):
    ctx = _make_user(tmp_path, user_name="risk_user", user_id=10003)
    ctx.state.history = [0, 1] * 40

    adaptive_tasks.create_task(ctx, "risk_cut", cron_minutes=0, mode="regime")
    started = adaptive_tasks.start_task(ctx, "risk_cut", trigger_type="manual")
    assert started["ok"] is True

    ctx.state.runtime["task_run_loss_limit"] = 100
    update = adaptive_tasks.on_settle(ctx, win=False, profit=-120)
    assert update.get("active") is True
    assert update.get("risk_paused") is True
    assert update.get("status") == "loss_stop"
    assert ctx.state.runtime.get("current_task_run_id", "") != ""
    assert ctx.state.runtime.get("task_mode_state", "idle") == "paused_risk"
    assert str(ctx.state.runtime.get("task_freeze_until", "")).strip() == ""


def test_adaptive_task_light_risk_off_does_not_pause(tmp_path):
    ctx = _make_user(tmp_path, user_name="light_off_user", user_id=10009)
    ctx.state.history = [0, 1] * 40

    adaptive_tasks.create_task(ctx, "risk_cut", cron_minutes=0, mode="regime")
    started = adaptive_tasks.start_task(ctx, "risk_cut", trigger_type="manual")
    assert started["ok"] is True

    ctx.state.runtime["task_run_loss_limit"] = 100
    ctx.state.runtime["risk_light_enabled"] = False
    update = adaptive_tasks.on_settle(ctx, win=False, profit=-120)
    assert update.get("risk_paused") is not True
    assert ctx.state.runtime.get("current_task_run_id", "") != ""
    assert ctx.state.runtime.get("task_mode_state", "idle") == "running"


def test_adaptive_task_risk_pause_blocks_auto_trigger(tmp_path):
    ctx = _make_user(tmp_path, user_name="risk_pause_user", user_id=10004)
    ctx.state.history = [0, 1] * 40
    adaptive_tasks.create_task(ctx, "auto_mix", cron_minutes=1, mode="hybrid")

    ctx.state.runtime["task_mode_state"] = "paused_risk"
    blocked = adaptive_tasks.maybe_trigger_task(ctx)
    assert blocked.get("ok") is False
    assert blocked.get("reason") == "paused_risk"


def test_adaptive_task_manual_pause_blocks_auto_trigger(tmp_path):
    ctx = _make_user(tmp_path, user_name="manual_pause_user", user_id=10006)
    ctx.state.history = [0, 1] * 40
    adaptive_tasks.create_task(ctx, "auto_mix", cron_minutes=1, mode="hybrid")
    ctx.state.runtime["task_mode_state"] = "paused_manual"

    blocked = adaptive_tasks.maybe_trigger_task(ctx)
    assert blocked.get("ok") is False
    assert blocked.get("reason") == "paused_manual"


def test_adaptive_task_create_with_custom_risk_params(tmp_path):
    ctx = _make_user(tmp_path, user_name="custom_risk_user", user_id=10007)
    created = adaptive_tasks.create_task(
        ctx,
        "custom_task",
        cron_minutes=10,
        mode="hybrid",
        task_loss_pct=0.01,
        daily_loss_pct=0.03,
        max_consecutive_losses=6,
    )
    assert created.get("ok") is True
    task = adaptive_tasks.list_tasks(ctx)[0]
    assert float(task.get("task_loss_pct", 0.0)) == 0.01
    assert float(task.get("daily_loss_pct", 0.0)) == 0.03
    assert int(task.get("max_consecutive_losses", 0)) == 6


def test_regime_text_outputs_chinese_labels():
    assert adaptive_tasks.regime_text("CHAOS") == "混沌震荡"
    assert adaptive_tasks.regime_text("RANGE") == "区间震荡"
    assert "共5类" in adaptive_tasks.regime_catalog_text()


def test_format_task_cycle_report_contains_core_fields(tmp_path):
    ctx = _make_user(tmp_path, user_name="cycle_report_user", user_id=10008)
    ctx.state.history = [0, 1] * 40
    assert adaptive_tasks.create_task(ctx, "cycle_task", cron_minutes=10, mode="hybrid")["ok"] is True
    assert adaptive_tasks.start_task(ctx, "cycle_task", trigger_type="manual")["ok"] is True
    ctx.state.runtime["task_run_total_rounds"] = 40
    ctx.state.runtime["task_run_pnl"] = 1234
    ctx.state.runtime["task_run_max_dd"] = 500

    mes = adaptive_tasks.format_task_cycle_report(ctx)
    assert "任务运行简报（每40局）" in mes
    assert "累计局数：40" in mes
    assert "累计收益：1234" in mes


def test_analytics_ingest_idempotent(tmp_path):
    ctx = _make_user(tmp_path, user_name="analytics_user", user_id=10005)
    user_dir = Path(ctx.user_dir)

    replay_rows = [
        {
            "timestamp": "2026-03-05 10:00:00",
            "event_type": "decision",
            "user_id": 10005,
            "account_name": "analytics_user",
            "payload": {
                "decision_id": "dec_1",
                "round": 1,
                "mode": "M-SMP",
                "source": "model",
                "tag": "RANGE",
                "confidence": 70,
                "prediction": 1,
                "model_id": "m1",
                "reason": "r1",
            },
        },
        {
            "timestamp": "2026-03-05 10:00:02",
            "event_type": "bet_placed",
            "user_id": 10005,
            "account_name": "analytics_user",
            "payload": {
                "bet_id": "b1",
                "round": 1,
                "sequence": 1,
                "direction": "big",
                "amount": 1000,
                "decision_id": "dec_1",
                "preset": "yc1",
                "regime": "RANGE",
                "task_name": "t1",
                "task_run_id": "run1",
            },
        },
        {
            "timestamp": "2026-03-05 10:00:05",
            "event_type": "bet_settled",
            "user_id": 10005,
            "account_name": "analytics_user",
            "payload": {
                "bet_id": "b1",
                "result": "赢",
                "profit": 990,
                "settle_result_num": 12,
                "settle_result_type": "大",
                "history_index": 0,
                "decision_id": "dec_1",
                "preset": "yc1",
                "regime": "RANGE",
                "task_name": "t1",
                "task_run_id": "run1",
            },
        },
    ]

    _write_jsonl(user_dir / "replay_events.log", replay_rows)
    _write_jsonl(
        user_dir / "decisions.log",
        [
            {
                "decision_id": "dec_1",
                "timestamp": "2026-03-05 10:00:00",
                "round": 1,
                "mode": "M-SMP",
                "prediction_source": "model",
                "pattern_tag": "RANGE",
                "model_id": "m1",
                "output": {"prediction": 1, "confidence": 70, "reason": "r1"},
            }
        ],
    )

    first = analytics.ingest_user_history(ctx)
    second = analytics.ingest_user_history(ctx)

    assert first["ok"] is True
    assert second["ok"] is True
    assert first["decision_rows"] == second["decision_rows"]
    assert first["bet_rows"] == second["bet_rows"]
    assert first["settle_rows"] == second["settle_rows"]

    db_path = analytics.get_db_path(ctx.user_dir)
    conn = sqlite3.connect(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) FROM settle_events").fetchone()[0]
    finally:
        conn.close()
    assert total == 1

    coverage = analytics.linkage_coverage_report(ctx)
    assert coverage["coverage_pct"] == 100.0
