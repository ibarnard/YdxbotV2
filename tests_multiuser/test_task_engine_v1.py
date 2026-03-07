import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import task_engine
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="任务测试", user_id=9301):
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


def _task_run_count(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def test_create_task_and_persist(tmp_path):
    ctx = _make_user_context(tmp_path)

    result = task_engine.create_task(
        ctx,
        name="巡航",
        base_preset="yc10",
        max_bets=10,
        trigger_mode=task_engine.TASK_MODE_MANUAL,
        enabled=False,
    )

    assert result["ok"] is True
    tasks_path = Path(ctx.user_dir) / "tasks.json"
    payload = json.loads(tasks_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert len(payload["tasks"]) == 1
    assert payload["tasks"][0]["name"] == "巡航"


def test_create_task_from_template(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9307)

    tpl_text = task_engine.build_task_template_text()
    assert "任务模板" in tpl_text
    assert "保守巡航" in tpl_text

    result = task_engine.create_task_from_template(ctx, "保守", "模板任务")

    assert result["ok"] is True
    assert ctx.tasks[0]["name"] == "模板任务"
    assert ctx.tasks[0]["base_preset"] == "yc5"
    assert ctx.tasks[0]["trigger_mode"] == task_engine.TASK_MODE_REGIME
    assert ctx.tasks[0]["regimes"] == ["延续盘"]


def test_create_task_from_template_with_overrides(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9309)

    parsed = task_engine.parse_template_new_args(["保守巡航", "模板任务", "preset=yc10", "bets=12", "loss=30000"])
    assert parsed["ok"] is True

    overrides = parsed["overrides"]
    result = task_engine.create_task_from_template(
        ctx,
        parsed["template_name"],
        parsed["task_name"],
        base_preset=overrides["base_preset"],
        max_bets=overrides["max_bets"],
        max_loss=overrides["max_loss"],
    )

    assert result["ok"] is True
    assert ctx.tasks[0]["name"] == "模板任务"
    assert ctx.tasks[0]["base_preset"] == "yc10"
    assert ctx.tasks[0]["max_bets"] == 12
    assert ctx.tasks[0]["max_loss"] == 30000


def test_build_task_overview_text_wraps_command_hints(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9310)

    text = task_engine.build_task_overview_text(ctx)

    assert "任务总数：0 | 已启用：0" in text
    assert "命令：`task tpl` / `task new <模板>` / `task list`" in text
    assert "详情：`task add ...` / `task show <id>` / `task run <id>`" in text
    assert "控制：`task pause <id>` / `task resume <id>` / `task logs [id]` / `task stats [id]`" in text


def test_prepare_task_for_round_starts_regime_task(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9302)
    rt = ctx.state.runtime
    rt["bet_on"] = True
    result = task_engine.create_task(
        ctx,
        name="趋势任务",
        base_preset="yc20",
        max_bets=3,
        trigger_mode=task_engine.TASK_MODE_REGIME,
        regimes=["延续盘"],
        enabled=True,
    )
    assert result["ok"] is True

    plan = task_engine.prepare_task_for_round(ctx, {"regime_label": "延续盘"})

    assert plan["started"] is True
    assert rt["task_current_name"] == "趋势任务"
    assert rt["current_preset_name"] == "yc20"
    assert ctx.tasks[0]["status"] == task_engine.TASK_STATUS_RUNNING


def test_prepare_task_for_round_waits_when_sequence_active(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9303)
    rt = ctx.state.runtime
    rt["bet_sequence_count"] = 2
    result = task_engine.create_task(
        ctx,
        name="等待任务",
        base_preset="yc5",
        max_bets=5,
        trigger_mode=task_engine.TASK_MODE_SCHEDULE,
        interval_minutes=1,
        enabled=True,
    )
    assert result["ok"] is True

    plan = task_engine.prepare_task_for_round(ctx, {"regime_label": "震荡盘"})

    assert plan["active"] is False
    assert "等待接管" in plan["message"]
    assert ctx.tasks[0]["status"] == task_engine.TASK_STATUS_IDLE


def test_task_settlement_finishes_after_target_and_logs(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9304)
    result = task_engine.create_task(
        ctx,
        name="两笔完成",
        base_preset="yc5",
        max_bets=2,
        trigger_mode=task_engine.TASK_MODE_MANUAL,
        enabled=True,
    )
    assert result["ok"] is True
    run_result = task_engine.run_task_now(ctx, result["task"]["task_id"])
    assert run_result["ok"] is True

    settle1 = task_engine.record_settlement(ctx, {"bet_id": "b1", "dynamic_tier": "yc5"}, 1200)
    assert settle1["task_finished"] is False
    settle2 = task_engine.record_settlement(ctx, {"bet_id": "b2", "dynamic_tier": "yc5"}, -800)
    assert settle2["task_finished"] is True
    assert "任务结束" in settle2["summary"]
    assert ctx.tasks[0]["status"] == task_engine.TASK_STATUS_IDLE
    assert ctx.state.runtime["task_current_id"] == ""

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _task_run_count(db_path) >= 4


def test_process_bet_on_starts_due_task_and_records_events(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9305)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 60
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 5
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False
    rt["gambling_fund"] = 1_000_000

    created = task_engine.create_task(
        ctx,
        name="盘面巡航",
        base_preset="yc20",
        max_bets=1,
        trigger_mode=task_engine.TASK_MODE_REGIME,
        regimes=["延续盘"],
        enabled=True,
    )
    assert created["ok"] is True

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 78
        user_ctx.state.runtime["last_predict_tag"] = "TREND"
        user_ctx.state.runtime["last_predict_info"] = "延续盘"
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        return SimpleNamespace(chat_id=1, id=1)

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    async def fake_notice(*args, **kwargs):
        return None

    async def fake_click(client, event, user_ctx, button_data):
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
    monkeypatch.setattr(zm, "calculate_bet_amount", lambda runtime: 20000)
    monkeypatch.setattr(zm, "generate_mobile_bet_report", lambda *args, **kwargs: "BET_REPORT")
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:task:59:5",
            "regime_label": "延续盘",
            "features": {},
            "similar_cases": {"similar_count": 30, "evidence_strength": "weak", "tiers": {}, "source": "analytics"},
            "recent_temperature": {"level": "normal"},
        },
    )
    monkeypatch.setattr(
        zm.risk_control,
        "evaluate_fk1",
        lambda snapshot, runtime: {
            "action": "allow",
            "action_text": "盘面风控通过，按当前策略执行",
            "tier_cap": "",
            "reason_text": "延续盘通过",
            "regime_label": "延续盘",
        },
    )
    monkeypatch.setattr(zm.constants, "find_combination", lambda amount, buttons: [amount])
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {1000: "BIG1000", 5000: "BIG5000", 20000: "BIG20000"})

    event = SimpleNamespace(
        reply_markup=True,
        chat_id=1,
        id=1,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(60))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    assert rt["task_current_name"] == "盘面巡航"
    assert rt["current_preset_name"] == "yc20"
    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _task_run_count(db_path) >= 2
    logs_text = task_engine.build_task_logs_text(ctx, created["task"]["task_id"])
    assert "盘面巡航" in logs_text


def test_task_stats_text_and_command_views(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9306)
    created = task_engine.create_task(
        ctx,
        name="统计任务",
        base_preset="yc10",
        max_bets=2,
        trigger_mode=task_engine.TASK_MODE_MANUAL,
        enabled=True,
    )
    assert created["ok"] is True
    task_id = created["task"]["task_id"]
    run_result = task_engine.run_task_now(ctx, task_id)
    assert run_result["ok"] is True
    task_engine.record_settlement(ctx, {"bet_id": "b1", "dynamic_tier": "yc10"}, 1600)
    finish_result = task_engine.record_settlement(ctx, {"bet_id": "b2", "dynamic_tier": "yc10"}, -400)
    assert finish_result["task_finished"] is True

    stats_text = task_engine.build_task_stats_text(ctx, task_id)
    assert "任务统计" in stats_text
    assert "统计任务" in stats_text
    assert "累计真实下注：2 笔" in stats_text

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="task list", chat_id=9306, id=1),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text=f"task show {task_id}", chat_id=9306, id=2),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text=f"task stats {task_id}", chat_id=9306, id=3),
            ctx,
            {},
        )
    )

    assert any("任务列表" in msg for msg in sent_messages)
    assert any("任务详情" in msg and "统计任务" in msg for msg in sent_messages)
    assert any("任务统计" in msg and "累计真实下注：2 笔" in msg for msg in sent_messages)


def test_process_user_command_task_templates(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9308)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="task tpl", chat_id=9308, id=1),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="task new 保守巡航 模板A", chat_id=9308, id=2),
            ctx,
            {},
        )
    )

    assert any("任务模板" in msg and "保守巡航" in msg for msg in sent_messages)
    assert any("任务已创建" in msg and "模板A" in msg for msg in sent_messages)
    assert ctx.tasks[0]["name"] == "模板A"


def test_process_user_command_task_template_with_overrides(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9310)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="task new 保守巡航 模板B preset=yc10 bets=12 loss=30000", chat_id=9310, id=1),
            ctx,
            {},
        )
    )

    assert any("任务已创建" in msg and "模板B" in msg for msg in sent_messages)
    assert ctx.tasks[0]["base_preset"] == "yc10"
    assert ctx.tasks[0]["max_bets"] == 12
    assert ctx.tasks[0]["max_loss"] == 30000
