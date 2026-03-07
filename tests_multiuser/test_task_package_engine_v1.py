import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import task_engine
import task_package_engine
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="任务包测试", user_id=9401):
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


def _table_count(db_path: Path, table_name: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def test_create_package_from_template(tmp_path):
    ctx = _make_user_context(tmp_path)

    result = task_package_engine.create_package_from_template(ctx, "稳健包", "主包")

    assert result["ok"] is True
    assert len(ctx.task_packages) == 1
    assert ctx.task_packages[0]["name"] == "主包"
    assert len(ctx.task_packages[0]["members"]) == 2
    assert len(ctx.tasks) == 2
    packages_path = Path(ctx.user_dir) / "task_packages.json"
    assert packages_path.exists()


def test_create_package_from_template_with_overrides(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9405)

    result = task_package_engine.create_package_from_template(
        ctx,
        "稳健包",
        "主包",
        base_preset="yc10",
        max_bets=6,
        max_loss=18000,
    )

    assert result["ok"] is True
    assert len(ctx.tasks) == 2
    assert all(task["base_preset"] == "yc10" for task in ctx.tasks)
    assert all(task["max_bets"] == 6 for task in ctx.tasks)
    assert all(task["max_loss"] == 18000 for task in ctx.tasks)


def test_build_package_overview_text_wraps_command_hints(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9406)

    text = task_package_engine.build_package_overview_text(ctx)

    assert "当前运行：无" in text
    assert "命令：`pkg tpl` / `pkg new <模板>` / `pkg list`" in text
    assert "详情：`pkg show <id>` / `pkg logs [id]` / `pkg stats [id]`" in text
    assert "控制：`pkg run <id>` / `pkg pause <id>` / `pkg resume <id>`" in text


def test_prepare_package_for_round_selects_trend_task(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9402)
    result = task_package_engine.create_package_from_template(ctx, "稳健包", "主包")
    assert result["ok"] is True
    package_id = result["package"]["package_id"]

    run_result = task_package_engine.run_package_now(ctx, package_id)
    assert run_result["ok"] is True

    plan = task_package_engine.prepare_package_for_round(
        ctx,
        {
            "regime_label": "延续盘",
            "recent_temperature": {"level": "normal"},
        },
    )

    assert plan["started"] is True
    assert ctx.state.runtime["package_current_name"] == "主包"
    assert "趋势跟随" in ctx.state.runtime["task_current_name"]
    assert ctx.task_packages[0]["current_task_name"].endswith("趋势跟随")


def test_package_settlement_updates_profit_and_clears_task(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=9403)
    created = task_package_engine.create_package_from_template(ctx, "稳健包", "主包")
    package_id = created["package"]["package_id"]
    assert task_package_engine.run_package_now(ctx, package_id)["ok"] is True
    plan = task_package_engine.prepare_package_for_round(
        ctx,
        {
            "regime_label": "延续盘",
            "recent_temperature": {"level": "normal"},
        },
    )
    assert plan["started"] is True
    current_task = task_engine.current_task(ctx)
    assert current_task is not None

    task_engine.record_settlement(ctx, {"bet_id": "b1", "dynamic_tier": "yc20"}, 1200)
    task_package_engine.record_settlement(ctx, 1200)
    task_engine.record_settlement(ctx, {"bet_id": "b2", "dynamic_tier": "yc20"}, -600)
    task_package_engine.record_settlement(ctx, -600)
    task_engine.stop_task_run(ctx, current_task["task_id"], reason="测试结束当前任务")
    task_package_engine.record_settlement(ctx, 0)

    assert ctx.task_packages[0]["total_profit"] == 600
    assert ctx.task_packages[0]["current_task_id"] == ""

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert _table_count(db_path, "package_runs") >= 3


def test_pkg_commands_and_process_bet_on_integration(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9404)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 60
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 6
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False
    rt["gambling_fund"] = 1_000_000

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    async def fake_notice(*args, **kwargs):
        return None

    async def fake_click(client, event, user_ctx, button_data):
        return None

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 82
        user_ctx.state.runtime["last_predict_tag"] = "TREND"
        user_ctx.state.runtime["last_predict_info"] = "延续盘"
        return 1

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "delete_later", fake_delete_later)
    monkeypatch.setattr(zm, "_send_transient_admin_notice", fake_notice)
    monkeypatch.setattr(zm, "_click_bet_button_with_recover", fake_click)
    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict_next_bet_v10)
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
            "round_key": "rk:pkg:59:6",
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
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {5000: "BIG5000", 20000: "BIG20000"})

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="pkg new 稳健包 主包", chat_id=9404, id=1),
            ctx,
            {},
        )
    )
    package_id = ctx.task_packages[0]["package_id"]
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text=f"pkg run {package_id}", chat_id=9404, id=2),
            ctx,
            {},
        )
    )

    event = SimpleNamespace(
        reply_markup=True,
        chat_id=1,
        id=1,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(60))),
    )
    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    assert ctx.state.runtime["package_current_name"] == "主包"
    assert ctx.state.runtime["task_current_name"]
    assert any("任务包已创建" in msg for msg in sent_messages)
    assert any("任务包已启动" in msg for msg in sent_messages)


def test_pkg_new_with_overrides(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9406)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=1, id=len(sent_messages))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="pkg new 稳健包 主包 preset=yc10 bets=6 loss=18000", chat_id=9406, id=1),
            ctx,
            {},
        )
    )

    assert any("任务包已创建" in msg and "主包" in msg for msg in sent_messages)
    assert len(ctx.tasks) == 2
    assert all(task["base_preset"] == "yc10" for task in ctx.tasks)
    assert all(task["max_bets"] == 6 for task in ctx.tasks)
    assert all(task["max_loss"] == 18000 for task in ctx.tasks)
