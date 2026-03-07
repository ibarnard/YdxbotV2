import asyncio
import json
from pathlib import Path

import interaction_journal
import main_multiuser as mm
import multi_account_orchestrator
import tg_watch
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="Tim", user_id=7899409995):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": 5721909476},
            "notification": {
                "admin_chat": 5721909476,
                "allowed_sender_ids": [5721909476],
                "iyuu": {"enable": False},
                "tg_bot": {"enable": False},
            },
        },
    )
    return UserContext(str(user_dir))


def _capture_background_tasks(monkeypatch, module):
    tasks = []

    def fake_create_task(coro):
        tasks.append(coro)
        return None

    monkeypatch.setattr(module.asyncio, "create_task", fake_create_task)
    return tasks


def _close_background_tasks(tasks):
    for coro in tasks:
        coro.close()


def test_inject_admin_command_records_help_round_trip(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899409997)
    tasks = _capture_background_tasks(monkeypatch, mm)

    try:
        result = asyncio.run(mm.inject_admin_command(ctx, {}, "help"))
    finally:
        _close_background_tasks(tasks)

    assert result["accepted"] is True
    assert result["command"] == "help"
    assert result["outbound_messages"]
    assert result["outbound_messages"][0]["target"] == 5721909476

    command_records = interaction_journal.read_recent_events(ctx, 5, "commands")
    outbound_records = interaction_journal.read_recent_events(ctx, 5, "outbound")

    assert command_records[-1]["source"] == "local_admin_probe"
    assert command_records[-1]["accepted"] is True
    assert outbound_records[-1]["target"] == 5721909476
    assert outbound_records[-1]["ok"] is True


def test_inject_admin_command_rejects_sender_outside_allowlist(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=7899409998)

    result = asyncio.run(mm.inject_admin_command(ctx, {}, "help", sender_id=1))

    assert result["accepted"] is False
    assert result["reason"] == "sender_not_allowed"
    assert result["outbound_messages"] == []


def test_inject_admin_command_dashboard_captures_dashboard_message(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899409999)
    tasks = _capture_background_tasks(monkeypatch, mm)

    try:
        result = asyncio.run(mm.inject_admin_command(ctx, {}, "dashboard"))
    finally:
        _close_background_tasks(tasks)

    assert result["accepted"] is True
    assert result["outbound_messages"]

    outbound_records = interaction_journal.read_recent_events(ctx, 5, "outbound")

    assert outbound_records[-1]["msg_type"] == "dashboard"
    assert outbound_records[-1]["message_kind"] == "dashboard"


def test_watch_and_fleet_status_follow_unified_status_text(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=7899410000)
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["manual_pause"] = False
    rt["bet_on"] = False
    rt["bet"] = False
    rt["mode_stop"] = True

    watch_text = tg_watch.build_watch_overview_text(ctx)
    fleet_text = multi_account_orchestrator.build_fleet_overview_text(ctx)
    fleet_account_text = multi_account_orchestrator.build_fleet_account_text(ctx, str(ctx.user_id))

    assert "待机中" in watch_text
    assert "待机中" in fleet_text
    assert "待机中" in fleet_account_text
    assert "已暂停" not in watch_text
