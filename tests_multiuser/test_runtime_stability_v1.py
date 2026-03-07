import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import main_multiuser as mm
import runtime_stability
import tg_watch
import zq_multiuser as zm
from user_manager import UserContext, clear_registered_user_contexts


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _merge_dict(base, override):
    result = dict(base)
    for key, value in override.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _make_user_context(tmp_path, name, user_id, override=None):
    config = {
        "account": {"name": name},
        "telegram": {
            "user_id": user_id,
            "api_id": 123456,
            "api_hash": "hash-123",
            "session_name": f"session-{user_id}",
        },
        "groups": {
            "admin_chat": user_id,
            "zq_group": ["me"],
            "zq_bot": "bot_a",
        },
        "notification": {
            "admin_chat": user_id,
            "watch": {
                "admin_chat": f"-90{user_id}",
                "tg_bot": {
                    "enable": True,
                    "bot_token": "watch-token",
                    "chat_id": f"watch-{user_id}",
                },
            },
        },
        "zhuque": {
            "cookie": "cookie=value",
            "csrf_token": "csrf",
            "api_url": "https://example.com/api",
        },
    }
    if override:
        config = _merge_dict(config, override)
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(user_dir / "config.json", config)
    return UserContext(str(user_dir))


def test_inspect_user_context_reports_blockers_and_warnings(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(
        tmp_path,
        "阻断用户",
        9701,
        override={
            "telegram": {"api_id": "", "api_hash": ""},
            "groups": {"admin_chat": "", "zq_group": [], "zq_bot": ""},
            "notification": {
                "admin_chat": "",
                "watch": {
                    "admin_chat": "",
                    "tg_bot": {"enable": False, "bot_token": "", "chat_id": ""},
                },
            },
            "zhuque": {"cookie": "", "csrf_token": ""},
        },
    )

    result = runtime_stability.inspect_user_context(ctx)
    blocker_messages = [item["message"] for item in result["blockers"]]
    warning_messages = [item["message"] for item in result["warnings"]]

    assert result["status"] == "blocked"
    assert any("api_id/api_hash" in text for text in blocker_messages)
    assert any("zq_group" in text for text in blocker_messages)
    assert any("zq_bot" in text for text in blocker_messages)
    assert any("admin_chat" in text for text in warning_messages)
    assert any("值守播报" in text for text in warning_messages)
    assert any("cookie/csrf" in text for text in warning_messages)


def test_inspect_user_context_treats_zero_targets_as_missing(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(
        tmp_path,
        "zero-targets",
        9708,
        override={
            "groups": {"admin_chat": 0, "zq_group": [0], "zq_bot": 0},
            "notification": {
                "admin_chat": 0,
                "watch": {
                    "admin_chat": 0,
                    "tg_bot": {"enable": True, "bot_token": "watch-token", "chat_id": "0"},
                },
                "tg_bot": {"enable": True, "bot_token": "base-token", "chat_id": "0"},
            },
        },
    )

    result = runtime_stability.inspect_user_context(ctx)
    blocker_messages = [item["message"] for item in result["blockers"]]
    warning_messages = [item["message"] for item in result["warnings"]]

    assert result["status"] == "blocked"
    assert any("zq_group" in text for text in blocker_messages)
    assert any("zq_bot" in text for text in blocker_messages)
    assert any("admin_chat" in text for text in warning_messages)
    assert any("值守播报" in text for text in warning_messages)


def test_reconcile_runtime_state_clears_stale_runtime_fields(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "清理用户", 9702)
    rt = ctx.state.runtime
    rt["watch_event_state"] = {"ok": {"last_sent_ts": 1}, "bad": "x", 3: {"oops": 1}}
    rt["watch_alerts"] = ["bad", {"event_type": "fund_pause"}, 9]
    rt["pause_count"] = 0
    rt["pause_resume_pending"] = True
    rt["pause_resume_pending_reason"] = "旧暂停"
    rt["pause_countdown_active"] = True
    rt["pause_countdown_reason"] = "旧倒计时"
    rt["pause_countdown_total_rounds"] = 3
    rt["pause_countdown_last_remaining"] = 2
    rt["shadow_probe_active"] = False
    rt["shadow_probe_origin_reason"] = "旧影子"
    rt["shadow_probe_target_rounds"] = 3
    rt["shadow_probe_pass_required"] = 2
    rt["shadow_probe_checked"] = 2
    rt["shadow_probe_hits"] = 1
    rt["shadow_probe_pending_prediction"] = 1
    rt["shadow_probe_last_history_len"] = 88
    rt["shadow_probe_rearm"] = True
    rt["pending_bet_id"] = "bet-1"
    rt["current_round_key"] = "round-1"
    rt["runtime_faults"] = "oops"
    rt["last_runtime_fault"] = {"stage": "old"}
    ctx.state.bet_sequence_log = [{"bet_id": "other", "result": 1}]

    result = runtime_stability.reconcile_runtime_state(ctx)

    assert result["changed"] is True
    assert rt["watch_event_state"] == {"ok": {"last_sent_ts": 1}}
    assert rt["watch_alerts"] == [{"event_type": "fund_pause"}]
    assert rt["pause_resume_pending"] is False
    assert rt["pause_countdown_active"] is False
    assert rt["pause_countdown_reason"] == ""
    assert rt["shadow_probe_origin_reason"] == ""
    assert rt["shadow_probe_target_rounds"] == 0
    assert rt["shadow_probe_pending_prediction"] is None
    assert rt["pending_bet_id"] == ""
    assert rt["current_round_key"] == ""
    assert rt["runtime_faults"] == []
    assert rt["last_runtime_fault"] == {}


def test_record_runtime_fault_deduplicates_and_doctor_shows_recent_fault(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "异常用户", 9703)

    first = runtime_stability.record_runtime_fault(
        ctx,
        "send_watch",
        RuntimeError("network down"),
        action="值守消息已跳过",
    )
    second = runtime_stability.record_runtime_fault(
        ctx,
        "send_watch",
        RuntimeError("network down"),
        action="值守消息已跳过",
    )
    doctor_text = runtime_stability.build_doctor_text(ctx)

    assert first["count"] == 1
    assert second["count"] == 2
    assert "最近异常" in doctor_text
    assert "send_watch" in doctor_text
    assert "x2" in doctor_text
    assert "network down" in doctor_text
    assert doctor_text.count("\n  ") >= 2


def test_clear_runtime_faults_removes_matching_startup_entries(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "清理异常用户", 97031)
    runtime_stability.record_runtime_fault(ctx, "startup", RuntimeError("dup key"), persist=True)
    runtime_stability.record_runtime_fault(ctx, "startup_dashboard", RuntimeError("dashboard fail"), persist=True)
    runtime_stability.record_runtime_fault(ctx, "send_watch", RuntimeError("network down"), persist=True)

    result = runtime_stability.clear_runtime_faults(ctx, stage_prefixes=["startup"])

    assert result["changed"] is True
    assert result["removed"] == 2
    remaining = runtime_stability.list_runtime_faults(ctx, limit=5)
    assert len(remaining) == 1
    assert remaining[0]["stage"] == "send_watch"


def test_process_user_command_doctor_fleet_routes_summary(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    master = _make_user_context(tmp_path, "master", 9704)
    _make_user_context(
        tmp_path,
        "warn-user",
        9705,
        override={"notification": {"watch": {}}, "zhuque": {"cookie": "", "csrf_token": ""}},
    )
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    event = SimpleNamespace(raw_text="doctor fleet", chat_id=1, id=1)
    asyncio.run(zm.process_user_command(None, event, master, {}))

    assert sent_messages
    assert "🩺 多账号自检" in sent_messages[-1]
    assert "warn-user (9705)" in sent_messages[-1]


def test_start_user_blocks_before_creating_client_when_doctor_fails(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(
        tmp_path,
        "startup-blocked",
        9706,
        override={
            "telegram": {"api_id": "", "api_hash": ""},
            "groups": {"zq_group": [], "zq_bot": ""},
        },
    )
    called = {"create_client": False}

    async def fake_create_client(user_ctx, global_config):
        called["create_client"] = True
        raise AssertionError("should not create client when doctor is blocked")

    monkeypatch.setattr(mm, "create_client", fake_create_client)

    result = asyncio.run(mm.start_user(ctx, {}))

    assert result is None
    assert called["create_client"] is False


def test_watch_alerts_include_recent_runtime_fault(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "watch-fault", 9707)
    runtime_stability.record_runtime_fault(
        ctx,
        "send_watch",
        RuntimeError("timeout while sending"),
        action="值守消息已跳过",
        severity="error",
    )
    monkeypatch.setattr(
        tg_watch,
        "_build_watch_evidence",
        lambda user_ctx: {
            "overview_24h": {"pnl_total": 0, "max_drawdown": 0, "settled_count": 0, "win_rate": 0.0},
            "recent_temperature": {"level": "normal"},
        },
    )

    text = tg_watch.build_watch_alerts_text(ctx)

    assert "运行异常" in text
    assert "send_watch" in text
