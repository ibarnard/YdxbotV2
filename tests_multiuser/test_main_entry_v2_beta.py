import asyncio
import json
from types import SimpleNamespace
from pathlib import Path

import interaction_journal
import main_multiuser as mm
import zq_multiuser as zm
from user_manager import UserContext


def _fake_user(user_id, user_dir, account_name):
    return SimpleNamespace(
        user_id=user_id,
        user_dir=user_dir,
        config=SimpleNamespace(name=account_name),
    )


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


def test_select_user_contexts_returns_all_when_no_selector():
    users = {
        1: _fake_user(1, r"D:\repo\users\tim", "Tim"),
        2: _fake_user(2, r"D:\repo\users\xu", "Xu"),
    }

    selected = mm._select_user_contexts(users, None)

    assert list(selected.keys()) == [1, 2]


def test_select_user_contexts_matches_dir_name_account_name_and_user_id():
    users = {
        7899409995: _fake_user(7899409995, r"D:\repo\users\tim", "Tim"),
        7565593515: _fake_user(7565593515, r"D:\repo\users\xu", "Musk Xu"),
    }

    by_dir = mm._select_user_contexts(users, ["tim"])
    by_name = mm._select_user_contexts(users, ["musk xu"])
    by_id = mm._select_user_contexts(users, ["7899409995"])

    assert list(by_dir.keys()) == [7899409995]
    assert list(by_name.keys()) == [7565593515]
    assert list(by_id.keys()) == [7899409995]


def test_select_user_contexts_returns_empty_when_selector_not_found():
    users = {
        7899409995: _fake_user(7899409995, r"D:\repo\users\tim", "Tim"),
    }

    selected = mm._select_user_contexts(users, ["not-exists"])

    assert selected == {}


def test_check_models_for_user_uses_serial_probe_without_fallback(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "tim"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "Tim"},
            "telegram": {"user_id": 7899409995},
            "groups": {"admin_chat": 5721909476},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))

    validate_calls = []
    sleep_calls = []
    sent_messages = []

    class FakeManager:
        def load_models(self):
            return None

        def list_models(self):
            return {
                "iflow": [
                    {"model_id": "model-1", "enabled": True},
                    {"model_id": "model-2", "enabled": True},
                ]
            }

        async def validate_model(self, model_id, *, allow_fallback=True):
            validate_calls.append((model_id, allow_fallback))
            return {"success": True, "latency": "12"}

    class DummyClient:
        async def send_message(self, target, message, parse_mode=None):
            sent_messages.append((target, message))
            return SimpleNamespace(chat_id=target, id=len(sent_messages))

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        return None

    monkeypatch.setattr(ctx, "get_model_manager", lambda: FakeManager())
    monkeypatch.setattr(mm.asyncio, "sleep", fake_sleep)

    asyncio.run(mm.check_models_for_user(DummyClient(), ctx))

    assert validate_calls == [("model-1", False), ("model-2", False)]
    assert sleep_calls == [0.6]
    assert sent_messages
    assert "串行探测" in sent_messages[0][1]


def test_sender_allowed_for_user_command_accepts_self_sender_when_whitelist_present(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=7899409995)
    event = SimpleNamespace(sender_id=7899409995, out=True)

    allowed = mm._get_allowed_sender_ids(ctx)

    assert mm._sender_allowed_for_user_command(ctx, event, allowed) is True


def test_interaction_journal_records_and_reads_recent_events(tmp_path):
    ctx = _make_user_context(tmp_path, user_id=7899409996)

    interaction_journal.record_command(
        ctx,
        source="telegram_admin_chat",
        command="help",
        accepted=True,
        sender_id=5721909476,
        chat_id=5721909476,
    )
    interaction_journal.record_inbound(
        ctx,
        source="zq_group_settle",
        message="已结算: 结果为 5 大",
        sender_id=5697370563,
        chat_id=-1001833464786,
        msg_id=9,
    )
    interaction_journal.record_message(
        ctx,
        channel="admin_chat",
        target=5721909476,
        message="test dashboard",
        ok=True,
    )

    records = interaction_journal.read_recent_events(ctx, 5)
    command_records = interaction_journal.read_recent_events(ctx, 5, "commands")
    inbound_records = interaction_journal.read_recent_events(ctx, 5, "inbound")
    outbound_records = interaction_journal.read_recent_events(ctx, 5, "outbound")

    assert len(records) == 3
    assert records[0]["kind"] == "command"
    assert records[1]["kind"] == "inbound"
    assert records[2]["kind"] == "message"
    assert command_records[0]["kind"] == "command"
    assert inbound_records[0]["kind"] == "inbound"
    assert outbound_records[0]["kind"] == "message"
    assert Path(ctx.user_dir, "analytics", "interaction_journal.jsonl").exists()
    assert Path(ctx.user_dir, "analytics", "telegram_commands.jsonl").exists()
    assert Path(ctx.user_dir, "analytics", "telegram_inbound.jsonl").exists()
    assert Path(ctx.user_dir, "analytics", "telegram_outbound.jsonl").exists()


def test_start_user_refreshes_admin_dashboard_on_success(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899409997)
    refreshed = []
    cleared = []

    class DummyClient:
        async def connect(self):
            return None

        async def is_user_authorized(self):
            return True

        async def send_message(self, target, message, parse_mode=None):
            return SimpleNamespace(chat_id=target, id=1)

    async def fake_create_client(user_ctx, global_config):
        return DummyClient()

    async def fake_check_models_for_user(client, user_ctx):
        return None

    async def fake_fetch_account_balance(user_ctx):
        return 1887605

    async def fake_refresh_admin_dashboard(client, user_ctx, global_config):
        refreshed.append(user_ctx.user_id)
        return SimpleNamespace(chat_id=5721909476, id=9)

    monkeypatch.setattr(mm, "_acquire_session_lock", lambda user_ctx: True)
    monkeypatch.setattr(mm, "create_client", fake_create_client)
    monkeypatch.setattr(mm, "register_handlers", lambda client, user_ctx, global_config: None)
    monkeypatch.setattr(mm, "check_models_for_user", fake_check_models_for_user)
    monkeypatch.setattr(mm, "fetch_account_balance", fake_fetch_account_balance)
    monkeypatch.setattr(mm, "_apply_startup_balance_snapshot", lambda user_ctx, balance: balance)
    monkeypatch.setattr(mm.runtime_stability, "inspect_user_context", lambda user_ctx: {"status": "ok", "warnings": []})
    monkeypatch.setattr(mm.runtime_stability, "reconcile_runtime_state", lambda user_ctx: {"changed": False})
    monkeypatch.setattr(mm.runtime_stability, "clear_runtime_faults", lambda user_ctx, stage_prefixes=None, error_types=None: cleared.append(tuple(stage_prefixes or [])) or {"changed": False, "removed": 0})
    monkeypatch.setattr(zm, "apply_account_risk_default_mode", lambda rt: {"base_enabled": True, "deep_enabled": True})
    monkeypatch.setattr(zm, "build_startup_focus_reminder", lambda user_ctx: "focus")
    monkeypatch.setattr(zm, "heal_stale_pending_bets", lambda user_ctx: {"count": 0, "items": []})
    monkeypatch.setattr(zm, "_refresh_admin_dashboard", fake_refresh_admin_dashboard)

    client = asyncio.run(mm.start_user(ctx, {}))

    assert client is not None
    assert refreshed == [ctx.user_id]
    assert cleared == [("startup",)]


def test_process_user_command_model_list_routes_to_admin_chat(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899409998)
    ctx.config.ai["models"] = {
        "iflow_1": {"model_id": "qwen3-coder-plus", "enabled": True},
        "iflow_2": {"model_id": "deepseek-v3", "enabled": True},
    }
    sent = {}

    async def fake_reply(client, event, message, user_ctx, global_config, parse_mode="markdown"):
        sent["message"] = message
        sent["parse_mode"] = parse_mode
        return SimpleNamespace(chat_id=5721909476, id=11)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "_reply_admin_command_result", fake_reply)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="model list", chat_id=5721909476, id=1),
            ctx,
            {},
        )
    )

    assert "可用模型列表" in sent["message"]
    assert "qwen3-coder-plus" in sent["message"]
    assert sent["parse_mode"] == "markdown"


def test_process_user_command_apikey_show_routes_to_admin_chat(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899409999)
    ctx.config.ai["api_keys"] = ["sk-test-1234567890"]
    sent = {}

    async def fake_reply(client, event, message, user_ctx, global_config, parse_mode="markdown"):
        sent["message"] = message
        return SimpleNamespace(chat_id=5721909476, id=12)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "_reply_admin_command_result", fake_reply)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="apikey show", chat_id=5721909476, id=2),
            ctx,
            {},
        )
    )

    assert "AI key 列表" in sent["message"]
    assert "sk-" in sent["message"]


def test_process_user_command_help_defaults_to_compact_summary(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=7899410001)
    sent = {}

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent["message"] = message
        return SimpleNamespace(chat_id=5721909476, id=13)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="help", chat_id=5721909476, id=3),
            ctx,
            {},
        )
    )

    assert "命令速览" in sent["message"]
    assert "`help all`" in sent["message"]
    assert "管理员 chat" in sent["message"]
