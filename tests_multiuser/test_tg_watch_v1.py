import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from user_manager import UserContext
import zq_multiuser as zm


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_user_context_normalizes_watch_config_from_legacy_fields(tmp_path):
    user_dir = tmp_path / "users" / "8101"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "值守归一化"},
            "telegram": {"user_id": 8101},
            "groups": {"admin_chat": 8101},
            "notification": {
                "watch_chat": "-9001",
                "watch_tg_bot": {
                    "enable": True,
                    "bot_token": "watch-token",
                    "chat_id": "watch-chat",
                },
            },
        },
    )

    ctx = UserContext(str(user_dir))

    assert ctx.config.notification["watch"]["admin_chat"] == "-9001"
    assert ctx.config.notification["watch"]["tg_bot"]["bot_token"] == "watch-token"
    assert ctx.config.notification["watch"]["tg_bot"]["chat_id"] == "watch-chat"


def test_send_to_watch_prefers_watch_targets_and_keeps_account_prefix(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8102"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "值守用户"},
            "telegram": {"user_id": 8102},
            "groups": {"admin_chat": 8102},
            "notification": {
                "admin_chat": 8102,
                "tg_bot": {"enable": True, "bot_token": "base-token", "chat_id": "base-chat"},
                "watch": {
                    "admin_chat": "-9002",
                    "tg_bot": {
                        "enable": True,
                        "bot_token": "watch-token",
                        "chat_id": "watch-chat",
                    },
                },
            },
        },
    )
    ctx = UserContext(str(user_dir))
    requests_payloads = []

    def fake_post(url, data=None, json=None, timeout=5):
        requests_payloads.append({"url": url, "data": data, "json": json})
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(zm.requests, "post", fake_post)

    class DummyClient:
        def __init__(self):
            self.messages = []

        async def send_message(self, target, message, parse_mode=None):
            self.messages.append((target, message, parse_mode))
            return SimpleNamespace(chat_id=target, id=18)

    client = DummyClient()
    asyncio.run(zm.send_to_watch(client, "测试值守", ctx, {}))

    assert client.messages == [(-9002, "【账号：值守用户】\n测试值守", "markdown")]
    assert len(requests_payloads) == 1
    assert requests_payloads[0]["url"] == "https://api.telegram.org/botwatch-token/sendMessage"
    assert requests_payloads[0]["json"]["chat_id"] == "watch-chat"
    assert requests_payloads[0]["json"]["text"].startswith("【账号：值守用户】")


def test_send_to_watch_falls_back_to_admin_and_tg_bot_when_watch_missing(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8103"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "回退用户"},
            "telegram": {"user_id": 8103},
            "groups": {"admin_chat": "9103"},
            "notification": {
                "tg_bot": {"enable": True, "bot_token": "base-token", "chat_id": "base-chat"},
            },
        },
    )
    ctx = UserContext(str(user_dir))
    requests_payloads = []

    def fake_post(url, data=None, json=None, timeout=5):
        requests_payloads.append({"url": url, "data": data, "json": json})
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(zm.requests, "post", fake_post)

    class DummyClient:
        def __init__(self):
            self.messages = []

        async def send_message(self, target, message, parse_mode=None):
            self.messages.append((target, message))
            return SimpleNamespace(chat_id=target, id=19)

    client = DummyClient()
    asyncio.run(zm.send_to_watch(client, "回退测试", ctx, {}))

    assert client.messages == [(9103, "【账号：回退用户】\n回退测试")]
    assert len(requests_payloads) == 1
    assert requests_payloads[0]["url"] == "https://api.telegram.org/botbase-token/sendMessage"
    assert requests_payloads[0]["json"]["chat_id"] == "base-chat"
    assert requests_payloads[0]["json"]["text"].startswith("【账号：回退用户】")
