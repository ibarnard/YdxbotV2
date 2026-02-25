import asyncio
import json
import re
import threading
from pathlib import Path
from types import SimpleNamespace

from user_manager import UserContext, UserManager
from model_manager import ModelManager
import constants
import zq_multiuser as zm


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_user_context_user_id_fallback_numeric_dir(tmp_path):
    user_dir = tmp_path / "users" / "1001"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "测试用户"},
            "telegram": {},
        },
    )

    ctx = UserContext(str(user_dir))
    assert ctx.user_id == 1001
    assert ctx.config.name == "测试用户"


def test_user_context_user_id_fallback_hash_dir(tmp_path):
    user_dir = tmp_path / "users" / "alpha_user"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "Alpha"},
            "telegram": {},
        },
    )

    ctx = UserContext(str(user_dir))
    assert isinstance(ctx.user_id, int)
    assert ctx.user_id > 0


def test_user_manager_get_iflow_config_compatible_with_ai_key(tmp_path):
    users_dir = tmp_path / "users"
    shared_dir = tmp_path / "shared"
    users_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        shared_dir / "global.json",
        {
            "ai": {"enabled": True, "base_url": "https://apis.iflow.cn/v1"},
        },
    )

    mgr = UserManager(users_dir=str(users_dir), shared_dir=str(shared_dir))
    mgr.load_all_users()
    cfg = mgr.get_iflow_config()
    assert cfg.get("enabled") is True
    assert "base_url" in cfg


def test_user_context_merges_shared_and_user_config(tmp_path):
    users_dir = tmp_path / "users"
    shared_dir = tmp_path / "shared"
    users_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        shared_dir / "global.json",
        {
            "groups": {"monitor": [101, 102], "zq_group": [201], "admin_chat": 9, "zq_bot": 8},
            "zhuque": {"api_url": "https://zhuque.in/api/user/getInfo?"},
            "notification": {"iyuu": {"enable": True}, "tg_bot": {"enable": True, "chat_id": "9"}},
            "proxy": {"enabled": False, "host": "127.0.0.1", "port": 7890},
            "ai": {"enabled": True, "base_url": "https://apis.iflow.cn/v1", "models": {"1": {"model_id": "m1", "enabled": True}}},
        },
    )
    _write_json(
        users_dir / "6001" / "config.json",
        {
            "account": {"name": "合并用户"},
            "telegram": {"user_id": 6001},
            "groups": {"admin_chat": 6001},
            "zhuque": {"cookie": "c1", "x_csrf": "x1"},
        },
    )

    mgr = UserManager(users_dir=str(users_dir), shared_dir=str(shared_dir))
    assert mgr.load_all_users() == 1

    ctx = mgr.get_user(6001)
    assert ctx is not None
    assert ctx.config.groups["monitor"] == [101, 102]
    assert ctx.config.groups["admin_chat"] == 6001
    assert ctx.config.zhuque["api_url"] == "https://zhuque.in/api/user/getInfo?"
    assert ctx.config.zhuque["cookie"] == "c1"
    assert ctx.config.notification["iyuu"]["enable"] is True
    assert ctx.config.ai["base_url"] == "https://apis.iflow.cn/v1"


def test_model_manager_apply_shared_config_uses_shared_chain():
    mgr = ModelManager()
    mgr.apply_shared_config(
        {
            "ai": {
                "enabled": True,
                "api_keys": ["k1"],
                "base_url": "https://apis.iflow.cn/v1",
                "models": {
                    "1": {"model_id": "model-1", "enabled": True},
                    "2": {"model_id": "model-2", "enabled": True},
                },
                "fallback_chain": ["2", "1"],
            }
        }
    )

    assert mgr.fallback_chain == ["2", "1"]
    assert mgr.get_model("1")["model_id"] == "model-1"
    assert mgr.get_model("2")["model_id"] == "model-2"


def test_user_context_supports_hash_comments_in_config(tmp_path):
    user_dir = tmp_path / "users" / "commented"
    user_dir.mkdir(parents=True, exist_ok=True)
    config_text = """{
    # Telegram 登录参数
    "telegram": {
        "api_id": 123456,
        "api_hash": "abc123",
        "session_name": "demo",
        "user_id": 778899
    },
    # 账号信息
    "account": {"name": "注释用户"} # 行尾注释
}
"""
    (user_dir / "config.json").write_text(config_text, encoding="utf-8")
    ctx = UserContext(str(user_dir))
    assert ctx.user_id == 778899
    assert ctx.config.name == "注释用户"


def test_user_context_refreshes_builtin_presets_but_keeps_custom(tmp_path):
    user_dir = tmp_path / "users" / "preset_user"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "预设用户"},
            "telegram": {"user_id": 6123},
        },
    )
    _write_json(
        user_dir / "presets.json",
        {
            "yc05": ["1", "13", "3", "2.1", "2.1", "2.05", "500"],
            "my_custom": ["1", "6", "2.2", "2.1", "2.0", "2.0", "800"],
        },
    )

    ctx = UserContext(str(user_dir))
    assert ctx.presets["yc05"] == constants.PRESETS["yc05"]
    assert ctx.presets["my_custom"] == ["1", "6", "2.2", "2.1", "2.0", "2.0", "800"]

    saved_presets = json.loads((user_dir / "presets.json").read_text(encoding="utf-8"))
    assert saved_presets["yc05"] == constants.PRESETS["yc05"]
    assert saved_presets["my_custom"] == ["1", "6", "2.2", "2.1", "2.0", "2.0", "800"]


def test_main_multiuser_settle_regex_is_strict():
    source = Path("main_multiuser.py").read_text(encoding="utf-8")
    assert 'pattern=r"已结算: 结果为 (\\d+) (大|小)"' in source

    pattern = re.compile(r"已结算: 结果为 (\d+) (大|小)")
    assert pattern.search("已结算: 结果为 12 大")
    assert pattern.search("已结算: 结果为 8 小")
    assert pattern.search("已结算: 结果为 9 |") is None


def test_user_isolation_between_two_contexts(tmp_path):
    users_dir = tmp_path / "users"
    shared_dir = tmp_path / "shared"
    _write_json(shared_dir / "global.json", {"ai": {"enabled": True}})

    _write_json(users_dir / "1001" / "config.json", {"account": {"name": "U1"}, "telegram": {"user_id": 1001}})
    _write_json(users_dir / "1002" / "config.json", {"account": {"name": "U2"}, "telegram": {"user_id": 1002}})

    mgr = UserManager(users_dir=str(users_dir), shared_dir=str(shared_dir))
    assert mgr.load_all_users() == 2

    u1 = mgr.get_user(1001)
    u2 = mgr.get_user(1002)
    assert u1 is not None and u2 is not None

    u1.set_runtime("bet_amount", 12345)
    u2.set_runtime("bet_amount", 54321)

    assert u1.get_runtime("bet_amount") == 12345
    assert u2.get_runtime("bet_amount") == 54321


def test_user_state_save_concurrent_no_corruption(tmp_path):
    user_dir = tmp_path / "users" / "2001"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "并发用户"},
            "telegram": {"user_id": 2001},
        },
    )
    ctx = UserContext(str(user_dir))

    def worker(i):
        ctx.set_runtime("counter", i)
        ctx.save_state()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state_path = user_dir / "state.json"
    loaded = json.loads(state_path.read_text(encoding="utf-8"))
    assert "runtime" in loaded
    assert isinstance(loaded["runtime"], dict)
    assert "counter" in loaded["runtime"]


def test_send_message_returns_admin_message_object(tmp_path):
    user_dir = tmp_path / "users" / "3001"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "消息用户"},
            "telegram": {"user_id": 3001},
            "groups": {"admin_chat": 3001},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))

    class DummyClient:
        async def send_message(self, target, message, parse_mode=None):
            return SimpleNamespace(chat_id=target, id=88)

    message = asyncio.run(
        zm.send_message(
            DummyClient(),
            "admin",
            "hello",
            ctx,
            {},
        )
    )
    assert message is not None
    assert message.id == 88


def test_process_bet_on_parses_history_and_places_bet(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "4001"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "下注用户"},
            "telegram": {"user_id": 4001},
            "groups": {"admin_chat": 4001},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["stop_count"] = 0
    rt["bet_amount"] = 500
    rt["lose_count"] = 0
    rt["win_count"] = 0

    async def fake_predict(user_ctx, global_cfg):
        user_ctx.state.runtime["last_predict_info"] = "test"
        user_ctx.state.predictions.append(1)
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=1, id=1)

    async def fake_delete_later(*args, **kwargs):
        return None

    async def fake_sleep(*args, **kwargs):
        return None

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "delete_later", fake_delete_later)
    monkeypatch.setattr(zm.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    class DummyEvent:
        def __init__(self):
            history = " ".join((["0", "1"] * 20))
            self.message = SimpleNamespace(message=f"[近 40 次结果][由近及远][0 小 1 大] {history}")
            self.reply_markup = object()
            self.chat_id = 1
            self.id = 1
            self.clicks = []

        async def click(self, data):
            self.clicks.append(data)

    event = DummyEvent()
    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    assert len(ctx.state.history) >= 40
    assert rt.get("bet") is True
    assert len(ctx.state.bet_sequence_log) == 1
    assert rt.get("current_bet_seq", 1) >= 2


def test_process_bet_on_allows_short_history_like_master(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "4002"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "短历史用户"},
            "telegram": {"user_id": 4002},
            "groups": {"admin_chat": 4002},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["stop_count"] = 0
    rt["initial_amount"] = 500
    rt["bet_amount"] = 500
    rt["lose_count"] = 0
    rt["win_count"] = 0

    async def fake_predict(user_ctx, global_cfg):
        user_ctx.state.runtime["last_predict_info"] = "test-short-history"
        user_ctx.state.predictions.append(1)
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=1, id=1)

    async def fake_delete_later(*args, **kwargs):
        return None

    async def fake_sleep(*args, **kwargs):
        return None

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "delete_later", fake_delete_later)
    monkeypatch.setattr(zm.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    class DummyEvent:
        def __init__(self):
            self.message = SimpleNamespace(message="[近 40 次结果][由近及远][0 小 1 大] 1 0 1")
            self.reply_markup = object()
            self.chat_id = 1
            self.id = 1
            self.clicks = []

        async def click(self, data):
            self.clicks.append(data)

    event = DummyEvent()
    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    assert len(ctx.state.history) == 3
    assert rt.get("bet") is True
    assert len(ctx.state.bet_sequence_log) == 1


def test_user_context_migrates_legacy_state_when_history_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    legacy_user_id = 500099

    user_dir = tmp_path / "users" / "xu"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "迁移用户"},
            "telegram": {"user_id": legacy_user_id},
        },
    )
    _write_json(user_dir / "state.json", {"history": [], "runtime": {}})
    (tmp_path / "config.py").write_text(f"user = {legacy_user_id}\n", encoding="utf-8")

    legacy_state = {
        "history": [0, 1] * 30,  # 60条
        "bet_type_history": [0, 1] * 30,
        "predictions": [1, 0] * 30,
        "bet_sequence_log": [],
        "state": {"current_model_id": "qwen3-coder-plus", "bet_amount": 500},
    }
    _write_json(tmp_path / "state.json", legacy_state)

    ctx = UserContext(str(user_dir))
    assert len(ctx.state.history) >= 40


def test_send_message_v2_routes_and_account_prefix(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5001"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "路由用户"},
            "telegram": {"user_id": 5001},
            "groups": {"admin_chat": 5001},
            "notification": {
                "iyuu": {"enable": True, "url": "https://iyuu.test/send"},
                "tg_bot": {"enable": True, "bot_token": "token", "chat_id": "chat"},
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
            return SimpleNamespace(chat_id=target, id=7)

    client = DummyClient()
    asyncio.run(
        zm.send_message_v2(
            client,
            "lose_streak",
            "【账号：路由用户】\n测试告警",
            ctx,
            {},
            title="标题",
            desp="测试告警",
        )
    )

    assert client.messages == [(5001, "测试告警")]
    assert len(requests_payloads) == 2
    iyuu_payload = next(item for item in requests_payloads if "iyuu" in item["url"])
    tg_payload = next(item for item in requests_payloads if "api.telegram.org" in item["url"])
    assert iyuu_payload["data"]["desp"].startswith("【账号：路由用户】")
    assert tg_payload["json"]["text"].startswith("【账号：路由用户】")


def test_send_message_v2_lose_end_priority_keeps_account_prefix(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5011"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "回补用户"},
            "telegram": {"user_id": 5011},
            "groups": {"admin_chat": 5011},
            "notification": {
                "iyuu": {"enable": True, "url": "https://iyuu.test/send"},
                "tg_bot": {"enable": True, "bot_token": "token", "chat_id": "chat"},
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
            return SimpleNamespace(chat_id=target, id=8)

    client = DummyClient()
    asyncio.run(
        zm.send_message_v2(
            client,
            "lose_end",
            "【账号：回补用户】\n连输已终止",
            ctx,
            {},
            title="标题",
            desp="连输已终止",
        )
    )

    # 管理员通道不带账号前缀
    assert client.messages == [(5011, "连输已终止")]
    # 重点通道必须带账号前缀
    iyuu_payload = next(item for item in requests_payloads if "iyuu" in item["url"])
    tg_payload = next(item for item in requests_payloads if "api.telegram.org" in item["url"])
    assert iyuu_payload["data"]["desp"].startswith("【账号：回补用户】")
    assert tg_payload["json"]["text"].startswith("【账号：回补用户】")


def test_build_yc_result_message_uses_codeblock_table():
    params = {
        "continuous": 1,
        "lose_stop": 13,
        "lose_once": 3.0,
        "lose_twice": 2.1,
        "lose_three": 2.1,
        "lose_four": 2.05,
        "initial_amount": 3000,
    }
    msg = zm._build_yc_result_message(params, "yc_demo", current_fund=30_000_000, auto_trigger=False)

    assert msg.startswith("```")
    assert "🎯 策略参数" in msg
    assert "策略命令: 1 13 3.0 2.1 2.1 2.05 3000" in msg
    assert "🎯 策略总结:" in msg
    assert "资金最多连数:" in msg
    assert "连数|倍率|下注| 盈利 |所需本金" in msg
    assert " 15|" in msg
    assert msg.count("```") == 2


def test_process_settle_open_ydx_supports_monitor_list(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5002"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "结算用户"},
            "telegram": {"user_id": 5002},
            "groups": {"admin_chat": 5002, "monitor": [101, 102]},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    ctx.state.runtime["open_ydx"] = True
    ctx.state.runtime["bet"] = False

    async def fake_fetch_balance(user_ctx):
        return 123456

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=5002, id=99)

    monkeypatch.setattr(zm, "fetch_balance", fake_fetch_balance)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    class DummyClient:
        def __init__(self):
            self.sent = []

        async def send_message(self, target, message, parse_mode=None):
            self.sent.append((target, message))
            return SimpleNamespace(chat_id=target, id=1)

        async def delete_messages(self, chat_id, message_id):
            return None

    event = SimpleNamespace(message=SimpleNamespace(message="已结算: 结果为 8 小"))
    client = DummyClient()
    asyncio.run(zm.process_settle(client, event, ctx, {}))

    monitor_messages = [msg for msg in client.sent if msg[1] == "/ydx"]
    assert (101, "/ydx") in monitor_messages
    assert (102, "/ydx") in monitor_messages
    assert ctx.state.history[-1] == 0


def test_check_bet_status_can_resume_when_fund_sufficient(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5003"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "恢复用户"},
            "telegram": {"user_id": 5003},
            "groups": {"admin_chat": 5003},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["stop_count"] = 0
    rt["bet"] = False
    rt["gambling_fund"] = 2_000_000
    rt["bet_amount"] = 0
    rt["lose_count"] = 0
    rt["win_count"] = 0

    sent = {}

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent["message"] = message
        return SimpleNamespace(chat_id=5003, id=1)

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    asyncio.run(zm.check_bet_status(SimpleNamespace(), ctx, {}))

    assert rt["bet"] is True
    assert rt["pause_count"] == 0
    assert "押注已恢复" in sent["message"]


def test_pause_command_sets_manual_pause_and_blocks_bet_on(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5005"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "暂停用户"},
            "telegram": {"user_id": 5005},
            "groups": {"admin_chat": 5005},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["bet"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=5005, id=1)

    async def fake_sleep(*args, **kwargs):
        return None

    async def fail_predict(*args, **kwargs):
        raise AssertionError("predict should not run while manual pause is active")

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(zm, "predict_next_bet_v10", fail_predict)

    cmd_event = SimpleNamespace(raw_text="pause", chat_id=5005, id=10)
    asyncio.run(zm.process_user_command(SimpleNamespace(), cmd_event, ctx, {}))

    assert rt["manual_pause"] is True
    assert rt["bet_on"] is False
    assert rt["bet"] is False

    class DummyEvent:
        def __init__(self):
            history = " ".join((["0", "1"] * 20))
            self.message = SimpleNamespace(message=f"[近 40 次结果][由近及远][0 小 1 大] {history}")
            self.reply_markup = object()
            self.chat_id = 5005
            self.id = 11

    asyncio.run(zm.process_bet_on(SimpleNamespace(), DummyEvent(), ctx, {}))


def test_check_bet_status_does_not_resume_when_manual_pause(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5006"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "手动暂停用户"},
            "telegram": {"user_id": 5006},
            "groups": {"admin_chat": 5006},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["manual_pause"] = True
    rt["stop_count"] = 0
    rt["bet"] = False
    rt["gambling_fund"] = 2_000_000
    rt["bet_amount"] = 0
    rt["lose_count"] = 0
    rt["win_count"] = 0

    sent = {"called": False}

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent["called"] = True
        return SimpleNamespace(chat_id=5006, id=1)

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    asyncio.run(zm.check_bet_status(SimpleNamespace(), ctx, {}))

    assert rt["bet"] is False
    assert sent["called"] is False


def test_process_settle_lose_warning_matches_master_style(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5004"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "告警用户"},
            "telegram": {"user_id": 5004},
            "groups": {"admin_chat": 5004},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["bet"] = True
    rt["bet_type"] = 0  # 押小
    rt["bet_amount"] = 500
    rt["warning_lose_count"] = 1
    rt["bet_sequence_count"] = 1
    rt["account_balance"] = 10_000_000
    rt["gambling_fund"] = 9_000_000
    rt["current_round"] = 1
    rt["current_bet_seq"] = 2
    ctx.state.bet_sequence_log = [{"bet_id": "20260223_1_1", "profit": None}]

    captured = {}

    async def fake_send_message_v2(client, msg_type, message, user_ctx, global_cfg, parse_mode="markdown", title=None, desp=None):
        captured["type"] = msg_type
        captured["message"] = message
        return None

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=5004, id=12)

    async def fake_fetch_balance(user_ctx):
        return rt["account_balance"]

    monkeypatch.setattr(zm, "send_message_v2", fake_send_message_v2)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "fetch_balance", fake_fetch_balance)

    class DummyClient:
        async def send_message(self, target, message, parse_mode=None):
            return SimpleNamespace(chat_id=target, id=1)

        async def delete_messages(self, chat_id, message_id):
            return None

    event = SimpleNamespace(message=SimpleNamespace(message="已结算: 结果为 9 大"))
    asyncio.run(zm.process_settle(DummyClient(), event, ctx, {}))

    assert captured["type"] == "lose_streak"
    assert "⚠️ 1 连输告警 ⚠️" in captured["message"]
    assert "第 1 轮第 1 次" in captured["message"]
    assert "💰 账户余额：" in captured["message"]
    assert "🤖 当局 AI 预测提示" not in captured["message"]


def test_process_settle_lose_end_message_contains_balance_lines(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5007"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "回补用户"},
            "telegram": {"user_id": 5007},
            "groups": {"admin_chat": 5007},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["bet"] = True
    rt["bet_type"] = 1  # 押大，下面开大 -> 赢
    rt["bet_amount"] = 1000
    rt["warning_lose_count"] = 1
    rt["lose_count"] = 3
    rt["lose_notify_pending"] = True
    rt["lose_start_info"] = {"round": 1, "seq": 5, "fund": 24_566_390}
    rt["current_round"] = 1
    rt["current_bet_seq"] = 10
    rt["account_balance"] = 24_634_900
    rt["gambling_fund"] = 24_567_390
    ctx.state.bet_sequence_log = [{"bet_id": "20260224_1_9", "profit": None}]

    captured = {}

    async def fake_send_message_v2(client, msg_type, message, user_ctx, global_cfg, parse_mode="markdown", title=None, desp=None):
        if msg_type == "lose_end":
            captured["message"] = message
        return None

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        return SimpleNamespace(chat_id=5007, id=1)

    async def fake_fetch_balance(user_ctx):
        return rt["account_balance"]

    monkeypatch.setattr(zm, "send_message_v2", fake_send_message_v2)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "fetch_balance", fake_fetch_balance)

    class DummyClient:
        async def send_message(self, target, message, parse_mode=None):
            return SimpleNamespace(chat_id=target, id=1)

        async def delete_messages(self, chat_id, message_id):
            return None

    event = SimpleNamespace(message=SimpleNamespace(message="已结算: 结果为 9 大"))
    asyncio.run(zm.process_settle(DummyClient(), event, ctx, {}))

    msg = captured["message"]
    assert "✅ 连输已终止！✅" in msg
    assert "🔢 " in msg and "第 1 轮第 5 次 至 第 9 次" in msg
    assert "😀 连续押注：4 次" in msg
    assert "⚠️本局连输： 3 次" in msg
    assert "💰 本局盈利： 1,990" in msg
    assert "💰 账户余额：2463.49 万" in msg
    assert "💰 菠菜资金剩余：2456.84 万" in msg


def test_st_command_triggers_auto_yc_report(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5008"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "预设测算用户"},
            "telegram": {"user_id": 5008},
            "groups": {"admin_chat": 5008},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=5008, id=len(sent_messages))

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    cmd_event = SimpleNamespace(raw_text="st yc05", chat_id=5008, id=21)
    asyncio.run(zm.process_user_command(SimpleNamespace(), cmd_event, ctx, {}))

    assert ctx.state.runtime.get("current_preset_name") == "yc05"
    assert any("预设启动成功: yc05" in msg for msg in sent_messages)
    assert any("🔮 已根据当前预设自动测算" in msg for msg in sent_messages)
    assert any("🎯 策略参数" in msg for msg in sent_messages)
    assert any("连数|倍率|下注| 盈利 |所需本金" in msg for msg in sent_messages)


def test_xx_command_cleans_messages_in_config_groups(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5009"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "清理用户"},
            "telegram": {"user_id": 5009},
            "groups": {"admin_chat": 5009, "zq_group": [111], "monitor": [222]},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))

    sent_messages = []
    deleted_calls = []

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent_messages.append(message)
        return SimpleNamespace(chat_id=5009, id=len(sent_messages))

    def fake_create_task(coro):
        coro.close()
        return None

    class DummyClient:
        def iter_messages(self, chat_id, from_user=None, limit=None):
            async def _gen():
                sample = {111: [1, 2, 3], 222: [10]}
                for msg_id in sample.get(chat_id, []):
                    yield SimpleNamespace(id=msg_id)

            return _gen()

        async def delete_messages(self, chat_id, message_ids):
            deleted_calls.append((chat_id, list(message_ids)))

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)

    cmd_event = SimpleNamespace(raw_text="xx", chat_id=5009, id=30)
    asyncio.run(zm.process_user_command(DummyClient(), cmd_event, ctx, {}))

    assert (111, [1, 2, 3]) in deleted_calls
    assert (222, [10]) in deleted_calls
    assert any("群组消息已清理" in msg for msg in sent_messages)
    assert any("删除消息：4" in msg for msg in sent_messages)


def test_process_red_packet_claim_success_sends_admin_notice(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5010"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "红包用户"},
            "telegram": {"user_id": 5010},
            "groups": {"admin_chat": 5010, "zq_bot": 9001},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    sent = {}

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent["message"] = message
        return SimpleNamespace(chat_id=5010, id=1)

    async def fake_sleep(*args, **kwargs):
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "sleep", fake_sleep)

    class DummyClient:
        async def __call__(self, request):
            return SimpleNamespace(message="已获得 88 灵石")

    class DummyButton:
        data = b"red-packet"

    class DummyRow:
        buttons = [DummyButton()]

    class DummyMarkup:
        rows = [DummyRow()]

    class DummyEvent:
        sender_id = 9001
        raw_text = "恭喜领取灵石红包"
        text = "恭喜领取灵石红包"
        chat_id = -10001
        id = 99
        reply_markup = DummyMarkup()

        def __init__(self):
            self.clicked = []

        async def click(self, *args):
            self.clicked.append(args)

    event = DummyEvent()
    asyncio.run(zm.process_red_packet(DummyClient(), event, ctx, {}))

    assert event.clicked
    assert sent.get("message") == "🎉 抢到红包88灵石！"


def test_process_red_packet_ignores_game_message(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "5012"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "游戏过滤用户"},
            "telegram": {"user_id": 5012},
            "groups": {"admin_chat": 5012, "zq_bot": 9001},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    sent = {"called": False}

    async def fake_send_to_admin(client, message, user_ctx, global_cfg):
        sent["called"] = True
        return SimpleNamespace(chat_id=5012, id=1)

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    class DummyClient:
        async def __call__(self, request):
            raise AssertionError("游戏消息不应触发红包回调请求")

    class DummyButton:
        data = b"game-start"
        text = "开始游戏"

    class DummyRow:
        buttons = [DummyButton()]

    class DummyMarkup:
        rows = [DummyRow()]

    class DummyEvent:
        sender_id = 9001
        raw_text = "灵石对战游戏开始啦"
        text = "灵石对战游戏开始啦"
        chat_id = -10001
        id = 109
        reply_markup = DummyMarkup()

        def __init__(self):
            self.clicked = []

        async def click(self, *args):
            self.clicked.append(args)

    event = DummyEvent()
    asyncio.run(zm.process_red_packet(DummyClient(), event, ctx, {}))

    assert event.clicked == []
    assert sent["called"] is False
