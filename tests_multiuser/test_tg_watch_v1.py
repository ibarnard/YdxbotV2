import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import tg_watch
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


def test_build_watch_overview_text_includes_key_fields(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8104"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "摘要用户"},
            "telegram": {"user_id": 8104},
            "groups": {"admin_chat": 8104},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["current_preset_name"] = "yc10"
    rt["task_current_name"] = "午盘"
    rt["earnings"] = 36000
    rt["gambling_fund"] = 880000
    rt["account_balance"] = 930000
    rt["current_fk1_action_text"] = "观望"
    rt["last_predict_tag"] = "STABILITY"
    rt["last_predict_confidence"] = 82
    rt["last_predict_source"] = "model"

    monkeypatch.setattr(tg_watch, "_policy_brief", lambda user_ctx: "v3(基线)")
    monkeypatch.setattr(tg_watch, "_task_brief", lambda runtime: "午盘")
    monkeypatch.setattr(tg_watch, "_learning_brief", lambda user_ctx: "shadow c2")
    monkeypatch.setattr(
        tg_watch,
        "_build_watch_evidence",
        lambda user_ctx: {
            "current_regime": "延续盘",
            "similar_cases": {"similar_count": 17, "recommended_tier_cap": "mid"},
            "recent_temperature": {"level": "cold"},
            "overview_24h": {
                "win_rate": 0.58,
                "pnl_total": 12800,
                "max_drawdown": 6400,
                "settled_count": 24,
            },
        },
    )

    text = tg_watch.build_watch_overview_text(ctx)

    assert "👀 值守摘要" in text
    assert "预设：yc10 | 任务 午盘 | 策略 v3(基线)" in text
    assert "学习：shadow c2 | 当前建议 观望" in text
    assert "24h：胜率 58.0% | 盈亏 +12,800 | 回撤 6,400 | 样本 24" in text
    assert "盘面：延续盘 | 温度 偏冷 | 相似 17 | 历史建议 mid" in text
    assert "最近决策：STABILITY / 82% / model" in text


def test_process_user_command_watch_routes_summary_to_watch_channel(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8105"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "命令用户"},
            "telegram": {"user_id": 8105},
            "groups": {"admin_chat": 8105},
        },
    )
    ctx = UserContext(str(user_dir))
    sent = {}
    acked = {}

    async def fake_send_to_watch(client, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None):
        sent["message"] = message
        return SimpleNamespace(chat_id=-9005, id=3)

    async def fake_send_watch_ack(client, event, text):
        acked["text"] = text
        return SimpleNamespace(chat_id=event.chat_id, id=4)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "send_to_watch", fake_send_to_watch)
    monkeypatch.setattr(zm, "_send_watch_command_ack", fake_send_watch_ack)
    monkeypatch.setattr(zm, "_watch_reply_visible_in_chat", lambda user_ctx, chat_id: False)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(tg_watch, "build_watch_overview_text", lambda user_ctx: "WATCH_BODY")

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="watch", chat_id=8105, id=1),
            ctx,
            {},
        )
    )

    assert sent["message"] == "WATCH_BODY"
    assert acked["text"] == "👀 值守摘要已发送到值守通道"


def test_process_user_command_watch_fleet_routes_fleet_summary(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8106"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "多账号命令用户"},
            "telegram": {"user_id": 8106},
            "groups": {"admin_chat": 8106},
        },
    )
    ctx = UserContext(str(user_dir))
    sent = {}

    async def fake_send_to_watch(client, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None):
        sent["message"] = message
        return SimpleNamespace(chat_id=-9006, id=5)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "send_to_watch", fake_send_to_watch)
    monkeypatch.setattr(zm, "_watch_reply_visible_in_chat", lambda user_ctx, chat_id: True)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(tg_watch, "build_watch_fleet_text", lambda user_ctx: "WATCH_FLEET")

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="watch fleet", chat_id=8106, id=2),
            ctx,
            {},
        )
    )

    assert sent["message"] == "WATCH_FLEET"


def test_record_watch_event_throttles_same_fingerprint(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8107"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "节流用户"},
            "telegram": {"user_id": 8107},
            "groups": {"admin_chat": 8107},
        },
    )
    ctx = UserContext(str(user_dir))
    timeline = iter([100, 100, 100, 100, 120, 120])
    textline = iter(
        [
            "2026-03-07 10:00:00",
            "2026-03-07 10:00:00",
            "2026-03-07 10:00:00",
            "2026-03-07 10:00:00",
            "2026-03-07 10:02:00",
            "2026-03-07 10:02:00",
        ]
    )

    monkeypatch.setattr(tg_watch, "_now_ts", lambda: next(timeline))
    monkeypatch.setattr(tg_watch, "_now_text", lambda: next(textline))

    first = tg_watch.record_watch_event(ctx, "fund_pause", "A", fingerprint="same", throttle_sec=300)
    second = tg_watch.record_watch_event(ctx, "fund_pause", "A", fingerprint="same", throttle_sec=300)
    third = tg_watch.record_watch_event(ctx, "fund_pause", "B", fingerprint="other", throttle_sec=300)

    alerts = tg_watch.list_watch_alerts(ctx, 10)

    assert first["should_notify"] is True
    assert second["should_notify"] is False
    assert third["should_notify"] is True
    assert len(alerts) == 2
    assert alerts[0]["count"] == 2
    assert alerts[1]["fingerprint"] == "other"


def test_process_user_command_learn_promote_emits_watch_event(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8108"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "学习播报用户"},
            "telegram": {"user_id": 8108},
            "groups": {"admin_chat": 8108},
        },
    )
    ctx = UserContext(str(user_dir))
    sent = {}
    emitted = {}

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent["message"] = message
        return None

    async def fake_emit_learning_watch_event(client, user_ctx, global_config, event_type, result, severity="info", throttle_sec=60):
        emitted["event_type"] = event_type
        emitted["message"] = result["message"]
        return True

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "_emit_learning_watch_event", fake_emit_learning_watch_event)
    monkeypatch.setattr(
        zm.self_learning_engine,
        "promote_candidate",
        lambda user_ctx, ident="": {
            "ok": True,
            "message": "PROMOTE_MSG",
            "candidate": {"candidate_id": "cand_1"},
        },
    )

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="learn promote c1", chat_id=8108, id=1),
            ctx,
            {},
        )
    )

    assert sent["message"] == "PROMOTE_MSG"
    assert emitted["event_type"] == "learn_promote"
    assert emitted["message"] == "PROMOTE_MSG"


def test_check_bet_status_fund_pause_emits_watch_event(tmp_path, monkeypatch):
    user_dir = tmp_path / "users" / "8109"
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": "资金播报用户"},
            "telegram": {"user_id": 8109},
            "groups": {"admin_chat": 8109},
            "notification": {"iyuu": {"enable": False}, "tg_bot": {"enable": False}},
        },
    )
    ctx = UserContext(str(user_dir))
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["bet"] = False
    rt["bet_on"] = True
    rt["stop_count"] = 0
    rt["fund_pause_notified"] = False
    rt["gambling_fund"] = 100
    rt["account_balance"] = 100

    sent = {}
    emitted = {}

    async def fake_send_message_v2(client, msg_type, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None):
        sent["msg_type"] = msg_type
        sent["message"] = message
        return None

    async def fake_emit_watch_event(client, user_ctx, global_config, event_type, message, severity="info", fingerprint="", throttle_sec=300, meta=None):
        emitted["event_type"] = event_type
        emitted["message"] = message
        return True

    async def fake_clear_pause_countdown_notice(client, user_ctx):
        return None

    monkeypatch.setattr(zm, "calculate_bet_amount", lambda runtime: 500)
    monkeypatch.setattr(zm, "is_fund_available", lambda user_ctx, amount: False)
    monkeypatch.setattr(zm, "_sync_fund_from_account_when_insufficient", lambda runtime, amount: False)
    monkeypatch.setattr(zm, "send_message_v2", fake_send_message_v2)
    monkeypatch.setattr(zm, "_emit_watch_event", fake_emit_watch_event)
    monkeypatch.setattr(zm, "_clear_pause_countdown_notice", fake_clear_pause_countdown_notice)

    asyncio.run(zm.check_bet_status(SimpleNamespace(), ctx, {}))

    assert sent["msg_type"] == "fund_pause"
    assert emitted["event_type"] == "fund_pause"
    assert "菠菜资金不足" in emitted["message"]
