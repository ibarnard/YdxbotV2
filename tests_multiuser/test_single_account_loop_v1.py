import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import main_multiuser as mm
import zq_multiuser as zm
from user_manager import UserContext, clear_registered_user_contexts


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="single-loop", user_id=9961):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
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
                "watch": {"admin_chat": f"-90{user_id}"},
            },
            "zhuque": {
                "cookie": "cookie=value",
                "csrf_token": "csrf",
                "api_url": "https://example.com/api",
            },
        },
    )
    return UserContext(str(user_dir))


class _DummySentMessage:
    def __init__(self, chat_id, message_id, text):
        self.chat_id = chat_id
        self.id = message_id
        self.text = text
        self.deleted = False

    async def delete(self):
        self.deleted = True
        return None


class _FakeClient:
    def __init__(self):
        self.handlers = {}
        self.sent_messages = []
        self.deleted_messages = []

    def on(self, *args, **kwargs):
        def decorator(func):
            self.handlers[func.__name__] = func
            return func

        return decorator

    async def connect(self):
        return None

    async def is_user_authorized(self):
        return True

    async def send_message(self, target, message, parse_mode=None):
        sent = _DummySentMessage(target, len(self.sent_messages) + 1, message)
        self.sent_messages.append(sent)
        return sent

    async def delete_messages(self, chat_id, message_id):
        self.deleted_messages.append((chat_id, message_id))
        return None


def test_single_account_startup_and_one_bet_settle_loop(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, user_id=9961)
    client = _FakeClient()
    admin_messages = []
    routed_messages = []
    clicked_buttons = []

    def fake_create_task(coro):
        coro.close()
        return None

    async def fake_create_client(user_ctx, global_config):
        return client

    async def fake_check_models_for_user(client_obj, user_ctx):
        return None

    async def fake_fetch_account_balance(user_ctx):
        return 1_000_000

    async def fake_send_to_admin(client_obj, message, user_ctx, global_config):
        admin_messages.append(message)
        return _DummySentMessage(user_ctx.user_id, len(admin_messages) + 100, message)

    async def fake_send_message_v2(client_obj, msg_type, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None):
        routed_messages.append((msg_type, message))
        return _DummySentMessage(user_ctx.user_id, len(routed_messages) + 200, message)

    async def fake_click_bet_button_with_recover(client_obj, event, user_ctx, button_data):
        clicked_buttons.append(button_data)
        return None

    async def fake_emit_watch_event(*args, **kwargs):
        return False

    async def fake_transient_notice(*args, **kwargs):
        return None

    def fake_refresh_snapshot(user_ctx):
        user_ctx.state.runtime["current_round"] = 1
        user_ctx.state.runtime["current_round_key"] = "rk_single_1"
        return {
            "round_key": "rk_single_1",
            "current_round_no": 1,
            "regime_label": zm.history_analysis.REGIME_CONTINUATION,
            "recent_temperature": {"level": "normal"},
            "similar_cases": {"similar_count": 8},
        }

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        return 1

    async def fake_fetch_balance(user_ctx):
        return int(user_ctx.state.runtime.get("account_balance", 1_000_000) or 1_000_000)

    monkeypatch.setattr(mm, "_acquire_session_lock", lambda user_ctx: True)
    monkeypatch.setattr(mm, "_release_session_lock", lambda user_ctx: None)
    monkeypatch.setattr(mm, "create_client", fake_create_client)
    monkeypatch.setattr(mm, "check_models_for_user", fake_check_models_for_user)
    monkeypatch.setattr(mm, "fetch_account_balance", fake_fetch_account_balance)

    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm, "send_message_v2", fake_send_message_v2)
    monkeypatch.setattr(zm, "_click_bet_button_with_recover", fake_click_bet_button_with_recover)
    monkeypatch.setattr(zm, "_emit_watch_event", fake_emit_watch_event)
    monkeypatch.setattr(zm, "_send_transient_admin_notice", fake_transient_notice)
    monkeypatch.setattr(zm, "_read_timing_config", lambda cfg: {
        "prompt_wait_sec": 0.0,
        "predict_timeout_sec": 3.0,
        "click_interval_sec": 0.0,
        "click_timeout_sec": 1.0,
    })
    monkeypatch.setattr(zm, "_refresh_current_analysis_snapshot", fake_refresh_snapshot)
    monkeypatch.setattr(zm, "predict_next_bet_v10", fake_predict_next_bet_v10)
    monkeypatch.setattr(zm, "fetch_balance", fake_fetch_balance)
    monkeypatch.setattr(zm, "append_replay_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(zm.history_analysis, "record_execution_action", lambda *args, **kwargs: None)
    monkeypatch.setattr(zm.history_analysis, "record_settlement", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk_single_1",
            "current_round_no": 1,
            "regime_label": zm.history_analysis.REGIME_CONTINUATION,
            "recent_temperature": {"level": "normal"},
            "similar_cases": {"similar_count": 8},
        },
    )
    monkeypatch.setattr(
        zm.history_analysis,
        "build_policy_evidence_package",
        lambda user_ctx, analysis_snapshot=None: {
            "overview_24h": {
                "settled_count": 1,
                "win_rate": 1.0,
                "pnl_total": 500,
                "max_drawdown": 0,
                "observe_count": 0,
                "blocked_count": 0,
            }
        },
    )
    monkeypatch.setattr(
        zm.risk_control,
        "evaluate_fk1",
        lambda snapshot, rt: {
            "action": "allow",
            "action_text": "可下注",
            "tier_cap": "",
            "reason_text": "单账号烟测放行",
        },
    )
    monkeypatch.setattr(zm.risk_control, "clamp_bet_amount_by_tier_cap", lambda amount, tier: (amount, ""))
    monkeypatch.setattr(
        zm.dynamic_betting,
        "evaluate_dynamic_bet",
        lambda snapshot, rt: {
            "base_tier": "yc10",
            "applied_tier": "yc10",
            "reason_text": "smoke",
            "action_text": "keep",
            "floor_tier": "yc10",
            "ceiling_tier": "yc10",
        },
    )
    monkeypatch.setattr(zm.dynamic_betting, "build_dynamic_summary", lambda payload: "")
    monkeypatch.setattr(zm.dynamic_betting, "clear_dynamic_decision", lambda rt: None)
    monkeypatch.setattr(zm.task_engine, "prepare_task_for_round", lambda user_ctx, snapshot: {"started": False})
    monkeypatch.setattr(zm.task_engine, "record_round_action", lambda *args, **kwargs: None)
    monkeypatch.setattr(zm.task_engine, "record_settlement", lambda *args, **kwargs: {"task_finished": False, "summary": ""})
    monkeypatch.setattr(zm.task_package_engine, "prepare_package_for_round", lambda user_ctx, snapshot: {"started": False})
    monkeypatch.setattr(zm.task_package_engine, "record_settlement", lambda *args, **kwargs: None)
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {500: "big_500"})
    monkeypatch.setattr(zm.constants, "SMALL_BUTTON", {500: "small_500"})
    monkeypatch.setattr(zm.constants, "find_combination", lambda amount, buttons: [500] if amount == 500 else [])

    started_client = asyncio.run(mm.start_user(ctx, {}))

    assert started_client is client
    assert {"bet_on_handler", "settle_handler", "red_packet_handler", "user_handler"}.issubset(set(client.handlers.keys()))
    assert ctx.state.runtime["gambling_fund"] == 1_000_000
    assert ctx.state.runtime["account_balance"] == 1_000_000
    assert client.sent_messages

    bet_event = SimpleNamespace(
        id=1,
        chat_id=123,
        raw_text="[近 40 次结果][由近及远][0 小 1 大]\n1 0 1 0 1 0 1 0",
        message=SimpleNamespace(message="[近 40 次结果][由近及远][0 小 1 大]\n1 0 1 0 1 0 1 0"),
        reply_markup=SimpleNamespace(rows=[]),
    )
    asyncio.run(client.handlers["bet_on_handler"](bet_event))

    assert clicked_buttons == ["big_500"]
    assert ctx.state.runtime["pending_bet_id"]
    assert ctx.state.runtime["total"] == 1
    assert ctx.state.runtime["bet_sequence_count"] == 1
    assert ctx.state.bet_sequence_log[-1]["status"] == "placed"
    assert any("Admin 驾驶舱" in text for text in admin_messages)

    settle_event = SimpleNamespace(
        id=2,
        chat_id=123,
        raw_text="已结算: 结果为 9 大",
        message=SimpleNamespace(message="已结算: 结果为 9 大"),
    )
    asyncio.run(client.handlers["settle_handler"](settle_event))

    assert ctx.state.history[-1] == 1
    assert ctx.state.runtime["pending_bet_id"] == ""
    assert ctx.state.runtime["bet_sequence_count"] == 0
    assert ctx.state.runtime["win_total"] == 1
    assert ctx.state.bet_sequence_log[-1]["status"] == "settled"
    assert ctx.dashboard_message is not None
    assert isinstance(routed_messages, list)
    assert sum(1 for text in admin_messages if "Admin 驾驶舱" in text) >= 2
