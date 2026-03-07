import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import self_learning_engine
import tg_watch
import zq_multiuser as zm
from user_manager import UserContext, clear_registered_user_contexts


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name, user_id):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": user_id},
            "notification": {"watch": {"admin_chat": f"-90{user_id}"}},
        },
    )
    return UserContext(str(user_dir))


def test_build_watch_subviews_and_learn_details(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "query-user", 9801)
    rt = ctx.state.runtime
    rt["current_fk1_action_text"] = "观望"
    rt["current_fk1_tier_cap"] = "mid"
    rt["stop_count"] = 2
    rt["fund_pause_notified"] = True
    rt["last_blocked_by"] = "fk2"
    rt["shadow_probe_active"] = True
    rt["current_preset_name"] = "yc10"
    rt["package_current_name"] = "午盘包"
    rt["package_current_status"] = "running"
    rt["task_current_name"] = "午盘任务"
    rt["task_current_progress_bets"] = 3
    rt["task_current_target_bets"] = 8
    rt["task_current_trigger_mode"] = "auto"
    rt["task_last_action"] = "takeover"
    rt["task_last_reason"] = "延续盘"
    rt["task_last_event_at"] = "2026-03-07 12:00:00"
    rt["gambling_fund"] = 880000
    rt["account_balance"] = 930000
    rt["earnings"] = 12000
    rt["period_profit"] = 3500
    rt["profit"] = 100000
    rt["profit_stop"] = 2
    rt["bet_amount"] = 500
    rt["balance_status"] = "success"

    monkeypatch.setattr(
        tg_watch,
        "_build_watch_evidence",
        lambda user_ctx: {
            "overview_24h": {"pnl_total": -5000, "max_drawdown": 6200, "settled_count": 18, "win_rate": 0.5},
            "recent_temperature": {"level": "cold"},
        },
    )

    self_learning_engine._write_learning_center(
        ctx,
        {
            "version": 1,
            "learning_id": f"learn_{ctx.user_id}",
            "sequence": 1,
            "last_generated_at": "2026-03-07 10:00:00",
            "last_generated_candidate_id": "lc_9801_001",
            "last_shadow_recorded_at": "2026-03-07 10:10:00",
            "last_promotion_event_at": "2026-03-07 10:20:00",
            "active_shadow_candidate_id": "lc_9801_001",
            "active_gray_candidate_id": "lc_9801_002",
            "promoted_candidate_id": "lc_9801_003",
            "candidates": [
                {
                    "candidate_id": "lc_9801_001",
                    "candidate_version": "c1",
                    "candidate_no": 1,
                    "status": self_learning_engine.LEARNING_STATUS_SHADOW,
                    "created_at": "2026-03-07 10:00:00",
                    "updated_at": "2026-03-07 10:05:00",
                    "rule_name": "影子规则",
                },
                {
                    "candidate_id": "lc_9801_002",
                    "candidate_version": "c2",
                    "candidate_no": 2,
                    "status": self_learning_engine.LEARNING_STATUS_GRAY,
                    "created_at": "2026-03-07 10:00:00",
                    "updated_at": "2026-03-07 10:15:00",
                    "rule_name": "灰度规则",
                    "gray_policy_version": "v4",
                    "gray_target_user_name": "query-user",
                },
                {
                    "candidate_id": "lc_9801_003",
                    "candidate_version": "c3",
                    "candidate_no": 3,
                    "status": self_learning_engine.LEARNING_STATUS_PROMOTED,
                    "created_at": "2026-03-07 10:00:00",
                    "updated_at": "2026-03-07 10:20:00",
                    "rule_name": "转正规则",
                    "last_evaluation_status": "pass",
                    "last_score_total": 88,
                },
            ],
        },
    )
    monkeypatch.setattr(
        self_learning_engine,
        "_shadow_metrics",
        lambda user_ctx, candidate_id: {
            "sample_size": 12,
            "delta_pnl": 1500,
            "delta_drawdown": 800,
            "status": self_learning_engine.LEARNING_SHADOW_PASS,
        },
    )

    risk_text = tg_watch.build_watch_risk_text(ctx)
    task_text = tg_watch.build_watch_task_text(ctx)
    funds_text = tg_watch.build_watch_funds_text(ctx)
    learn_text = tg_watch.build_watch_learn_text(ctx)

    assert "当前建议：观望 | 限档 mid" in risk_text
    assert "资金暂停 是" in risk_text
    assert "任务包：午盘包 | 状态 running" in task_text
    assert "任务：午盘任务 | 进度 3/8" in task_text
    assert "菠菜资金：880,000 | 账户余额：930,000" in funds_text
    assert "余额状态：正常" in funds_text
    assert "最新 c3 (lc_9801_003)" in learn_text
    assert "影子：c1 (lc_9801_001)" in learn_text
    assert "灰度：c2 (lc_9801_002) | 策略 v4 | 目标 query-user" in learn_text


def test_process_user_command_watch_target_and_subviews_use_override(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    master = _make_user_context(tmp_path, "master", 9802)
    target = _make_user_context(tmp_path, "target", 9803)
    sent = []

    def fake_create_task(coro):
        coro.close()
        return None

    async def fake_send_to_watch(client, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None, account_name_override=None):
        sent.append({"message": message, "override": account_name_override})
        return SimpleNamespace(chat_id=-1, id=1)

    monkeypatch.setattr(zm, "send_to_watch", fake_send_to_watch)
    monkeypatch.setattr(zm, "_watch_reply_visible_in_chat", lambda user_ctx, chat_id: True)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(tg_watch, "build_watch_risk_text", lambda user_ctx: f"RISK:{user_ctx.user_id}")
    monkeypatch.setattr(tg_watch, "build_watch_overview_text", lambda user_ctx: f"OVERVIEW:{user_ctx.user_id}")

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="watch risk target", chat_id=1, id=11),
            master,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="watch target", chat_id=1, id=12),
            master,
            {},
        )
    )

    assert sent[0]["message"] == f"RISK:{target.user_id}"
    assert sent[0]["override"] == "target"
    assert sent[1]["message"] == f"OVERVIEW:{target.user_id}"
    assert sent[1]["override"] == "target"


def test_watch_quiet_commands_and_status(tmp_path):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "quiet-user", 9804)

    result = tg_watch.set_watch_quiet(ctx, 15)
    status = tg_watch.get_watch_quiet_status(ctx)
    cleared = tg_watch.clear_watch_quiet(ctx)

    assert result["active"] is True
    assert "静音 15 分钟" in result["message"]
    assert status["active"] is True
    assert status["remaining_min"] >= 14
    assert cleared["active"] is False
    assert tg_watch.get_watch_quiet_status(ctx)["active"] is False


def test_emit_watch_event_respects_quiet_mode(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "quiet-emit", 9805)
    sent = []
    monkeypatch.setattr(tg_watch, "_now_ts", lambda: 100)
    tg_watch.set_watch_quiet(ctx, 30)

    async def fake_send_to_watch(client, message, user_ctx, global_config, parse_mode="markdown", title=None, desp=None, account_name_override=None):
        sent.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_watch", fake_send_to_watch)

    notified = asyncio.run(
        zm._emit_watch_event(
            SimpleNamespace(),
            ctx,
            {},
            "model_timeout",
            "模型超时",
            severity="warning",
        )
    )

    assert notified is False
    assert sent == []
    alerts = tg_watch.list_watch_alerts(ctx, 5)
    assert alerts
    assert alerts[-1]["message"] == "模型超时"


def test_watch_quiet_command_routes_to_admin(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    ctx = _make_user_context(tmp_path, "quiet-cmd", 9806)
    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    asyncio.run(
        zm.process_user_command(
            None,
            SimpleNamespace(raw_text="watch quiet 20", chat_id=1, id=21),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            SimpleNamespace(raw_text="watch quiet", chat_id=1, id=22),
            ctx,
            {},
        )
    )
    asyncio.run(
        zm.process_user_command(
            None,
            SimpleNamespace(raw_text="watch quiet off", chat_id=1, id=23),
            ctx,
            {},
        )
    )

    assert "静音 20 分钟" in sent_messages[0]
    assert "当前值守主动播报已静音" in sent_messages[1]
    assert "值守主动播报已恢复" in sent_messages[2]
