import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import dynamic_betting
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(__import__("json").dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="动态测试", user_id=9201):
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


def test_evaluate_dynamic_bet_caps_new_sequence_for_chaos():
    rt = {
        "current_preset_name": "yc50",
        "current_fk1_tier_cap": "",
        "lose_count": 0,
        "dynamic_sequence_start_tier": "",
    }
    snapshot = {
        "regime_label": "混乱盘",
        "similar_cases": {
            "similar_count": 42,
            "evidence_strength": "strong",
            "recommended_tier_cap": "yc5",
            "tiers": {
                "high": {"avg_pnl": -12000, "win_rate": 0.45},
                "low": {"avg_pnl": 1800, "win_rate": 0.54},
            },
        },
        "recent_temperature": {"level": "normal"},
    }

    result = dynamic_betting.evaluate_dynamic_bet(snapshot, rt)

    assert result["base_tier"] == "yc50"
    assert result["applied_tier"] == "yc1"
    assert result["adjusted"] is True


def test_evaluate_dynamic_bet_keeps_sequence_floor():
    rt = {
        "current_preset_name": "yc50",
        "current_fk1_tier_cap": "yc5",
        "lose_count": 2,
        "dynamic_sequence_start_tier": "yc20",
    }
    snapshot = {
        "regime_label": "混乱盘",
        "similar_cases": {
            "similar_count": 52,
            "evidence_strength": "strong",
            "recommended_tier_cap": "yc5",
            "tiers": {
                "high": {"avg_pnl": -22000, "win_rate": 0.42},
                "low": {"avg_pnl": 1200, "win_rate": 0.51},
            },
        },
        "recent_temperature": {"level": "cold"},
    }

    result = dynamic_betting.evaluate_dynamic_bet(snapshot, rt)

    assert result["suggested_tier"] == "yc1"
    assert result["applied_tier"] == "yc20"
    assert result["floor_locked"] is True


def test_process_bet_on_dynamic_tier_clamps_real_amount(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9202)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 60
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 3
    rt["current_preset_name"] = "yc50"
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False
    rt["gambling_fund"] = 1_000_000

    clicked_buttons = []

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 72
        user_ctx.state.runtime["last_predict_tag"] = "CHAOS"
        user_ctx.state.runtime["last_predict_info"] = "短线混乱"
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        return SimpleNamespace(chat_id=1, id=1)

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    async def fake_notice(*args, **kwargs):
        return None

    async def fake_click(client, event, user_ctx, button_data):
        clicked_buttons.append(button_data)
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
    monkeypatch.setattr(zm, "calculate_bet_amount", lambda runtime: 50000)
    monkeypatch.setattr(zm, "generate_mobile_bet_report", lambda *args, **kwargs: "BET_REPORT")
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:dyn:59:3",
            "regime_label": "混乱盘",
            "features": {},
            "similar_cases": {
                "similar_count": 45,
                "evidence_strength": "strong",
                "recommended_tier_cap": "yc5",
                "tiers": {
                    "high": {"avg_pnl": -10000, "win_rate": 0.44},
                    "low": {"avg_pnl": 1500, "win_rate": 0.53},
                },
                "source": "analytics",
            },
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
            "reason_text": "延续观察通过",
            "regime_label": "混乱盘",
        },
    )
    monkeypatch.setattr(zm.constants, "find_combination", lambda amount, buttons: [amount])
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {1000: "BIG1000", 5000: "BIG5000", 50000: "BIG50000"})

    event = SimpleNamespace(
        reply_markup=True,
        chat_id=1,
        id=1,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(60))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    db_path = Path(ctx.user_dir) / "analytics.db"
    assert rt["bet"] is True
    assert rt["bet_amount"] == 1000
    assert rt["current_dynamic_tier"] == "yc1"
    assert rt["dynamic_sequence_start_tier"] == "yc1"
    assert db_path.exists()
    assert _table_count(db_path, "execution_records") == 1
    assert clicked_buttons == ["BIG1000"]


def test_process_bet_on_dynamic_floor_keeps_first_tier(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9203)
    rt = ctx.state.runtime
    ctx.state.history = [1] * 60
    rt["switch"] = True
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["current_round"] = 4
    rt["current_preset_name"] = "yc50"
    rt["fk2_enabled"] = False
    rt["risk_deep_enabled"] = False
    rt["gambling_fund"] = 1_000_000
    rt["lose_count"] = 2
    rt["bet_amount"] = 20000
    rt["dynamic_sequence_start_tier"] = "yc20"

    clicked_buttons = []

    async def fake_predict_next_bet_v10(user_ctx, global_config):
        user_ctx.state.runtime["last_predict_source"] = "model"
        user_ctx.state.runtime["last_predict_confidence"] = 70
        user_ctx.state.runtime["last_predict_tag"] = "CHAOS"
        user_ctx.state.runtime["last_predict_info"] = "混乱盘"
        return 1

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        return SimpleNamespace(chat_id=1, id=1)

    async def fake_delete_later(client, chat_id, message_id, delay):
        return None

    async def fake_notice(*args, **kwargs):
        return None

    async def fake_click(client, event, user_ctx, button_data):
        clicked_buttons.append(button_data)
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
    monkeypatch.setattr(zm, "calculate_bet_amount", lambda runtime: 56000)
    monkeypatch.setattr(zm, "generate_mobile_bet_report", lambda *args, **kwargs: "BET_REPORT")
    monkeypatch.setattr(
        zm.history_analysis,
        "build_current_analysis_snapshot",
        lambda user_ctx: {
            "round_key": "rk:dyn:60:4",
            "regime_label": "混乱盘",
            "features": {},
            "similar_cases": {
                "similar_count": 45,
                "evidence_strength": "strong",
                "recommended_tier_cap": "yc5",
                "tiers": {
                    "high": {"avg_pnl": -10000, "win_rate": 0.44},
                    "low": {"avg_pnl": 1500, "win_rate": 0.53},
                },
                "source": "analytics",
            },
            "recent_temperature": {"level": "cold"},
        },
    )
    monkeypatch.setattr(
        zm.risk_control,
        "evaluate_fk1",
        lambda snapshot, runtime: {
            "action": "allow",
            "action_text": "盘面风控通过，按当前策略执行",
            "tier_cap": "",
            "reason_text": "继续跟踪",
            "regime_label": "混乱盘",
        },
    )
    monkeypatch.setattr(zm.constants, "find_combination", lambda amount, buttons: [amount])
    monkeypatch.setattr(zm.constants, "BIG_BUTTON", {20000: "BIG20000"})

    event = SimpleNamespace(
        reply_markup=True,
        chat_id=1,
        id=1,
        message=SimpleNamespace(message="[0 小 1 大] " + " ".join("1" for _ in range(60))),
    )

    asyncio.run(zm.process_bet_on(SimpleNamespace(), event, ctx, {}))

    assert rt["bet"] is True
    assert rt["bet_amount"] == 20000
    assert rt["current_dynamic_tier"] == "yc20"
    assert clicked_buttons == ["BIG20000"]
