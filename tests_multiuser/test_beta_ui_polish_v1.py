import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import policy_engine
import zq_multiuser as zm
from user_manager import UserContext


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_user_context(tmp_path, name="UI测试用户", user_id=9911):
    user_dir = tmp_path / "users" / str(user_id)
    _write_json(
        user_dir / "config.json",
        {
            "account": {"name": name},
            "telegram": {"user_id": user_id},
            "groups": {"admin_chat": user_id},
            "ai": {
                "api_keys": ["test-key"],
                "models": {
                    "primary": {"model_id": "model-a", "enabled": True},
                    "backup": {"model_id": "model-b", "enabled": True},
                },
            },
        },
    )
    return UserContext(str(user_dir))


def test_build_policy_overview_text_is_compact_summary(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9912)
    store = {
        "policy_id": "risk_policy",
        "active_version": "v4",
        "previous_version": "v3",
        "last_synced_at": "2026-03-08 09:00:00",
        "policies": [
            {
                "policy_id": "risk_policy",
                "policy_version": "v4",
                "activation_mode": "gray",
                "summary": "延续盘偏热时收紧高档位",
                "writeback_lines": ["延续盘偏热时上限不超过 yc20"],
                "evidence_package": {
                    "current_regime": "延续盘",
                    "similar_cases": {"similar_count": 12},
                    "overview_24h": {"settled_count": 18},
                },
            }
        ],
    }

    monkeypatch.setattr(policy_engine, "load_policy_store", lambda user_ctx: store)
    monkeypatch.setattr(policy_engine, "_update_runtime_policy_snapshot", lambda *args, **kwargs: None)

    text = policy_engine.build_policy_overview_text(ctx)

    assert "🧠 策略版本中心" in text
    assert "当前版本：v4（灰度）" in text
    assert "证据：延续盘 | 相似样本 12 | 24h 18 笔" in text
    assert "当前建议：灰度版本运行中，先盯表现，不要急着切换。" in text
    assert max(len(line) for line in text.splitlines()) <= 80


def test_process_user_command_ver_returns_compact_panel(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9913)
    sent = {}

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent["message"] = message
        return SimpleNamespace(chat_id=ctx.user_id, id=7)

    def fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)
    monkeypatch.setattr(zm.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(
        zm,
        "list_version_catalog",
        lambda repo_root=None, limit=3: {
            "success": True,
            "current": {"current_tag": "v2.0.1-beta.1", "short_commit": "db68e64"},
            "remote_head": {"short_commit": "b95ffab"},
            "remote_head_tag": "v2.0.1-beta.2",
            "pending_tags": ["v2.0.1-beta.2"],
            "recent_tags": [
                {"tag": "v2.0.1-beta.2", "date": "2026-03-08", "summary": "beta polish"},
                {"tag": "v2.0.1-beta.1", "date": "2026-03-07", "summary": "beta base"},
            ],
            "recent_commits": [
                {"short_commit": "b95ffab", "date": "2026-03-08", "summary": "beta polish"},
                {"short_commit": "db68e64", "date": "2026-03-07", "summary": "merge main"},
            ],
        },
    )

    asyncio.run(
        zm.process_user_command(
            SimpleNamespace(),
            SimpleNamespace(raw_text="ver", chat_id=ctx.user_id, id=1),
            ctx,
            {},
        )
    )

    assert "📦 版本信息概览" in sent["message"]
    assert "当前：v2.0.1-beta.1 | db68e64" in sent["message"]
    assert "可更新 Tag：v2.0.1-beta.2（`update v2.0.1-beta.2`）" in sent["message"]
    assert "最近 Commit（新 -> 旧）：" in sent["message"]
    assert max(len(line) for line in sent["message"].splitlines()) <= 90


def test_handle_model_command_uses_model_cards(tmp_path, monkeypatch):
    ctx = _make_user_context(tmp_path, user_id=9914)
    ctx.state.runtime["current_model_id"] = "model-a"
    messages = []

    async def fake_reply(client, event, message, user_ctx, global_config, parse_mode="markdown"):
        messages.append(message)
        return None

    monkeypatch.setattr(zm, "_reply_admin_command_result", fake_reply)

    asyncio.run(zm.handle_model_command_multiuser(None, SimpleNamespace(), ["list"], ctx, {}))
    asyncio.run(zm.handle_model_command_multiuser(None, SimpleNamespace(), ["select", "2"], ctx, {}))

    assert "🤖 模型卡" in messages[0]
    assert "当前：`model-a`" in messages[0]
    assert "1. `model-a` （当前）" in messages[0]
    assert "2. `model-b`" in messages[0]
    assert "动作：切换模型" in messages[1]
    assert "动作：切换模型完成" in messages[2]
    assert "当前：`model-b`" in messages[2]
    assert "下一步：后续预测将使用新模型" in messages[2]
