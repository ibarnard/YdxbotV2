import asyncio
import json
from pathlib import Path

import multi_account_orchestrator
import policy_engine
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
        },
    )
    return UserContext(str(user_dir))


def _prepare_account(ctx, preset_name, task_name="", package_name=""):
    rt = ctx.state.runtime
    rt["switch"] = True
    rt["bet_on"] = True
    rt["current_preset_name"] = preset_name
    rt["earnings"] = 12345
    rt["total"] = 10
    rt["win_total"] = 6
    rt["task_current_name"] = task_name
    rt["package_current_name"] = package_name
    ctx.state.history = [1, 1, 1, 0, 1, 1, 1, 0] * 10


def test_build_fleet_overview_and_policy_switch(tmp_path):
    clear_registered_user_contexts()
    master = _make_user_context(tmp_path, "master", 9601)
    xu = _make_user_context(tmp_path, "xu", 9602)

    _prepare_account(master, "yc10", task_name="巡航A")
    _prepare_account(xu, "yc5", package_name="稳健包")
    policy_engine.sync_policy_from_evidence(xu)

    overview = multi_account_orchestrator.build_fleet_overview_text(master)
    policy_text = multi_account_orchestrator.build_fleet_policy_text(master)

    assert "master (9601)" in overview
    assert "xu (9602)" in overview
    assert "策略" in overview
    assert "灰度" in policy_text

    result = multi_account_orchestrator.switch_account_policy_mode(master, "xu", "baseline")
    assert result["ok"] is True
    assert xu.state.runtime["policy_active_version"] == "v1"


def test_process_user_command_fleet(tmp_path, monkeypatch):
    clear_registered_user_contexts()
    master = _make_user_context(tmp_path, "master", 9603)
    xu = _make_user_context(tmp_path, "xu", 9604)
    _prepare_account(master, "yc20")
    _prepare_account(xu, "yc5", task_name="趋势任务")

    sent_messages = []

    async def fake_send_to_admin(client, message, user_ctx, global_config):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(zm, "send_to_admin", fake_send_to_admin)

    event = type(
        "E",
        (),
        {
            "raw_text": "fleet task",
            "chat_id": 1,
            "id": 1,
        },
    )()

    asyncio.run(zm.process_user_command(None, event, master, {}))

    assert sent_messages
    assert "多账号任务视图" in sent_messages[-1]
    assert "趋势任务" in sent_messages[-1]
