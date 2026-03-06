from __future__ import annotations

from typing import Any, Dict, List, Optional

import policy_engine
import risk_control
from user_manager import get_registered_user_contexts


def _all_users(current_user_ctx) -> Dict[int, Any]:
    users = get_registered_user_contexts()
    if not users and current_user_ctx is not None:
        users[int(current_user_ctx.user_id)] = current_user_ctx
    return dict(sorted(users.items(), key=lambda item: item[0]))


def _account_name(user_ctx) -> str:
    return str(getattr(getattr(user_ctx, "config", None), "name", "") or f"user-{getattr(user_ctx, 'user_id', 0)}")


def _status_text(rt: Dict[str, Any]) -> str:
    if bool(rt.get("manual_pause", False)):
        return "手动暂停"
    if not bool(rt.get("switch", True)):
        return "已关闭"
    if bool(rt.get("bet_on", False)):
        return "运行中"
    return "已暂停"


def _policy_brief(user_ctx) -> str:
    store = policy_engine.load_policy_store(user_ctx)
    active = policy_engine._find_policy_version(store, store.get("active_version", ""))  # type: ignore[attr-defined]
    if not active:
        return "无策略"
    mode_text = "基线" if str(active.get("activation_mode", "baseline")) == "baseline" else "灰度"
    return f"{active.get('policy_version', 'v1')}({mode_text})"


def _task_brief(rt: Dict[str, Any]) -> str:
    package_name = str(rt.get("package_current_name", "") or "")
    task_name = str(rt.get("task_current_name", "") or "")
    if package_name and task_name:
        return f"{package_name}/{task_name}"
    if package_name:
        return f"{package_name}/待切换"
    if task_name:
        return task_name
    return "无"


def _match_user(current_user_ctx, ident: str) -> Optional[Any]:
    users = _all_users(current_user_ctx)
    target = str(ident or "").strip().lower()
    if not target:
        return None
    if target.isdigit():
        return users.get(int(target))
    exact_name = [
        user_ctx
        for user_ctx in users.values()
        if _account_name(user_ctx).strip().lower() == target
    ]
    if exact_name:
        return exact_name[0]
    partial = [
        user_ctx
        for user_ctx in users.values()
        if target in _account_name(user_ctx).strip().lower()
    ]
    if len(partial) == 1:
        return partial[0]
    return None


def build_fleet_overview_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "🧭 多账号总览\n\n暂无已加载账号"
    lines = ["🧭 多账号总览", ""]
    for user_ctx in users.values():
        rt = user_ctx.state.runtime
        risk_modes = risk_control.normalize_fk_switches(rt, apply_default=False)
        total = int(rt.get("total", 0) or 0)
        wins = int(rt.get("win_total", 0) or 0)
        win_rate = (wins / total * 100.0) if total else 0.0
        fk_bits = f"{int(risk_modes['fk1_enabled'])}{int(risk_modes['fk2_enabled'])}{int(risk_modes['fk3_enabled'])}"
        lines.append(
            f"- {_account_name(user_ctx)} ({user_ctx.user_id}) | {_status_text(rt)} | "
            f"预设 {str(rt.get('current_preset_name', '') or '未设')} | "
            f"任务 {_task_brief(rt)} | 策略 {_policy_brief(user_ctx)} | "
            f"胜率 {win_rate:.1f}% | 盈利 {int(rt.get('earnings', 0) or 0):+,} | "
            f"fk {fk_bits}"
        )
    lines.append("")
    lines.append("用法：`fleet task` / `fleet policy` / `fleet show <账号名|ID>` / `fleet gray <账号名|ID> baseline|latest`")
    return "\n".join(lines)


def build_fleet_task_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "📦 多账号任务视图\n\n暂无已加载账号"
    lines = ["📦 多账号任务视图", ""]
    for user_ctx in users.values():
        rt = user_ctx.state.runtime
        lines.append(
            f"- {_account_name(user_ctx)} ({user_ctx.user_id}) | "
            f"任务包 {str(rt.get('package_current_name', '') or '无')} | "
            f"任务 {str(rt.get('task_current_name', '') or '无')} | "
            f"进度 {int(rt.get('task_current_progress_bets', 0) or 0)}/{int(rt.get('task_current_target_bets', 0) or 0)} | "
            f"最后动作 {str(rt.get('task_last_action', '') or '-')}"
        )
    return "\n".join(lines)


def build_fleet_policy_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "🧠 多账号策略灰度视图\n\n暂无已加载账号"
    lines = ["🧠 多账号策略灰度视图", ""]
    for user_ctx in users.values():
        store = policy_engine.load_policy_store(user_ctx)
        active = policy_engine._find_policy_version(store, store.get("active_version", ""))  # type: ignore[attr-defined]
        if not active:
            lines.append(f"- {_account_name(user_ctx)} ({user_ctx.user_id}) | 无策略版本")
            continue
        mode_text = "基线" if str(active.get("activation_mode", "baseline")) == "baseline" else "灰度"
        lines.append(
            f"- {_account_name(user_ctx)} ({user_ctx.user_id}) | "
            f"{active.get('policy_version', 'v1')} | {mode_text} | "
            f"上个版本 {store.get('previous_version', '') or '-'} | "
            f"{str(active.get('summary', '') or '')}"
        )
    return "\n".join(lines)


def build_fleet_account_text(current_user_ctx, ident: str) -> str:
    target = _match_user(current_user_ctx, ident)
    if not target:
        return f"❌ 未找到账号 `{ident}`"
    rt = target.state.runtime
    risk_modes = risk_control.normalize_fk_switches(rt, apply_default=False)
    return (
        "🧾 账号详情\n\n"
        f"账号：{_account_name(target)} ({target.user_id})\n"
        f"状态：{_status_text(rt)}\n"
        f"预设：{str(rt.get('current_preset_name', '') or '未设')}\n"
        f"任务包：{str(rt.get('package_current_name', '') or '无')}\n"
        f"任务：{str(rt.get('task_current_name', '') or '无')}\n"
        f"策略：{_policy_brief(target)}\n"
        f"模型：{str(rt.get('current_model_id', 'unknown') or 'unknown')}\n"
        f"资金：{int(rt.get('gambling_fund', 0) or 0):+,}\n"
        f"余额：{int(rt.get('account_balance', 0) or 0):+,}\n"
        f"盈利：{int(rt.get('earnings', 0) or 0):+,}\n"
        f"风控：fk1 {'ON' if risk_modes['fk1_enabled'] else 'OFF'} / fk2 {'ON' if risk_modes['fk2_enabled'] else 'OFF'} / fk3 {'ON' if risk_modes['fk3_enabled'] else 'OFF'}"
    )


def switch_account_policy_mode(current_user_ctx, ident: str, target_mode: str) -> Dict[str, Any]:
    user_ctx = _match_user(current_user_ctx, ident)
    if not user_ctx:
        return {"ok": False, "message": f"❌ 未找到账号 `{ident}`"}
    mode = str(target_mode or "").strip().lower()
    store = policy_engine.load_policy_store(user_ctx)
    policies = policy_engine._sorted_policies(store)  # type: ignore[attr-defined]
    if not policies:
        return {"ok": False, "message": f"❌ 账号 `{_account_name(user_ctx)}` 暂无策略版本"}

    if mode == "baseline":
        baseline = next((item for item in policies if str(item.get("source", "baseline")) == "baseline"), policies[0])
        result = policy_engine.activate_policy_version(user_ctx, str(baseline.get("policy_version", "") or ""), reason="多账号灰度切回基线")
    elif mode == "latest":
        latest = policies[-1]
        result = policy_engine.activate_policy_version(user_ctx, str(latest.get("policy_version", "") or ""), reason="多账号灰度切到最新版本")
    else:
        return {"ok": False, "message": "❌ 模式只支持 `baseline` 或 `latest`"}

    if result.get("ok", False):
        user_ctx.save_state()
        return {
            "ok": True,
            "message": f"✅ 账号 `{_account_name(user_ctx)}` 已切到 {mode}\n{str(result.get('message', '') or '')}",
        }
    return result
