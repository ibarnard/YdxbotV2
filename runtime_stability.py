from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

import policy_engine
import self_learning_engine
from user_manager import get_registered_user_contexts

WATCH_EVENT_STATE_KEY = "watch_event_state"
WATCH_ALERTS_KEY = "watch_alerts"
WATCH_ALERT_LIMIT = 30
RUNTIME_FAULTS_KEY = "runtime_faults"
LAST_RUNTIME_FAULT_KEY = "last_runtime_fault"
RUNTIME_FAULT_LIMIT = 20


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_ts() -> int:
    return int(datetime.now().timestamp())


def _all_users(current_user_ctx) -> Dict[int, Any]:
    users = get_registered_user_contexts()
    if not users and current_user_ctx is not None:
        users[int(current_user_ctx.user_id)] = current_user_ctx
    return dict(sorted(users.items(), key=lambda item: item[0]))


def _iter_targets(target: Any) -> List[Any]:
    if isinstance(target, (list, tuple, set)):
        return [item for item in target if item not in (None, "")]
    if target in (None, ""):
        return []
    return [target]


def _issue(message: str) -> Dict[str, str]:
    return {"message": str(message or "").strip()}


def _first_line(text: Any, limit: int = 120) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "-"
    line = raw.splitlines()[0].strip()
    if len(line) <= limit:
        return line
    return f"{line[: limit - 3].rstrip()}..."


def _normalize_watch_bot_cfg(notification: Dict[str, Any]) -> Dict[str, Any]:
    watch_cfg = notification.get("watch", {}) if isinstance(notification.get("watch", {}), dict) else {}
    watch_tg_bot = watch_cfg.get("tg_bot", {}) if isinstance(watch_cfg.get("tg_bot", {}), dict) else {}
    if not watch_tg_bot:
        legacy_watch_tg_bot = notification.get("watch_tg_bot", {})
        if isinstance(legacy_watch_tg_bot, dict):
            watch_tg_bot = legacy_watch_tg_bot
    if not watch_tg_bot:
        base_tg_bot = notification.get("tg_bot", {})
        if isinstance(base_tg_bot, dict):
            watch_tg_bot = dict(base_tg_bot)
    return watch_tg_bot if isinstance(watch_tg_bot, dict) else {}


def _task_exists(user_ctx, task_id: str, task_name: str) -> bool:
    for item in user_ctx.tasks:
        if not isinstance(item, dict):
            continue
        if task_id and str(item.get("id", "") or "") == task_id:
            return True
        if task_name and str(item.get("name", "") or "") == task_name:
            return True
    return False


def _package_exists(user_ctx, package_id: str, package_name: str) -> bool:
    for item in user_ctx.task_packages:
        if not isinstance(item, dict):
            continue
        if package_id and str(item.get("id", "") or "") == package_id:
            return True
        if package_name and str(item.get("name", "") or "") == package_name:
            return True
    return False


def list_runtime_faults(user_ctx, limit: int = 5) -> List[Dict[str, Any]]:
    faults = user_ctx.state.runtime.get(RUNTIME_FAULTS_KEY, [])
    if not isinstance(faults, list):
        return []
    rows = [item for item in faults if isinstance(item, dict)]
    return rows[-max(1, int(limit or 1)) :]


def get_recent_runtime_fault(user_ctx, max_age_sec: int = 24 * 3600) -> Dict[str, Any]:
    fault = user_ctx.state.runtime.get(LAST_RUNTIME_FAULT_KEY, {})
    if not isinstance(fault, dict) or not fault:
        return {}
    occurred_ts = int(fault.get("occurred_ts", 0) or 0)
    if occurred_ts <= 0:
        return {}
    if (_now_ts() - occurred_ts) > max(0, int(max_age_sec or 0)):
        return {}
    return dict(fault)


def format_runtime_fault_brief(fault: Dict[str, Any]) -> str:
    if not isinstance(fault, dict) or not fault:
        return "-"
    occurred_at = str(fault.get("occurred_at", "") or "-")
    stage = str(fault.get("stage", "") or "runtime")
    error_type = str(fault.get("error_type", "") or "RuntimeError")
    message = _first_line(fault.get("message", ""), 90)
    count = int(fault.get("count", 1) or 1)
    suffix = f" x{count}" if count > 1 else ""
    return f"{occurred_at} | {stage} | {error_type}{suffix} | {message}"


def record_runtime_fault(
    user_ctx,
    stage: str,
    error: Exception | None = None,
    *,
    message: str = "",
    severity: str = "error",
    action: str = "",
    persist: bool = False,
) -> Dict[str, Any]:
    rt = user_ctx.state.runtime
    error_type = type(error).__name__ if error is not None else "RuntimeError"
    summary = _first_line(message or str(error or ""), 180)
    fingerprint = f"{stage}|{error_type}|{summary}"
    now_text = _now_text()
    now_ts = _now_ts()

    faults = rt.get(RUNTIME_FAULTS_KEY, [])
    if not isinstance(faults, list):
        faults = []
    normalized = [item for item in faults if isinstance(item, dict)]

    entry = {
        "occurred_at": now_text,
        "occurred_ts": now_ts,
        "stage": str(stage or "runtime"),
        "severity": str(severity or "error"),
        "error_type": error_type,
        "message": summary or str(stage or "runtime"),
        "action": str(action or "").strip(),
        "fingerprint": fingerprint,
        "count": 1,
    }

    if normalized and str(normalized[-1].get("fingerprint", "") or "") == fingerprint:
        normalized[-1]["occurred_at"] = now_text
        normalized[-1]["occurred_ts"] = now_ts
        normalized[-1]["severity"] = entry["severity"]
        normalized[-1]["action"] = entry["action"]
        normalized[-1]["count"] = int(normalized[-1].get("count", 0) or 0) + 1
        entry = dict(normalized[-1])
    else:
        normalized.append(entry)

    rt[RUNTIME_FAULTS_KEY] = normalized[-RUNTIME_FAULT_LIMIT:]
    rt[LAST_RUNTIME_FAULT_KEY] = dict(entry)
    if persist:
        try:
            user_ctx.save_state()
        except Exception:
            pass
    return dict(entry)


def reconcile_runtime_state(user_ctx) -> Dict[str, Any]:
    rt = user_ctx.state.runtime
    changes: List[str] = []

    watch_state = rt.get(WATCH_EVENT_STATE_KEY, {})
    if not isinstance(watch_state, dict):
        rt[WATCH_EVENT_STATE_KEY] = {}
        changes.append("重置值守事件状态缓存")
    else:
        normalized_watch_state = {
            str(key): dict(value)
            for key, value in watch_state.items()
            if isinstance(key, str) and isinstance(value, dict)
        }
        if normalized_watch_state != watch_state:
            rt[WATCH_EVENT_STATE_KEY] = normalized_watch_state
            changes.append("清理非法值守事件状态")

    watch_alerts = rt.get(WATCH_ALERTS_KEY, [])
    if not isinstance(watch_alerts, list):
        rt[WATCH_ALERTS_KEY] = []
        changes.append("重置值守告警缓存")
    else:
        normalized_alerts = [dict(item) for item in watch_alerts if isinstance(item, dict)][-WATCH_ALERT_LIMIT:]
        if normalized_alerts != watch_alerts:
            rt[WATCH_ALERTS_KEY] = normalized_alerts
            changes.append("清理非法值守告警缓存")

    if int(rt.get("pause_count", 0) or 0) <= 0:
        pause_defaults = {
            "pause_resume_pending": False,
            "pause_resume_pending_reason": "",
            "pause_resume_probe_settled": -1,
            "pause_countdown_active": False,
            "pause_countdown_reason": "",
            "pause_countdown_total_rounds": 0,
            "pause_countdown_last_remaining": -1,
        }
        if any(rt.get(key) != value for key, value in pause_defaults.items()):
            for key, value in pause_defaults.items():
                rt[key] = value
            changes.append("清理过期暂停倒计时标记")

    if not bool(rt.get("shadow_probe_active", False)):
        shadow_defaults = {
            "shadow_probe_origin_reason": "",
            "shadow_probe_target_rounds": 0,
            "shadow_probe_pass_required": 0,
            "shadow_probe_checked": 0,
            "shadow_probe_hits": 0,
            "shadow_probe_pending_prediction": None,
            "shadow_probe_last_history_len": -1,
            "shadow_probe_rearm": False,
        }
        if any(rt.get(key) != value for key, value in shadow_defaults.items()):
            for key, value in shadow_defaults.items():
                rt[key] = value
            changes.append("清理过期影子验证状态")

    pending_bet_id = str(rt.get("pending_bet_id", "") or "")
    bet_logs = user_ctx.state.bet_sequence_log if isinstance(user_ctx.state.bet_sequence_log, list) else []
    if pending_bet_id:
        has_open_pending = any(
            isinstance(item, dict)
            and str(item.get("bet_id", "") or "") == pending_bet_id
            and item.get("result") is None
            for item in bet_logs
        )
        if not has_open_pending:
            rt["pending_bet_id"] = ""
            rt["current_round_key"] = ""
            changes.append(f"清理失效挂单标记 {pending_bet_id}")
    elif str(rt.get("current_round_key", "") or ""):
        has_any_open_pending = any(
            isinstance(item, dict) and item.get("result") is None
            for item in bet_logs
        )
        if not has_any_open_pending:
            rt["current_round_key"] = ""
            changes.append("清理孤立 round_key")

    faults = rt.get(RUNTIME_FAULTS_KEY, [])
    if not isinstance(faults, list):
        rt[RUNTIME_FAULTS_KEY] = []
        rt[LAST_RUNTIME_FAULT_KEY] = {}
        changes.append("重置运行异常快照缓存")
    else:
        normalized_faults = [dict(item) for item in faults if isinstance(item, dict)][-RUNTIME_FAULT_LIMIT:]
        if normalized_faults != faults:
            rt[RUNTIME_FAULTS_KEY] = normalized_faults
            changes.append("清理非法运行异常快照")
        if normalized_faults:
            rt[LAST_RUNTIME_FAULT_KEY] = dict(normalized_faults[-1])
        elif rt.get(LAST_RUNTIME_FAULT_KEY, {}):
            rt[LAST_RUNTIME_FAULT_KEY] = {}
            changes.append("清理孤立最近异常快照")

    return {
        "changed": bool(changes),
        "count": len(changes),
        "changes": changes,
    }


def inspect_user_context(user_ctx) -> Dict[str, Any]:
    blockers: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []
    infos: List[Dict[str, str]] = []

    telegram = user_ctx.config.telegram if isinstance(user_ctx.config.telegram, dict) else {}
    groups = user_ctx.config.groups if isinstance(user_ctx.config.groups, dict) else {}
    notification = user_ctx.config.notification if isinstance(user_ctx.config.notification, dict) else {}
    zhuque = user_ctx.config.zhuque if isinstance(user_ctx.config.zhuque, dict) else {}
    rt = user_ctx.state.runtime

    if not telegram.get("api_id") or not telegram.get("api_hash"):
        blockers.append(_issue("缺少 Telegram `api_id/api_hash`，无法建立客户端连接"))

    if not _iter_targets(groups.get("zq_group", [])):
        blockers.append(_issue("缺少 `groups.zq_group`，无法监听开奖/下注群消息"))

    if not _iter_targets(groups.get("zq_bot")):
        blockers.append(_issue("缺少 `groups.zq_bot`，无法识别下注机器人"))

    admin_chat = notification.get("admin_chat")
    if admin_chat in (None, ""):
        admin_chat = groups.get("admin_chat")
    if admin_chat in (None, ""):
        warnings.append(_issue("未配置 `admin_chat`，命令回显和管理员通知会不可用"))

    watch_cfg = notification.get("watch", {}) if isinstance(notification.get("watch", {}), dict) else {}
    watch_chat = watch_cfg.get("admin_chat")
    watch_bot = _normalize_watch_bot_cfg(notification)
    has_watch_chat = bool(_iter_targets(watch_chat))
    has_watch_bot = bool(
        isinstance(watch_bot, dict)
        and watch_bot.get("enable")
        and watch_bot.get("bot_token")
        and watch_bot.get("chat_id")
    )
    if has_watch_chat or has_watch_bot:
        infos.append(_issue("已配置值守播报目标"))
    elif admin_chat or (
        isinstance(notification.get("tg_bot", {}), dict)
        and notification.get("tg_bot", {}).get("enable")
        and notification.get("tg_bot", {}).get("bot_token")
        and notification.get("tg_bot", {}).get("chat_id")
    ):
        warnings.append(_issue("未配置独立 `notification.watch`，值守播报会回退到现有管理员通道"))
    else:
        warnings.append(_issue("未配置值守播报目标，主动值守播报将不可用"))

    if not zhuque.get("cookie") or not (zhuque.get("csrf_token") or zhuque.get("x_csrf")):
        warnings.append(_issue("未配置朱雀 `cookie/csrf`，余额刷新会不可用"))

    task_current_id = str(rt.get("task_current_id", "") or "")
    task_current_name = str(rt.get("task_current_name", "") or "")
    if (task_current_id or task_current_name) and not _task_exists(user_ctx, task_current_id, task_current_name):
        warnings.append(_issue(f"运行态任务引用失效：{task_current_id or task_current_name} 不在 tasks.json 中"))

    package_current_id = str(rt.get("package_current_id", "") or "")
    package_current_name = str(rt.get("package_current_name", "") or "")
    if (package_current_id or package_current_name) and not _package_exists(user_ctx, package_current_id, package_current_name):
        warnings.append(_issue(f"运行态任务包引用失效：{package_current_id or package_current_name} 不在 task_packages.json 中"))

    policy_store = policy_engine.load_policy_store(user_ctx)
    active_version = str(rt.get("policy_active_version", "") or policy_store.get("active_version", "") or "")
    if active_version and not policy_engine._find_policy_version(policy_store, active_version):  # type: ignore[attr-defined]
        warnings.append(_issue(f"运行态策略版本失效：{active_version} 不在 policy_versions.json 中"))

    center = self_learning_engine.load_learning_center(user_ctx)
    candidate_refs = [
        ("active_shadow_candidate_id", "学习影子候选"),
        ("active_gray_candidate_id", "学习灰度候选"),
        ("promoted_candidate_id", "学习转正候选"),
    ]
    for key, label in candidate_refs:
        candidate_id = str(center.get(key, "") or "")
        if candidate_id and not self_learning_engine._find_candidate_strict(center, candidate_id):  # type: ignore[attr-defined]
            warnings.append(_issue(f"{label}失效：{candidate_id} 不在 learning_center.json 中"))

    recent_fault = get_recent_runtime_fault(user_ctx, max_age_sec=7 * 24 * 3600)
    status = "blocked" if blockers else ("warning" if warnings else "ok")
    return {
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "infos": infos,
        "last_fault": recent_fault,
    }


def _status_rank(result: Dict[str, Any]) -> int:
    status = str(result.get("status", "ok") or "ok")
    if status == "blocked":
        return 0
    if status == "warning":
        return 1
    return 2


def build_doctor_text(user_ctx) -> str:
    result = inspect_user_context(user_ctx)
    blockers = result.get("blockers", [])
    warnings = result.get("warnings", [])
    infos = result.get("infos", [])
    last_fault = result.get("last_fault", {})
    status_text = {
        "blocked": "阻断，当前不建议启动",
        "warning": "可运行，但存在警告",
        "ok": "正常，可运行",
    }.get(str(result.get("status", "ok") or "ok"), "正常，可运行")

    lines = [
        "🩺 账号自检",
        "",
        f"账号：{user_ctx.config.name} ({user_ctx.user_id})",
        f"状态：{status_text}",
        f"阻断：{len(blockers)} | 警告：{len(warnings)}",
    ]

    if blockers:
        lines.append("")
        lines.append("阻断项：")
        lines.extend(f"- {item.get('message', '')}" for item in blockers[:8])

    if warnings:
        lines.append("")
        lines.append("警告项：")
        lines.extend(f"- {item.get('message', '')}" for item in warnings[:8])

    if infos:
        lines.append("")
        lines.append("说明：")
        lines.extend(f"- {item.get('message', '')}" for item in infos[:5])

    if last_fault:
        lines.append("")
        lines.append("最近异常：")
        lines.append(f"- {format_runtime_fault_brief(last_fault)}")

    lines.append("")
    lines.append("更多：`doctor fleet` / `watch alerts`")
    return "\n".join(lines)


def build_doctor_fleet_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "🩺 多账号自检\n\n暂无已加载账号"

    rows: List[Tuple[int, int, str]] = []
    blocked_count = 0
    warning_count = 0
    for user_ctx in users.values():
        result = inspect_user_context(user_ctx)
        blockers = result.get("blockers", [])
        warnings = result.get("warnings", [])
        last_fault = result.get("last_fault", {})
        if blockers:
            blocked_count += 1
        elif warnings:
            warning_count += 1
        row = (
            _status_rank(result),
            int(getattr(user_ctx, "user_id", 0) or 0),
            (
                f"- {user_ctx.config.name} ({user_ctx.user_id}) | "
                f"阻断 {len(blockers)} | 警告 {len(warnings)}"
                + (
                    f" | 最近异常 {str(last_fault.get('stage', '') or '-')} / {_first_line(last_fault.get('message', ''), 30)}"
                    if last_fault
                    else ""
                )
            ),
        )
        rows.append(row)

    rows.sort(key=lambda item: (item[0], item[1]))
    lines = [
        "🩺 多账号自检",
        "",
        f"总数：{len(rows)} | 阻断账号 {blocked_count} | 警告账号 {warning_count}",
        "",
    ]
    lines.extend(row[2] for row in rows)
    return "\n".join(lines)


def build_startup_health_text(user_ctx, doctor_result: Dict[str, Any], reconcile_result: Dict[str, Any]) -> str:
    blockers = doctor_result.get("blockers", [])
    warnings = doctor_result.get("warnings", [])
    changes = reconcile_result.get("changes", [])
    lines = [
        "🩺 启动自检摘要",
        "",
        f"账号：{user_ctx.config.name} ({user_ctx.user_id})",
        f"阻断：{len(blockers)} | 警告：{len(warnings)} | 已修复：{len(changes)}",
    ]

    if warnings:
        lines.append("")
        lines.append("警告：")
        lines.extend(f"- {item.get('message', '')}" for item in warnings[:6])

    if changes:
        lines.append("")
        lines.append("启动时已修复：")
        lines.extend(f"- {item}" for item in changes[:6])

    lines.append("")
    lines.append("命令：`doctor` / `watch alerts`")
    return "\n".join(lines)
