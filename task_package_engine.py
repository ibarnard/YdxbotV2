from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import history_analysis
import risk_control
import task_engine

PACKAGE_STATUS_IDLE = "idle"
PACKAGE_STATUS_RUNNING = "running"
PACKAGE_STATUS_PAUSED = "paused"

PACKAGE_TEMPLATE_STEADY = "稳健包"
PACKAGE_TEMPLATE_GUARD = "值守包"
PACKAGE_TEMPLATE_ALLDAY = "全天候包"

PACKAGE_TEMPLATE_ALIASES = {
    "稳健": PACKAGE_TEMPLATE_STEADY,
    "值守": PACKAGE_TEMPLATE_GUARD,
    "全天": PACKAGE_TEMPLATE_ALLDAY,
}

PACKAGE_TEMPLATES = {
    PACKAGE_TEMPLATE_STEADY: {
        "description": "趋势优先，失稳时退回保守任务",
        "members": [
            {"task_template": "趋势跟随", "member_name": "趋势跟随", "priority": 20},
            {"task_template": "保守巡航", "member_name": "保守巡航", "priority": 10},
        ],
    },
    PACKAGE_TEMPLATE_GUARD: {
        "description": "保守巡航 + 混合值守，适合长期盯盘",
        "members": [
            {"task_template": "混合值守", "member_name": "混合值守", "priority": 15},
            {"task_template": "保守巡航", "member_name": "保守巡航", "priority": 10},
        ],
    },
    PACKAGE_TEMPLATE_ALLDAY: {
        "description": "趋势、保守、定时三类任务组合",
        "members": [
            {"task_template": "趋势跟随", "member_name": "趋势跟随", "priority": 25},
            {"task_template": "混合值守", "member_name": "混合值守", "priority": 18},
            {"task_template": "保守巡航", "member_name": "保守巡航", "priority": 12},
            {"task_template": "定时巡航", "member_name": "定时巡航", "priority": 8},
        ],
    },
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _package_id() -> str:
    return f"pkg_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _package_run_id() -> str:
    return f"pkgrun_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _status_text(status: str) -> str:
    return {
        PACKAGE_STATUS_IDLE: "待命",
        PACKAGE_STATUS_RUNNING: "运行中",
        PACKAGE_STATUS_PAUSED: "已暂停",
    }.get(status, status or "未知")


def _tier_rank(name: str) -> int:
    try:
        return risk_control.TIER_ORDER.index(str(name or "").strip().lower())
    except ValueError:
        return -1


def _normalize_package_template_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    return PACKAGE_TEMPLATE_ALIASES.get(text, text)


def get_package_template(name: str) -> Optional[Dict[str, Any]]:
    template_name = _normalize_package_template_name(name)
    if not template_name:
        return None
    template = PACKAGE_TEMPLATES.get(template_name)
    if not template:
        return None
    return {
        "template_name": template_name,
        "description": str(template.get("description", "") or ""),
        "members": [dict(item) for item in list(template.get("members", []) or []) if isinstance(item, dict)],
    }


def _package_defaults(package_id: str = "") -> Dict[str, Any]:
    now_text = _now_text()
    return {
        "package_id": package_id or _package_id(),
        "name": "",
        "enabled": False,
        "status": PACKAGE_STATUS_IDLE,
        "switch_mode": "adaptive",
        "members": [],
        "created_at": now_text,
        "updated_at": now_text,
        "current_run_id": "",
        "current_task_id": "",
        "current_task_name": "",
        "progress_switches": 0,
        "total_runs": 0,
        "total_switches": 0,
        "total_profit": 0,
        "last_switch_at": "",
        "last_finish_at": "",
        "last_action": "",
        "last_reason": "",
    }


def _normalize_member(member: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "task_id": str(member.get("task_id", "") or ""),
        "task_name": str(member.get("task_name", "") or ""),
        "priority": max(1, _safe_int(member.get("priority", 100), 100)),
    }


def _normalize_package(package: Dict[str, Any]) -> Dict[str, Any]:
    base = _package_defaults(str(package.get("package_id", "") or ""))
    merged = {**base, **package}
    merged["package_id"] = str(merged.get("package_id", "") or base["package_id"])
    merged["name"] = str(merged.get("name", "") or merged["package_id"])
    status = str(merged.get("status", PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE).lower().strip()
    if status not in {PACKAGE_STATUS_IDLE, PACKAGE_STATUS_RUNNING, PACKAGE_STATUS_PAUSED}:
        status = PACKAGE_STATUS_IDLE
    merged["status"] = status
    merged["enabled"] = bool(merged.get("enabled", False))
    merged["switch_mode"] = str(merged.get("switch_mode", "adaptive") or "adaptive")
    merged["members"] = [_normalize_member(item) for item in list(merged.get("members", []) or []) if isinstance(item, dict)]
    merged["current_run_id"] = str(merged.get("current_run_id", "") or "")
    merged["current_task_id"] = str(merged.get("current_task_id", "") or "")
    merged["current_task_name"] = str(merged.get("current_task_name", "") or "")
    merged["progress_switches"] = max(0, _safe_int(merged.get("progress_switches", 0), 0))
    merged["total_runs"] = max(0, _safe_int(merged.get("total_runs", 0), 0))
    merged["total_switches"] = max(0, _safe_int(merged.get("total_switches", 0), 0))
    merged["total_profit"] = _safe_int(merged.get("total_profit", 0), 0)
    merged["last_switch_at"] = str(merged.get("last_switch_at", "") or "")
    merged["last_finish_at"] = str(merged.get("last_finish_at", "") or "")
    merged["last_action"] = str(merged.get("last_action", "") or "")
    merged["last_reason"] = str(merged.get("last_reason", "") or "")
    merged["created_at"] = str(merged.get("created_at", "") or base["created_at"])
    merged["updated_at"] = str(merged.get("updated_at", "") or base["updated_at"])
    return merged


def _clear_package_runtime(rt: Dict[str, Any]) -> None:
    rt["package_current_id"] = ""
    rt["package_current_name"] = ""
    rt["package_current_status"] = ""
    rt["package_current_task_id"] = ""
    rt["package_current_task_name"] = ""


def _sync_runtime_from_package(user_ctx, package: Optional[Dict[str, Any]]) -> None:
    rt = user_ctx.state.runtime
    if not package:
        _clear_package_runtime(rt)
        return
    rt["package_current_id"] = str(package.get("package_id", "") or "")
    rt["package_current_name"] = str(package.get("name", "") or "")
    rt["package_current_status"] = str(package.get("status", "") or "")
    rt["package_current_task_id"] = str(package.get("current_task_id", "") or "")
    rt["package_current_task_name"] = str(package.get("current_task_name", "") or "")


def _set_package_event(rt: Dict[str, Any], action: str, reason: str) -> None:
    rt["package_last_action"] = str(action or "")
    rt["package_last_reason"] = str(reason or "")
    rt["package_last_event_at"] = _now_text()


def normalize_packages(user_ctx) -> List[Dict[str, Any]]:
    packages = [_normalize_package(item) for item in list(getattr(user_ctx, "task_packages", []))]
    running = [item for item in packages if item.get("status") == PACKAGE_STATUS_RUNNING]
    if len(running) > 1:
        keep_id = str(running[0].get("package_id", "") or "")
        for item in packages:
            if item.get("status") == PACKAGE_STATUS_RUNNING and str(item.get("package_id", "") or "") != keep_id:
                item["status"] = PACKAGE_STATUS_PAUSED
                item["last_action"] = "auto_pause"
                item["last_reason"] = "检测到多个运行中任务包，自动暂停其余任务包"
                item["updated_at"] = _now_text()
        running = [item for item in packages if item.get("status") == PACKAGE_STATUS_RUNNING]
    user_ctx.task_packages = packages
    _sync_runtime_from_package(user_ctx, running[0] if running else None)
    return packages


def save_packages(user_ctx) -> None:
    user_ctx.task_packages = [_normalize_package(item) for item in list(getattr(user_ctx, "task_packages", []))]
    user_ctx.save_task_packages()
    normalize_packages(user_ctx)


def get_packages(user_ctx) -> List[Dict[str, Any]]:
    return normalize_packages(user_ctx)


def find_package(user_ctx, ident: str) -> Optional[Dict[str, Any]]:
    ident_text = str(ident or "").strip()
    if not ident_text:
        return None
    for package in normalize_packages(user_ctx):
        if package.get("package_id") == ident_text:
            return package
    for package in normalize_packages(user_ctx):
        if str(package.get("name", "")).strip() == ident_text:
            return package
    return None


def _find_package_by_id(user_ctx, package_id: str) -> Optional[Dict[str, Any]]:
    ident_text = str(package_id or "").strip()
    if not ident_text:
        return None
    for package in normalize_packages(user_ctx):
        if str(package.get("package_id", "") or "") == ident_text:
            return package
    return None


def current_package(user_ctx) -> Optional[Dict[str, Any]]:
    for package in normalize_packages(user_ctx):
        if package.get("status") == PACKAGE_STATUS_RUNNING:
            return package
    return None


def _record_package_event(user_ctx, package: Dict[str, Any], event_type: str, note: str = "", profit_delta: int = 0) -> None:
    rt = user_ctx.state.runtime
    current_task = task_engine.current_task(user_ctx)
    history_analysis.record_package_event(
        user_ctx,
        package_id=str(package.get("package_id", "") or ""),
        package_name=str(package.get("name", "") or ""),
        run_id=str(package.get("current_run_id", "") or ""),
        task_id=str(package.get("current_task_id", "") or ""),
        task_name=str(package.get("current_task_name", "") or (current_task.get("name", "") if current_task else "")),
        round_key=str(rt.get("current_round_key", "") or ""),
        event_type=event_type,
        status_text=_status_text(str(package.get("status", PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE)),
        progress_switches=_safe_int(package.get("progress_switches", 0), 0),
        active_task_count=1 if current_task else 0,
        profit_delta=_safe_int(profit_delta, 0),
        cum_profit=_safe_int(package.get("total_profit", 0), 0),
        note=str(note or ""),
    )


def _member_task(member: Dict[str, Any], user_ctx) -> Optional[Dict[str, Any]]:
    task_id = str(member.get("task_id", "") or "")
    if not task_id:
        return None
    return task_engine.find_task(user_ctx, task_id)


def _candidate_sort_key(task: Dict[str, Any], member: Dict[str, Any], snapshot: Dict[str, Any]) -> Tuple[int, int]:
    regime_label = str(snapshot.get("regime_label", history_analysis.REGIME_RANGE) or history_analysis.REGIME_RANGE)
    temperature = str(snapshot.get("recent_temperature", {}).get("level", "normal") or "normal")
    task_regimes = task.get("regimes", []) if isinstance(task.get("regimes", []), list) else []
    task_regimes = [str(item or "") for item in task_regimes]
    base_rank = _tier_rank(str(task.get("base_preset", "") or ""))
    priority = _safe_int(member.get("priority", 100), 100)

    match_score = 0
    if task_regimes and regime_label in task_regimes:
        match_score += 50
    elif not task_regimes:
        match_score += 15

    if regime_label == history_analysis.REGIME_CONTINUATION and temperature == "normal":
        tier_score = base_rank
    elif regime_label in {
        history_analysis.REGIME_EXHAUSTION,
        history_analysis.REGIME_REVERSAL,
        history_analysis.REGIME_RANGE,
        history_analysis.REGIME_CHAOS,
    } or temperature in {"cool", "cold"}:
        tier_score = -base_rank
    else:
        tier_score = -abs(base_rank - _tier_rank("yc10"))
    return (match_score + tier_score, -priority)


def _choose_member_task(user_ctx, package: Dict[str, Any], snapshot: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
    candidates: List[Tuple[Tuple[int, int], Dict[str, Any], Dict[str, Any]]] = []
    for member in list(package.get("members", []) or []):
        task = _member_task(member, user_ctx)
        if not task:
            continue
        if str(task.get("status", task_engine.TASK_STATUS_IDLE) or task_engine.TASK_STATUS_IDLE) == task_engine.TASK_STATUS_PAUSED:
            continue
        candidates.append((_candidate_sort_key(task, member, snapshot), task, member))
    if not candidates:
        return None, None, "任务包内没有可用任务"
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, task, member = candidates[0]
    regime_label = str(snapshot.get("regime_label", history_analysis.REGIME_RANGE) or history_analysis.REGIME_RANGE)
    temperature = str(snapshot.get("recent_temperature", {}).get("level", "normal") or "normal")
    reason = f"{regime_label} / 温度 {temperature} 选择任务 {task.get('name', '')}"
    return task, member, reason


def _start_package(user_ctx, package: Dict[str, Any], reason: str) -> Dict[str, Any]:
    active = current_package(user_ctx)
    package = _find_package_by_id(user_ctx, str(package.get("package_id", "") or "")) or package
    if active and active.get("package_id") != package.get("package_id"):
        return {"ok": False, "message": f"已有运行中任务包：{active.get('name', '')}"}
    package["enabled"] = True
    package["status"] = PACKAGE_STATUS_RUNNING
    package["current_run_id"] = _package_run_id()
    package["current_task_id"] = ""
    package["current_task_name"] = ""
    package["progress_switches"] = 0
    package["total_runs"] = _safe_int(package.get("total_runs", 0), 0) + 1
    package["updated_at"] = _now_text()
    package["last_action"] = "started"
    package["last_reason"] = reason
    _sync_runtime_from_package(user_ctx, package)
    _set_package_event(user_ctx.state.runtime, "started", reason)
    _record_package_event(user_ctx, package, "started", note=reason)
    save_packages(user_ctx)
    user_ctx.save_state()
    return {"ok": True, "package": package, "message": f"任务包已启动：{package.get('name', '')}"}


def run_package_now(user_ctx, ident: str) -> Dict[str, Any]:
    package = find_package(user_ctx, ident)
    if not package:
        return {"ok": False, "message": f"任务包不存在：{ident}"}
    return _start_package(user_ctx, package, "手动启动任务包")


def pause_package(user_ctx, ident: str, reason: str = "手动暂停任务包") -> Dict[str, Any]:
    package = find_package(user_ctx, ident)
    if not package:
        return {"ok": False, "message": f"任务包不存在：{ident}"}
    active_task = task_engine.current_task(user_ctx)
    if active_task and str(active_task.get("task_id", "") or "") in {str(item.get("task_id", "") or "") for item in list(package.get("members", []) or [])}:
        task_engine.stop_task_run(user_ctx, str(active_task.get("task_id", "") or ""), reason=f"任务包暂停：{package.get('name', '')}")
    package["status"] = PACKAGE_STATUS_PAUSED
    package["last_action"] = "paused"
    package["last_reason"] = reason
    package["last_finish_at"] = _now_text()
    _sync_runtime_from_package(user_ctx, None)
    _set_package_event(user_ctx.state.runtime, "paused", reason)
    _record_package_event(user_ctx, package, "paused", note=reason)
    save_packages(user_ctx)
    user_ctx.save_state()
    return {"ok": True, "package": package, "message": f"任务包已暂停：{package.get('name', '')}"}


def resume_package(user_ctx, ident: str) -> Dict[str, Any]:
    package = find_package(user_ctx, ident)
    if not package:
        return {"ok": False, "message": f"任务包不存在：{ident}"}
    return _start_package(user_ctx, package, "手动恢复任务包")


def create_package_from_template(
    user_ctx,
    template_name: str,
    package_name: str = "",
    *,
    base_preset: str = "",
    max_bets: int = 0,
    max_loss: Optional[int] = None,
    enabled: bool = False,
) -> Dict[str, Any]:
    template = get_package_template(template_name)
    if not template:
        return {"ok": False, "message": f"任务包模板不存在：{template_name}"}
    package_label = str(package_name or "").strip() or str(template.get("template_name", "") or "")
    members: List[Dict[str, Any]] = []
    for item in list(template.get("members", []) or []):
        member_label = str(item.get("member_name", "") or item.get("task_template", "") or "").strip()
        task_label = f"{package_label}-{member_label}"
        created = task_engine.create_task_from_template(
            user_ctx,
            str(item.get("task_template", "") or ""),
            task_label,
            base_preset=base_preset,
            max_bets=max_bets,
            max_loss=max_loss,
            enabled=False,
        )
        if not created.get("ok", False):
            return {"ok": False, "message": str(created.get("message", "任务模板创建失败"))}
        created_task = created.get("task", {}) if isinstance(created.get("task", {}), dict) else {}
        members.append(
            {
                "task_id": str(created_task.get("task_id", "") or ""),
                "task_name": str(created_task.get("name", "") or task_label),
                "priority": _safe_int(item.get("priority", 100), 100),
            }
        )
    package = _normalize_package(
        {
            "package_id": _package_id(),
            "name": package_label,
            "enabled": bool(enabled),
            "status": PACKAGE_STATUS_IDLE,
            "switch_mode": "adaptive",
            "members": members,
        }
    )
    user_ctx.task_packages.append(package)
    save_packages(user_ctx)
    return {"ok": True, "package": package, "message": f"任务包已创建：{package_label} ({package.get('package_id', '')})"}


def prepare_package_for_round(user_ctx, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    package = current_package(user_ctx)
    if not package:
        return {"active": False, "started": False, "switched": False, "message": "当前无运行中任务包"}
    active_task = task_engine.current_task(user_ctx)
    member_ids = {str(item.get("task_id", "") or "") for item in list(package.get("members", []) or [])}
    if active_task and str(active_task.get("task_id", "") or "") in member_ids:
        package["current_task_id"] = str(active_task.get("task_id", "") or "")
        package["current_task_name"] = str(active_task.get("name", "") or "")
        _sync_runtime_from_package(user_ctx, package)
        save_packages(user_ctx)
        return {"active": True, "started": False, "switched": False, "package": package, "task": active_task, "message": f"任务包继续运行：{package.get('name', '')}"}

    task, member, reason = _choose_member_task(user_ctx, package, snapshot)
    if not task or not member:
        package["last_action"] = "waiting"
        package["last_reason"] = reason
        _set_package_event(user_ctx.state.runtime, "waiting", reason)
        _record_package_event(user_ctx, package, "waiting", note=reason)
        save_packages(user_ctx)
        return {"active": True, "started": False, "switched": False, "package": package, "message": reason}

    start_result = task_engine.start_task_if_possible(user_ctx, str(task.get("task_id", "") or ""), f"任务包触发：{package.get('name', '')} / {reason}", force=False)
    if not start_result.get("ok", False):
        package["last_action"] = "waiting"
        package["last_reason"] = str(start_result.get("message", "") or reason)
        _set_package_event(user_ctx.state.runtime, "waiting", package["last_reason"])
        _record_package_event(user_ctx, package, "waiting", note=package["last_reason"])
        save_packages(user_ctx)
        return {"active": True, "started": False, "switched": False, "package": package, "message": package["last_reason"]}

    package["current_task_id"] = str(task.get("task_id", "") or "")
    package["current_task_name"] = str(task.get("name", "") or "")
    package["progress_switches"] = _safe_int(package.get("progress_switches", 0), 0) + 1
    package["total_switches"] = _safe_int(package.get("total_switches", 0), 0) + 1
    package["last_switch_at"] = _now_text()
    package["last_action"] = "switch_task"
    package["last_reason"] = reason
    _sync_runtime_from_package(user_ctx, package)
    _set_package_event(user_ctx.state.runtime, "switch_task", reason)
    _record_package_event(user_ctx, package, "switch_task", note=reason)
    save_packages(user_ctx)
    user_ctx.save_state()
    return {
        "active": True,
        "started": True,
        "switched": True,
        "package": package,
        "task": task,
        "message": f"任务包已切换任务：{package.get('name', '')} -> {task.get('name', '')}",
    }


def record_settlement(user_ctx, profit: int) -> None:
    package = current_package(user_ctx)
    if not package:
        return
    package["total_profit"] = _safe_int(package.get("total_profit", 0), 0) + _safe_int(profit, 0)
    if not task_engine.current_task(user_ctx):
        package["current_task_id"] = ""
        package["current_task_name"] = ""
    _record_package_event(user_ctx, package, "settled", note=f"任务包结算 {int(profit):+,}", profit_delta=profit)
    save_packages(user_ctx)


def build_package_template_text() -> str:
    lines = ["🧰 任务包模板", ""]
    for index, name in enumerate(PACKAGE_TEMPLATES.keys(), 1):
        template = get_package_template(name) or {}
        members = [str(item.get("task_template", "") or "") for item in list(template.get("members", []) or [])]
        lines.append(f"{index}. {name} | {template.get('description', '')} | 任务：{', '.join(members)}")
    lines.extend(["", "创建：`pkg new <模板>` 或 `pkg new <模板> <名称>`", "覆盖：`pkg new <模板> [名称] preset=yc10 bets=6 loss=18000`"])
    return "\n".join(lines)


def build_package_overview_text(user_ctx) -> str:
    package = current_package(user_ctx)
    if not package:
        return "🧰 任务包总览\n\n当前运行：无\n命令：`pkg tpl` / `pkg new <模板>` / `pkg list` / `pkg show <id>` / `pkg run <id>` / `pkg pause <id>` / `pkg resume <id>`"
    return (
        "🧰 任务包总览\n\n"
        f"当前运行：{package.get('name', '')}\n"
        f"任务包ID：{package.get('package_id', '')}\n"
        f"状态：{_status_text(str(package.get('status', PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE))}\n"
        f"当前任务：{package.get('current_task_name', '') or '无'}\n"
        f"切换次数：{package.get('progress_switches', 0)}\n"
        f"累计收益：{int(package.get('total_profit', 0)):+,}\n"
        f"最近原因：{package.get('last_reason', '') or '无'}"
    )


def build_package_list_text(user_ctx) -> str:
    packages = normalize_packages(user_ctx)
    if not packages:
        return "🧰 暂无任务包\n\n先看模板：`pkg tpl`\n创建：`pkg new 稳健包`"
    lines = ["🧰 任务包列表", ""]
    for index, package in enumerate(packages, 1):
        lines.append(
            f"{index}. {package.get('name', '')} | {package.get('package_id', '')} | {_status_text(str(package.get('status', PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE))} | "
            f"{'已启用' if bool(package.get('enabled', False)) else '已关闭'} | 成员 {len(package.get('members', []))}"
        )
    return "\n".join(lines)


def build_package_detail_text(user_ctx, ident: str) -> str:
    package = find_package(user_ctx, ident)
    if not package:
        return f"❌ 任务包不存在：{ident}"
    member_lines = []
    for member in list(package.get("members", []) or []):
        member_lines.append(
            f"- {member.get('task_name', '')} | task_id={member.get('task_id', '')} | 优先级 {member.get('priority', 100)}"
        )
    if not member_lines:
        member_lines = ["- 无成员任务"]
    return (
        "🧰 任务包详情\n\n"
        f"名称：{package.get('name', '')}\n"
        f"任务包ID：{package.get('package_id', '')}\n"
        f"状态：{_status_text(str(package.get('status', PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE))}\n"
        f"启用：{'是' if bool(package.get('enabled', False)) else '否'}\n"
        f"当前任务：{package.get('current_task_name', '') or '无'}\n"
        f"本轮切换：{package.get('progress_switches', 0)}\n"
        f"累计运行：{package.get('total_runs', 0)} 轮\n"
        f"累计切换：{package.get('total_switches', 0)} 次\n"
        f"累计收益：{int(package.get('total_profit', 0)):+,}\n"
        f"最近动作：{package.get('last_action', '') or '无'}\n"
        f"最近原因：{package.get('last_reason', '') or '无'}\n"
        f"成员任务：\n" + "\n".join(member_lines)
    )


def _package_log_rows(user_ctx, package_id: str = "", limit: int = 10) -> List[Dict[str, Any]]:
    db_path = os.path.join(user_ctx.user_dir, "analytics.db")
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        history_analysis._ensure_analytics_schema(conn)
        if package_id:
            rows = conn.execute(
                "SELECT * FROM package_runs WHERE package_id = ? ORDER BY created_at DESC LIMIT ?",
                (package_id, max(1, int(limit))),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM package_runs ORDER BY created_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def build_package_logs_text(user_ctx, ident: str = "") -> str:
    package = find_package(user_ctx, ident) if ident else None
    rows = _package_log_rows(user_ctx, package.get("package_id", "") if package else ident, limit=10)
    if not rows:
        return "🧰 暂无任务包运行记录"
    lines = ["🧰 任务包运行记录", ""]
    for row in rows:
        lines.append(
            f"- {row.get('created_at', '')} | {row.get('package_name', '')} | {row.get('event_type', '')} | "
            f"当前任务 {row.get('task_name', '') or '-'} | 收益 {int(row.get('profit_delta', 0) or 0):+,} | {row.get('note', '') or '-'}"
        )
    return "\n".join(lines)


def build_package_stats_text(user_ctx, ident: str = "") -> str:
    package = find_package(user_ctx, ident) if ident else None
    packages = [package] if package else normalize_packages(user_ctx)
    packages = [item for item in packages if item]
    if not packages:
        return "🧰 暂无任务包统计"
    total_runs = sum(_safe_int(item.get("total_runs", 0), 0) for item in packages)
    total_switches = sum(_safe_int(item.get("total_switches", 0), 0) for item in packages)
    total_profit = sum(_safe_int(item.get("total_profit", 0), 0) for item in packages)
    active_count = sum(1 for item in packages if item.get("status") == PACKAGE_STATUS_RUNNING)
    return (
        "🧰 任务包统计\n\n"
        f"任务包数：{len(packages)}\n"
        f"运行中：{active_count}\n"
        f"累计运行轮次：{total_runs}\n"
        f"累计切换次数：{total_switches}\n"
        f"累计收益：{total_profit:+,}"
    )


def build_package_focus_text(user_ctx) -> str:
    package = current_package(user_ctx)
    if not package:
        enabled_count = sum(1 for item in normalize_packages(user_ctx) if bool(item.get("enabled", False)))
        return f"🧰 任务包提醒：当前无运行中任务包（已创建 {enabled_count} 个，可用 `pkg` 查看）"
    return (
        f"🧰 任务包提醒：{package.get('name', '')} | {_status_text(str(package.get('status', PACKAGE_STATUS_IDLE) or PACKAGE_STATUS_IDLE))} | "
        f"当前任务 {package.get('current_task_name', '') or '无'} | 切换 {package.get('progress_switches', 0)} 次"
    )
