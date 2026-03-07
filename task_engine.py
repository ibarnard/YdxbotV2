from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import dynamic_betting
import history_analysis

TASK_STATUS_IDLE = "idle"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_PAUSED = "paused"

TASK_MODE_MANUAL = "manual"
TASK_MODE_SCHEDULE = "schedule"
TASK_MODE_REGIME = "regime"
TASK_MODE_HYBRID = "hybrid"

TASK_TRIGGER_MODES = {
    TASK_MODE_MANUAL,
    TASK_MODE_SCHEDULE,
    TASK_MODE_REGIME,
    TASK_MODE_HYBRID,
}
REGIME_LABELS = {
    history_analysis.REGIME_CONTINUATION,
    history_analysis.REGIME_EXHAUSTION,
    history_analysis.REGIME_REVERSAL,
    history_analysis.REGIME_RANGE,
    history_analysis.REGIME_CHAOS,
}

TASK_TEMPLATES = {
    "保守巡航": {
        "description": "只在延续盘接管，默认低档保守跑",
        "trigger_mode": TASK_MODE_REGIME,
        "interval_minutes": 0,
        "regimes": [history_analysis.REGIME_CONTINUATION],
        "base_preset": "yc5",
        "max_bets": 8,
        "max_loss": 10000,
    },
    "趋势跟随": {
        "description": "延续盘优先，衰竭盘保守跟随",
        "trigger_mode": TASK_MODE_REGIME,
        "interval_minutes": 0,
        "regimes": [
            history_analysis.REGIME_CONTINUATION,
            history_analysis.REGIME_EXHAUSTION,
        ],
        "base_preset": "yc20",
        "max_bets": 12,
        "max_loss": 30000,
    },
    "定时巡航": {
        "description": "按固定时间间隔巡航，适合低档灰度",
        "trigger_mode": TASK_MODE_SCHEDULE,
        "interval_minutes": 30,
        "regimes": [],
        "base_preset": "yc5",
        "max_bets": 10,
        "max_loss": 12000,
    },
    "混合值守": {
        "description": "定时 + 延续盘双条件，适合稳健值守",
        "trigger_mode": TASK_MODE_HYBRID,
        "interval_minutes": 30,
        "regimes": [history_analysis.REGIME_CONTINUATION],
        "base_preset": "yc10",
        "max_bets": 10,
        "max_loss": 18000,
    },
}

TASK_TEMPLATE_ALIASES = {
    "保守": "保守巡航",
    "趋势": "趋势跟随",
    "定时": "定时巡航",
    "混合": "混合值守",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _task_id() -> str:
    return f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _task_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _analytics_db_path(user_ctx) -> str:
    return os.path.join(user_ctx.user_dir, "analytics.db")


def _status_text(status: str) -> str:
    return {
        TASK_STATUS_IDLE: "待命",
        TASK_STATUS_RUNNING: "运行中",
        TASK_STATUS_PAUSED: "已暂停",
    }.get(status, status or "未知")


def _trigger_text(trigger_mode: str, interval_minutes: int, regimes: List[str]) -> str:
    if trigger_mode == TASK_MODE_MANUAL:
        return "手动"
    if trigger_mode == TASK_MODE_SCHEDULE:
        return f"定时({interval_minutes}分钟)"
    if trigger_mode == TASK_MODE_REGIME:
        return f"盘面({','.join(regimes) or '任意'})"
    if trigger_mode == TASK_MODE_HYBRID:
        return f"混合({interval_minutes}分钟 + {','.join(regimes) or '任意'})"
    return trigger_mode or "未知"


def _normalize_regimes(values: Any) -> List[str]:
    if isinstance(values, str):
        items = [item.strip() for item in values.replace("，", ",").split(",")]
    elif isinstance(values, list):
        items = [str(item).strip() for item in values]
    else:
        items = []
    result: List[str] = []
    for item in items:
        if item and item not in result:
            result.append(item)
    return result


def _task_template_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    return TASK_TEMPLATE_ALIASES.get(text, text)


def get_task_template(name: str) -> Optional[Dict[str, Any]]:
    template_name = _task_template_name(name)
    if not template_name:
        return None
    template = TASK_TEMPLATES.get(template_name)
    if not template:
        return None
    return {
        "template_name": template_name,
        "description": str(template.get("description", "") or ""),
        "trigger_mode": str(template.get("trigger_mode", TASK_MODE_MANUAL) or TASK_MODE_MANUAL),
        "interval_minutes": _safe_int(template.get("interval_minutes", 0), 0),
        "regimes": _normalize_regimes(template.get("regimes", [])),
        "base_preset": str(template.get("base_preset", "") or ""),
        "max_bets": max(1, _safe_int(template.get("max_bets", 1), 1)),
        "max_loss": max(0, _safe_int(template.get("max_loss", 0), 0)),
    }


def build_task_template_text() -> str:
    lines = ["📦 任务模板", ""]
    for index, name in enumerate(TASK_TEMPLATES.keys(), 1):
        template = get_task_template(name) or {}
        lines.append(
            f"{index}. {name} | {template.get('description', '')} | "
            f"基准 {template.get('base_preset', '')} | "
            f"触发 {_trigger_text(str(template.get('trigger_mode', '') or ''), _safe_int(template.get('interval_minutes', 0), 0), _normalize_regimes(template.get('regimes', [])))} | "
            f"目标 {template.get('max_bets', 0)} 笔 | 止损 {_safe_int(template.get('max_loss', 0), 0):,}"
        )
    lines.extend(
        [
            "",
            "创建：`task new <模板>` 或 `task new <模板> <名称>`",
            "覆盖：`task new <模板> [名称] preset=yc10 bets=12 loss=30000`",
            "示例：`task new 保守巡航` / `task new 趋势 巡航A`",
        ]
    )
    return "\n".join(lines)


def parse_template_new_args(args: List[str]) -> Dict[str, Any]:
    if not args:
        return {"ok": False, "message": "用法：task new <模板> [名称] [preset=yc10] [bets=12] [loss=20000]"}
    template_name = str(args[0] or "").strip()
    if not template_name:
        return {"ok": False, "message": "任务模板不能为空"}

    task_name = ""
    tokens = list(args[1:])
    if tokens and "=" not in str(tokens[0]):
        task_name = str(tokens[0] or "").strip()
        tokens = tokens[1:]

    overrides: Dict[str, Any] = {}
    for token in tokens:
        text = str(token or "").strip()
        if not text:
            continue
        if "=" not in text:
            return {"ok": False, "message": f"模板参数格式错误：{text}，应为 key=value"}
        key, value = text.split("=", 1)
        key = str(key or "").strip().lower()
        value = str(value or "").strip()
        if key in {"preset", "base_preset"}:
            overrides["base_preset"] = value
        elif key in {"bets", "max_bets"}:
            parsed = _safe_int(value, 0)
            if parsed <= 0:
                return {"ok": False, "message": "bets 必须是大于 0 的整数"}
            overrides["max_bets"] = parsed
        elif key in {"loss", "max_loss"}:
            parsed = _safe_int(value, -1)
            if parsed < 0:
                return {"ok": False, "message": "loss 不能为负数"}
            overrides["max_loss"] = parsed
        elif key == "name":
            task_name = value
        else:
            return {"ok": False, "message": f"不支持的模板参数：{key}"}

    return {"ok": True, "template_name": template_name, "task_name": task_name, "overrides": overrides}


def _task_defaults(task_id: str = "") -> Dict[str, Any]:
    now_text = _now_text()
    return {
        "task_id": task_id or _task_id(),
        "name": "",
        "enabled": False,
        "status": TASK_STATUS_IDLE,
        "trigger_mode": TASK_MODE_MANUAL,
        "interval_minutes": 0,
        "regimes": [],
        "base_preset": "",
        "max_bets": 10,
        "max_loss": 0,
        "priority": 100,
        "created_at": now_text,
        "updated_at": now_text,
        "last_trigger_at": "",
        "last_finish_at": "",
        "current_run_id": "",
        "progress_bets": 0,
        "progress_profit": 0,
        "progress_loss": 0,
        "last_run_bets": 0,
        "last_run_profit": 0,
        "last_run_loss": 0,
        "total_runs": 0,
        "total_bets": 0,
        "total_profit": 0,
        "last_action": "",
        "last_reason": "",
    }


def _normalize_task(task: Dict[str, Any]) -> Dict[str, Any]:
    base = _task_defaults(str(task.get("task_id", "") or ""))
    merged = {**base, **task}
    merged["task_id"] = str(merged.get("task_id", "") or base["task_id"])
    merged["name"] = str(merged.get("name", "") or merged["task_id"])
    trigger_mode = str(merged.get("trigger_mode", TASK_MODE_MANUAL) or TASK_MODE_MANUAL).lower().strip()
    merged["trigger_mode"] = trigger_mode if trigger_mode in TASK_TRIGGER_MODES else TASK_MODE_MANUAL
    status = str(merged.get("status", TASK_STATUS_IDLE) or TASK_STATUS_IDLE).lower().strip()
    if status not in {TASK_STATUS_IDLE, TASK_STATUS_RUNNING, TASK_STATUS_PAUSED}:
        status = TASK_STATUS_IDLE
    merged["status"] = status
    merged["enabled"] = bool(merged.get("enabled", False))
    merged["interval_minutes"] = max(0, _safe_int(merged.get("interval_minutes", 0), 0))
    merged["regimes"] = _normalize_regimes(merged.get("regimes", []))
    merged["base_preset"] = str(merged.get("base_preset", "") or "").strip()
    merged["max_bets"] = max(1, _safe_int(merged.get("max_bets", 10), 10))
    merged["max_loss"] = max(0, _safe_int(merged.get("max_loss", 0), 0))
    merged["priority"] = max(1, _safe_int(merged.get("priority", 100), 100))
    for key in (
        "progress_bets",
        "progress_profit",
        "progress_loss",
        "last_run_bets",
        "last_run_profit",
        "last_run_loss",
        "total_runs",
        "total_bets",
        "total_profit",
    ):
        merged[key] = _safe_int(merged.get(key, 0), 0)
    merged["current_run_id"] = str(merged.get("current_run_id", "") or "")
    merged["last_trigger_at"] = str(merged.get("last_trigger_at", "") or "")
    merged["last_finish_at"] = str(merged.get("last_finish_at", "") or "")
    merged["last_action"] = str(merged.get("last_action", "") or "")
    merged["last_reason"] = str(merged.get("last_reason", "") or "")
    merged["created_at"] = str(merged.get("created_at", "") or base["created_at"])
    merged["updated_at"] = str(merged.get("updated_at", "") or base["updated_at"])
    if merged["trigger_mode"] in {TASK_MODE_SCHEDULE, TASK_MODE_HYBRID} and merged["interval_minutes"] <= 0:
        merged["interval_minutes"] = 30
    if merged["trigger_mode"] in {TASK_MODE_REGIME, TASK_MODE_HYBRID} and not merged["regimes"]:
        merged["regimes"] = [history_analysis.REGIME_CONTINUATION]
    return merged


def _clear_task_runtime(rt: Dict[str, Any]) -> None:
    rt["task_current_id"] = ""
    rt["task_current_name"] = ""
    rt["task_current_run_id"] = ""
    rt["task_current_trigger_mode"] = ""
    rt["task_current_base_preset"] = ""
    rt["task_current_progress_bets"] = 0
    rt["task_current_target_bets"] = 0


def _sync_runtime_from_task(user_ctx, task: Optional[Dict[str, Any]]) -> None:
    rt = user_ctx.state.runtime
    if not task:
        _clear_task_runtime(rt)
        return
    rt["task_current_id"] = str(task.get("task_id", "") or "")
    rt["task_current_name"] = str(task.get("name", "") or "")
    rt["task_current_run_id"] = str(task.get("current_run_id", "") or "")
    rt["task_current_trigger_mode"] = str(task.get("trigger_mode", "") or "")
    rt["task_current_base_preset"] = str(task.get("base_preset", "") or "")
    rt["task_current_progress_bets"] = _safe_int(task.get("progress_bets", 0), 0)
    rt["task_current_target_bets"] = _safe_int(task.get("max_bets", 0), 0)


def _set_task_event(rt: Dict[str, Any], action: str, reason: str) -> None:
    rt["task_last_action"] = str(action or "")
    rt["task_last_reason"] = str(reason or "")
    rt["task_last_event_at"] = _now_text()


def normalize_tasks(user_ctx) -> List[Dict[str, Any]]:
    tasks = [_normalize_task(item) for item in list(getattr(user_ctx, "tasks", []))]
    running = [task for task in tasks if task.get("status") == TASK_STATUS_RUNNING]
    if len(running) > 1:
        current_id = str(running[0].get("task_id", "") or "")
        for task in tasks:
            if task.get("status") == TASK_STATUS_RUNNING and str(task.get("task_id", "") or "") != current_id:
                task["status"] = TASK_STATUS_PAUSED
                task["last_action"] = "auto_pause"
                task["last_reason"] = "检测到多个运行中任务，已自动暂停其余任务"
                task["updated_at"] = _now_text()
        running = [task for task in tasks if task.get("status") == TASK_STATUS_RUNNING]
    user_ctx.tasks = tasks
    _sync_runtime_from_task(user_ctx, running[0] if running else None)
    return tasks


def save_tasks(user_ctx) -> None:
    user_ctx.tasks = [_normalize_task(item) for item in list(getattr(user_ctx, "tasks", []))]
    user_ctx.save_tasks()
    normalize_tasks(user_ctx)


def get_tasks(user_ctx) -> List[Dict[str, Any]]:
    return normalize_tasks(user_ctx)


def find_task(user_ctx, ident: str) -> Optional[Dict[str, Any]]:
    ident_text = str(ident or "").strip()
    if not ident_text:
        return None
    tasks = normalize_tasks(user_ctx)
    for task in tasks:
        if task.get("task_id") == ident_text:
            return task
    for task in tasks:
        if str(task.get("name", "")).strip() == ident_text:
            return task
    return None


def _find_task_by_id(user_ctx, task_id: str) -> Optional[Dict[str, Any]]:
    ident_text = str(task_id or "").strip()
    if not ident_text:
        return None
    tasks = normalize_tasks(user_ctx)
    for task in tasks:
        if str(task.get("task_id", "") or "") == ident_text:
            return task
    return None


def current_task(user_ctx) -> Optional[Dict[str, Any]]:
    tasks = normalize_tasks(user_ctx)
    for task in tasks:
        if task.get("status") == TASK_STATUS_RUNNING:
            return task
    return None


def _touch(task: Dict[str, Any], action: str = "", reason: str = "") -> None:
    task["updated_at"] = _now_text()
    if action:
        task["last_action"] = action
    if reason:
        task["last_reason"] = reason


def _task_sort_key(task: Dict[str, Any]) -> Tuple[int, str]:
    return (_safe_int(task.get("priority", 100), 100), str(task.get("created_at", "") or ""))


def _can_start_new_task(rt: Dict[str, Any]) -> bool:
    if bool(rt.get("bet", False)):
        return False
    if bool(rt.get("manual_pause", False)):
        return False
    if _safe_int(rt.get("lose_count", 0), 0) > 0:
        return False
    if _safe_int(rt.get("bet_sequence_count", 0), 0) > 0:
        return False
    return True


def _apply_task_preset(user_ctx, task: Dict[str, Any], new_run: bool) -> None:
    rt = user_ctx.state.runtime
    preset_name = str(task.get("base_preset", "") or "")
    preset = user_ctx.presets.get(preset_name)
    if not isinstance(preset, (list, tuple)) or len(preset) < 7:
        raise ValueError(f"预设不存在：{preset_name}")
    rt["continuous"] = int(preset[0])
    rt["lose_stop"] = int(preset[1])
    rt["lose_once"] = float(preset[2])
    rt["lose_twice"] = float(preset[3])
    rt["lose_three"] = float(preset[4])
    rt["lose_four"] = float(preset[5])
    rt["initial_amount"] = int(preset[6])
    rt["current_preset_name"] = preset_name
    if new_run:
        rt["bet_amount"] = int(preset[6])
        rt["bet"] = False
        rt["bet_on"] = True
        rt["mode_stop"] = True
        rt["manual_pause"] = False
        rt["switch"] = True
        rt["open_ydx"] = False
        rt["risk_deep_triggered_milestones"] = []
        rt["fund_pause_notified"] = False
        rt["limit_stop_notified"] = False
        dynamic_betting.reset_dynamic_sequence(rt)


def _record_task_event(user_ctx, task: Dict[str, Any], event_type: str, note: str = "", **kwargs) -> None:
    rt = user_ctx.state.runtime
    history_analysis.record_task_event(
        user_ctx,
        task_id=str(task.get("task_id", "") or ""),
        task_name=str(task.get("name", "") or ""),
        run_id=str(task.get("current_run_id", "") or kwargs.get("run_id", "")),
        round_key=str(kwargs.get("round_key", rt.get("current_round_key", "")) or ""),
        decision_id=str(kwargs.get("decision_id", rt.get("last_decision_id", "")) or ""),
        bet_id=str(kwargs.get("bet_id", "") or ""),
        event_type=event_type,
        trigger_mode=str(task.get("trigger_mode", "") or ""),
        base_preset=str(task.get("base_preset", "") or ""),
        applied_preset=str(kwargs.get("applied_preset", rt.get("current_dynamic_tier", "") or rt.get("current_preset_name", "")) or ""),
        status_text=str(kwargs.get("status_text", _status_text(str(task.get("status", TASK_STATUS_IDLE) or TASK_STATUS_IDLE))) or ""),
        progress_bets=_safe_int(task.get("progress_bets", 0), 0),
        target_bets=_safe_int(task.get("max_bets", 0), 0),
        profit_delta=_safe_int(kwargs.get("profit_delta", 0), 0),
        cum_profit=_safe_int(task.get("progress_profit", 0), 0),
        cum_loss=_safe_int(task.get("progress_loss", 0), 0),
        note=str(note or ""),
    )


def _start_task(user_ctx, task: Dict[str, Any], trigger_reason: str, force: bool = False) -> Dict[str, Any]:
    rt = user_ctx.state.runtime
    active = current_task(user_ctx)
    task = _find_task_by_id(user_ctx, str(task.get("task_id", "") or "")) or task
    if active and active.get("task_id") != task.get("task_id"):
        return {"ok": False, "message": f"已有运行中任务：{active.get('name', active.get('task_id', ''))}"}
    if not force and not _can_start_new_task(rt):
        task["last_action"] = "waiting"
        task["last_reason"] = "当前仍有进行中的下注序列，任务等待接管"
        task["updated_at"] = _now_text()
        save_tasks(user_ctx)
        return {"ok": False, "message": task["last_reason"], "task": task}
    try:
        _apply_task_preset(user_ctx, task, new_run=True)
    except ValueError as exc:
        task["status"] = TASK_STATUS_PAUSED
        _touch(task, "preset_missing", str(exc))
        save_tasks(user_ctx)
        return {"ok": False, "message": str(exc), "task": task}

    task["status"] = TASK_STATUS_RUNNING
    task["current_run_id"] = _task_run_id()
    task["progress_bets"] = 0
    task["progress_profit"] = 0
    task["progress_loss"] = 0
    task["last_trigger_at"] = _now_text()
    task["total_runs"] = _safe_int(task.get("total_runs", 0), 0) + 1
    _touch(task, "started", trigger_reason)
    _sync_runtime_from_task(user_ctx, task)
    _set_task_event(rt, "started", trigger_reason)
    _record_task_event(user_ctx, task, "started", note=trigger_reason, status_text="运行中")
    save_tasks(user_ctx)
    user_ctx.save_state()
    return {"ok": True, "message": f"任务已启动：{task.get('name', '')}", "task": task}


def _finish_task(user_ctx, task: Dict[str, Any], reason: str, event_type: str = "completed") -> Dict[str, Any]:
    task = _find_task_by_id(user_ctx, str(task.get("task_id", "") or "")) or task
    rt = user_ctx.state.runtime
    task["last_run_bets"] = _safe_int(task.get("progress_bets", 0), 0)
    task["last_run_profit"] = _safe_int(task.get("progress_profit", 0), 0)
    task["last_run_loss"] = _safe_int(task.get("progress_loss", 0), 0)
    task["last_finish_at"] = _now_text()
    task["status"] = TASK_STATUS_IDLE
    task["current_run_id"] = ""
    _touch(task, event_type, reason)
    _record_task_event(user_ctx, task, event_type, note=reason, status_text="已完成")
    _clear_task_runtime(rt)
    _set_task_event(rt, event_type, reason)
    save_tasks(user_ctx)
    user_ctx.save_state()
    return {
        "task_finished": True,
        "summary": (
            f"📦 任务结束：{task.get('name', '')}\n"
            f"原因：{reason}\n"
            f"本轮完成：{task.get('last_run_bets', 0)}/{task.get('max_bets', 0)} 笔\n"
            f"本轮收益：{int(task.get('last_run_profit', 0)):+,}"
        ),
    }


def create_task(
    user_ctx,
    *,
    name: str,
    base_preset: str,
    max_bets: int,
    trigger_mode: str = TASK_MODE_MANUAL,
    interval_minutes: int = 0,
    regimes: Optional[List[str]] = None,
    max_loss: int = 0,
    enabled: bool = False,
) -> Dict[str, Any]:
    if base_preset not in user_ctx.presets:
        return {"ok": False, "message": f"预设不存在：{base_preset}"}
    mode = str(trigger_mode or TASK_MODE_MANUAL).strip().lower()
    if mode not in TASK_TRIGGER_MODES:
        return {"ok": False, "message": f"触发模式不支持：{mode}"}
    task = _normalize_task(
        {
            "task_id": _task_id(),
            "name": str(name or "").strip() or f"任务{len(user_ctx.tasks) + 1}",
            "enabled": bool(enabled),
            "status": TASK_STATUS_IDLE,
            "trigger_mode": mode,
            "interval_minutes": interval_minutes,
            "regimes": regimes or [],
            "base_preset": str(base_preset or "").strip(),
            "max_bets": max_bets,
            "max_loss": max_loss,
            "priority": 100 + len(user_ctx.tasks),
        }
    )
    user_ctx.tasks.append(task)
    save_tasks(user_ctx)
    return {"ok": True, "task": task, "message": f"任务已创建：{task['name']} ({task['task_id']})"}


def create_task_from_template(
    user_ctx,
    template_name: str,
    task_name: str = "",
    *,
    base_preset: str = "",
    max_bets: int = 0,
    max_loss: Optional[int] = None,
    enabled: bool = False,
) -> Dict[str, Any]:
    template = get_task_template(template_name)
    if not template:
        return {"ok": False, "message": f"任务模板不存在：{template_name}"}
    task_label = str(task_name or "").strip() or str(template.get("template_name", "") or "")
    preset_value = str(base_preset or template.get("base_preset", "") or "").strip()
    max_bets_value = max(1, _safe_int(max_bets or template.get("max_bets", 1), 1))
    max_loss_value = _safe_int(template.get("max_loss", 0), 0) if max_loss is None else max(0, _safe_int(max_loss, 0))
    return create_task(
        user_ctx,
        name=task_label,
        base_preset=preset_value,
        max_bets=max_bets_value,
        trigger_mode=str(template.get("trigger_mode", TASK_MODE_MANUAL) or TASK_MODE_MANUAL),
        interval_minutes=_safe_int(template.get("interval_minutes", 0), 0),
        regimes=_normalize_regimes(template.get("regimes", [])),
        max_loss=max_loss_value,
        enabled=bool(enabled),
    )


def parse_create_args(args: List[str]) -> Dict[str, Any]:
    if len(args) < 3:
        return {"ok": False, "message": "用法：task add <名称> <预设> <局数> [manual|schedule|regime|hybrid] [分钟] [盘面列表] [max_loss]"}
    name = str(args[0]).strip()
    base_preset = str(args[1]).strip()
    max_bets = _safe_int(args[2], 0)
    if max_bets <= 0:
        return {"ok": False, "message": "局数必须是大于 0 的整数"}
    if len(args) == 3:
        return {
            "ok": True,
            "name": name,
            "base_preset": base_preset,
            "max_bets": max_bets,
            "trigger_mode": TASK_MODE_MANUAL,
            "interval_minutes": 0,
            "regimes": [],
            "max_loss": 0,
        }
    mode = str(args[3]).strip().lower()
    if mode not in TASK_TRIGGER_MODES:
        return {"ok": False, "message": f"触发模式不支持：{mode}"}
    interval_minutes = 0
    regimes: List[str] = []
    max_loss = 0
    if mode == TASK_MODE_MANUAL:
        if len(args) >= 5:
            max_loss = max(0, _safe_int(args[4], 0))
    elif mode == TASK_MODE_SCHEDULE:
        if len(args) < 5:
            return {"ok": False, "message": "schedule 模式需要分钟数：task add <名称> <预设> <局数> schedule <分钟> [max_loss]"}
        interval_minutes = max(1, _safe_int(args[4], 0))
        if len(args) >= 6:
            max_loss = max(0, _safe_int(args[5], 0))
    elif mode == TASK_MODE_REGIME:
        if len(args) < 5:
            return {"ok": False, "message": "regime 模式需要盘面列表：task add <名称> <预设> <局数> regime 延续盘,衰竭盘 [max_loss]"}
        regimes = _normalize_regimes(args[4])
        if not regimes:
            return {"ok": False, "message": "盘面列表不能为空，支持：延续盘,衰竭盘,反转盘,震荡盘,混乱盘"}
        if len(args) >= 6:
            max_loss = max(0, _safe_int(args[5], 0))
    elif mode == TASK_MODE_HYBRID:
        if len(args) < 6:
            return {"ok": False, "message": "hybrid 模式需要分钟数和盘面列表：task add <名称> <预设> <局数> hybrid <分钟> 延续盘,衰竭盘 [max_loss]"}
        interval_minutes = max(1, _safe_int(args[4], 0))
        regimes = _normalize_regimes(args[5])
        if not regimes:
            return {"ok": False, "message": "盘面列表不能为空，支持：延续盘,衰竭盘,反转盘,震荡盘,混乱盘"}
        if len(args) >= 7:
            max_loss = max(0, _safe_int(args[6], 0))
    return {
        "ok": True,
        "name": name,
        "base_preset": base_preset,
        "max_bets": max_bets,
        "trigger_mode": mode,
        "interval_minutes": interval_minutes,
        "regimes": regimes,
        "max_loss": max_loss,
    }


def set_task_enabled(user_ctx, ident: str, enabled: bool) -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    task["enabled"] = bool(enabled)
    if not enabled and task.get("status") == TASK_STATUS_RUNNING:
        task["status"] = TASK_STATUS_IDLE
        task["current_run_id"] = ""
    _touch(task, "enabled" if enabled else "disabled", "已开启" if enabled else "已关闭")
    rt = user_ctx.state.runtime
    if not enabled and str(rt.get("task_current_id", "") or "") == str(task.get("task_id", "") or ""):
        _clear_task_runtime(rt)
        _set_task_event(rt, "disabled", f"任务已关闭：{task.get('name', '')}")
    save_tasks(user_ctx)
    user_ctx.save_state()
    return {"ok": True, "task": task, "message": f"任务已{'开启' if enabled else '关闭'}：{task.get('name', '')}"}


def pause_task(user_ctx, ident: str, reason: str = "手动暂停") -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    task["status"] = TASK_STATUS_PAUSED
    _touch(task, "paused", reason)
    _record_task_event(user_ctx, task, "paused", note=reason, status_text="已暂停")
    rt = user_ctx.state.runtime
    if str(rt.get("task_current_id", "") or "") == str(task.get("task_id", "") or ""):
        _clear_task_runtime(rt)
        rt["bet_on"] = False
        rt["bet"] = False
        rt["mode_stop"] = True
        _set_task_event(rt, "paused", reason)
        user_ctx.save_state()
    save_tasks(user_ctx)
    return {"ok": True, "task": task, "message": f"任务已暂停：{task.get('name', '')}"}


def stop_task_run(user_ctx, ident: str, reason: str = "任务停止") -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    task["status"] = TASK_STATUS_IDLE
    task["current_run_id"] = ""
    _touch(task, "stopped", reason)
    _record_task_event(user_ctx, task, "stopped", note=reason, status_text="已停止")
    rt = user_ctx.state.runtime
    if str(rt.get("task_current_id", "") or "") == str(task.get("task_id", "") or ""):
        _clear_task_runtime(rt)
        rt["bet_on"] = False
        rt["bet"] = False
        rt["mode_stop"] = True
        _set_task_event(rt, "stopped", reason)
        user_ctx.save_state()
    save_tasks(user_ctx)
    return {"ok": True, "task": task, "message": f"任务已停止：{task.get('name', '')}"}


def resume_task(user_ctx, ident: str) -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    if not bool(task.get("enabled", False)):
        task["enabled"] = True
    if task.get("current_run_id"):
        task["status"] = TASK_STATUS_RUNNING
        _sync_runtime_from_task(user_ctx, task)
        _touch(task, "resumed", "继续当前任务轮次")
        _record_task_event(user_ctx, task, "resumed", note="继续当前任务轮次", status_text="运行中")
        rt = user_ctx.state.runtime
        rt["bet_on"] = True
        rt["manual_pause"] = False
        rt["mode_stop"] = True
        _set_task_event(rt, "resumed", f"任务继续：{task.get('name', '')}")
        save_tasks(user_ctx)
        user_ctx.save_state()
        return {"ok": True, "task": task, "message": f"任务已恢复：{task.get('name', '')}"}
    return _start_task(user_ctx, task, "手动恢复触发", force=True)


def run_task_now(user_ctx, ident: str) -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    task["enabled"] = True
    return _start_task(user_ctx, task, "手动触发", force=True)


def start_task_if_possible(user_ctx, ident: str, reason: str, force: bool = False) -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    task["enabled"] = True
    return _start_task(user_ctx, task, reason, force=force)


def delete_task(user_ctx, ident: str) -> Dict[str, Any]:
    task = find_task(user_ctx, ident)
    if not task:
        return {"ok": False, "message": f"任务不存在：{ident}"}
    user_ctx.tasks = [item for item in normalize_tasks(user_ctx) if item.get("task_id") != task.get("task_id")]
    rt = user_ctx.state.runtime
    if str(rt.get("task_current_id", "") or "") == str(task.get("task_id", "") or ""):
        _clear_task_runtime(rt)
        _set_task_event(rt, "deleted", f"任务已删除：{task.get('name', '')}")
        user_ctx.save_state()
    save_tasks(user_ctx)
    return {"ok": True, "message": f"任务已删除：{task.get('name', '')}"}


def _task_due(task: Dict[str, Any], snapshot: Dict[str, Any]) -> Tuple[bool, str]:
    trigger_mode = str(task.get("trigger_mode", TASK_MODE_MANUAL) or TASK_MODE_MANUAL)
    regime_label = str(snapshot.get("regime_label", history_analysis.REGIME_RANGE) or history_analysis.REGIME_RANGE)
    regimes = _normalize_regimes(task.get("regimes", []))
    last_trigger_at = _safe_dt(task.get("last_trigger_at", ""))
    interval_minutes = max(0, _safe_int(task.get("interval_minutes", 0), 0))
    time_due = True
    if trigger_mode in {TASK_MODE_SCHEDULE, TASK_MODE_HYBRID}:
        if interval_minutes <= 0:
            interval_minutes = 30
        if last_trigger_at is not None:
            time_due = datetime.now() >= last_trigger_at + timedelta(minutes=interval_minutes)
    regime_due = True
    if trigger_mode in {TASK_MODE_REGIME, TASK_MODE_HYBRID}:
        regime_due = regime_label in regimes if regimes else True
    if trigger_mode == TASK_MODE_MANUAL:
        return False, "手动任务不自动触发"
    if trigger_mode == TASK_MODE_SCHEDULE:
        return time_due, f"定时触发({interval_minutes}分钟)" if time_due else "未到下次定时触发"
    if trigger_mode == TASK_MODE_REGIME:
        return regime_due, f"盘面触发({regime_label})" if regime_due else f"当前盘面 {regime_label} 未命中"
    due = time_due and regime_due
    if due:
        return True, f"混合触发({interval_minutes}分钟 + {regime_label})"
    if not time_due and not regime_due:
        return False, f"未到定时且当前盘面 {regime_label} 未命中"
    if not time_due:
        return False, "未到下次定时触发"
    return False, f"当前盘面 {regime_label} 未命中"


def prepare_task_for_round(user_ctx, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    tasks = sorted(normalize_tasks(user_ctx), key=_task_sort_key)
    active = current_task(user_ctx)
    if active:
        try:
            _apply_task_preset(user_ctx, active, new_run=False)
            return {"active": True, "started": False, "task": active, "message": f"任务继续运行：{active.get('name', '')}"}
        except ValueError as exc:
            active["status"] = TASK_STATUS_PAUSED
            _touch(active, "preset_missing", str(exc))
            save_tasks(user_ctx)
            return {"active": False, "started": False, "task": active, "message": str(exc)}

    for task in tasks:
        if not bool(task.get("enabled", False)):
            continue
        if task.get("status") == TASK_STATUS_PAUSED:
            continue
        due, reason = _task_due(task, snapshot)
        if not due:
            task["last_action"] = "waiting"
            task["last_reason"] = reason
            continue
        result = _start_task(user_ctx, task, reason, force=False)
        result.setdefault("task", task)
        result["active"] = bool(result.get("ok", False))
        result["started"] = bool(result.get("ok", False))
        return result
    user_ctx.tasks = tasks
    return {"active": False, "started": False, "message": "当前无待执行任务"}


def record_round_action(
    user_ctx,
    *,
    event_type: str,
    note: str = "",
    applied_preset: str = "",
    bet_id: str = "",
    profit_delta: int = 0,
) -> None:
    task = current_task(user_ctx)
    if not task:
        return
    rt = user_ctx.state.runtime
    _touch(task, event_type, note)
    _set_task_event(rt, event_type, note)
    _record_task_event(
        user_ctx,
        task,
        event_type,
        note=note,
        applied_preset=applied_preset,
        bet_id=bet_id,
        profit_delta=profit_delta,
    )
    save_tasks(user_ctx)


def record_settlement(user_ctx, settled_entry: Dict[str, Any], profit: int) -> Dict[str, Any]:
    task = current_task(user_ctx)
    if not task:
        return {"task_finished": False, "summary": ""}
    task["progress_bets"] = _safe_int(task.get("progress_bets", 0), 0) + 1
    task["progress_profit"] = _safe_int(task.get("progress_profit", 0), 0) + _safe_int(profit, 0)
    if _safe_int(profit, 0) < 0:
        task["progress_loss"] = _safe_int(task.get("progress_loss", 0), 0) + abs(_safe_int(profit, 0))
    task["total_bets"] = _safe_int(task.get("total_bets", 0), 0) + 1
    task["total_profit"] = _safe_int(task.get("total_profit", 0), 0) + _safe_int(profit, 0)
    _touch(task, "settled", f"任务结算 {profit:+,}")
    _set_task_event(user_ctx.state.runtime, "settled", f"任务结算 {profit:+,}")
    _record_task_event(
        user_ctx,
        task,
        "settled",
        note=f"任务结算 {profit:+,}",
        bet_id=str(settled_entry.get("bet_id", "") or ""),
        applied_preset=str(settled_entry.get("dynamic_tier", "") or settled_entry.get("preset_name", "") or ""),
        profit_delta=_safe_int(profit, 0),
    )
    save_tasks(user_ctx)

    max_bets = _safe_int(task.get("max_bets", 0), 0)
    max_loss = _safe_int(task.get("max_loss", 0), 0)
    if max_loss > 0 and _safe_int(task.get("progress_loss", 0), 0) >= max_loss:
        return _finish_task(user_ctx, task, f"达到任务亏损上限 {max_loss:,}", event_type="stop_loss")
    if max_bets > 0 and _safe_int(task.get("progress_bets", 0), 0) >= max_bets:
        return _finish_task(user_ctx, task, f"完成目标 {max_bets} 笔", event_type="completed")
    _sync_runtime_from_task(user_ctx, task)
    user_ctx.save_state()
    return {"task_finished": False, "summary": ""}


def build_task_focus_text(user_ctx) -> str:
    task = current_task(user_ctx)
    if not task:
        enabled_count = sum(1 for item in normalize_tasks(user_ctx) if bool(item.get("enabled", False)))
        return f"📦 任务提醒：当前无运行中任务（已启用 {enabled_count} 个，可用 `task` 查看）"
    return (
        f"📦 任务提醒：{task.get('name', '')} | {_status_text(str(task.get('status', TASK_STATUS_IDLE) or TASK_STATUS_IDLE))} | "
        f"进度 {task.get('progress_bets', 0)}/{task.get('max_bets', 0)} | 基准 {task.get('base_preset', '')}"
    )


def build_task_overview_text(user_ctx) -> str:
    tasks = normalize_tasks(user_ctx)
    active = current_task(user_ctx)
    enabled_count = sum(1 for item in tasks if bool(item.get("enabled", False)))
    lines = ["📦 任务总览", ""]
    if active:
        lines.extend(
            [
                f"当前运行：{active.get('name', '')}",
                f"任务ID：{active.get('task_id', '')}",
                f"触发：{_trigger_text(str(active.get('trigger_mode', '') or ''), _safe_int(active.get('interval_minutes', 0), 0), _normalize_regimes(active.get('regimes', [])))}",
                f"基准预设：{active.get('base_preset', '')}",
                f"进度：{active.get('progress_bets', 0)}/{active.get('max_bets', 0)}",
                f"本轮收益：{int(active.get('progress_profit', 0)):+,}",
                f"最近动作：{active.get('last_action', '') or '无'} | {active.get('last_reason', '') or '无'}",
            ]
        )
    else:
        lines.append("当前运行：无")
    lines.extend(
        [
            "",
            f"任务总数：{len(tasks)} | 已启用：{enabled_count}",
            "命令：`task tpl` / `task new <模板>` / `task list`",
            "详情：`task add ...` / `task show <id>` / `task run <id>`",
            "控制：`task pause <id>` / `task resume <id>` / `task logs [id]` / `task stats [id]`",
        ]
    )
    return "\n".join(lines)


def build_task_list_text(user_ctx) -> str:
    tasks = sorted(normalize_tasks(user_ctx), key=_task_sort_key)
    if not tasks:
        return "📦 暂无任务\n\n先看模板：`task tpl`\n快速创建：`task new 保守巡航`\n完整创建：`task add <名称> <预设> <局数> [manual|schedule|regime|hybrid] ...`"
    lines = ["📦 任务列表", ""]
    for index, task in enumerate(tasks, 1):
        lines.append(
            f"{index}. {task.get('name', '')} | {task.get('task_id', '')} | {_status_text(str(task.get('status', TASK_STATUS_IDLE) or TASK_STATUS_IDLE))} | "
            f"{'已启用' if bool(task.get('enabled', False)) else '已关闭'} | {task.get('base_preset', '')} | "
            f"{_trigger_text(str(task.get('trigger_mode', '') or ''), _safe_int(task.get('interval_minutes', 0), 0), _normalize_regimes(task.get('regimes', [])))}"
        )
    return "\n".join(lines)


def build_task_detail_text(user_ctx, ident: str) -> str:
    task = find_task(user_ctx, ident)
    if not task:
        return f"❌ 任务不存在：{ident}"
    return (
        "📦 任务详情\n\n"
        f"名称：{task.get('name', '')}\n"
        f"任务ID：{task.get('task_id', '')}\n"
        f"状态：{_status_text(str(task.get('status', TASK_STATUS_IDLE) or TASK_STATUS_IDLE))}\n"
        f"启用：{'是' if bool(task.get('enabled', False)) else '否'}\n"
        f"触发：{_trigger_text(str(task.get('trigger_mode', '') or ''), _safe_int(task.get('interval_minutes', 0), 0), _normalize_regimes(task.get('regimes', [])))}\n"
        f"基准预设：{task.get('base_preset', '')}\n"
        f"目标笔数：{task.get('max_bets', 0)}\n"
        f"亏损上限：{_safe_int(task.get('max_loss', 0), 0):,}\n"
        f"本轮进度：{task.get('progress_bets', 0)}/{task.get('max_bets', 0)}\n"
        f"本轮收益：{int(task.get('progress_profit', 0)):+,}\n"
        f"本轮亏损：{int(task.get('progress_loss', 0)):+,}\n"
        f"累计运行：{task.get('total_runs', 0)} 轮 / {task.get('total_bets', 0)} 笔\n"
        f"累计收益：{int(task.get('total_profit', 0)):+,}\n"
        f"最近动作：{task.get('last_action', '') or '无'}\n"
        f"最近原因：{task.get('last_reason', '') or '无'}\n"
        f"上次触发：{task.get('last_trigger_at', '') or '无'}\n"
        f"上次结束：{task.get('last_finish_at', '') or '无'}"
    )


def _task_log_rows(user_ctx, task_id: str = "", limit: int = 10) -> List[Dict[str, Any]]:
    db_path = _analytics_db_path(user_ctx)
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        history_analysis._ensure_analytics_schema(conn)
        if task_id:
            rows = conn.execute(
                "SELECT * FROM task_runs WHERE task_id = ? ORDER BY created_at DESC LIMIT ?",
                (task_id, max(1, int(limit))),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM task_runs ORDER BY created_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def build_task_logs_text(user_ctx, ident: str = "") -> str:
    task = find_task(user_ctx, ident) if ident else None
    rows = _task_log_rows(user_ctx, task.get("task_id", "") if task else ident, limit=10)
    if not rows:
        return "📦 暂无任务运行记录"
    lines = ["📦 任务运行记录", ""]
    for row in rows:
        lines.append(
            f"- {row.get('created_at', '')} | {row.get('task_name', '')} | {row.get('event_type', '')} | "
            f"进度 {row.get('progress_bets', 0)}/{row.get('target_bets', 0)} | "
            f"收益 {int(row.get('profit_delta', 0) or 0):+,} | {row.get('note', '') or '-'}"
        )
    return "\n".join(lines)


def build_task_stats_text(user_ctx, ident: str = "") -> str:
    task = find_task(user_ctx, ident) if ident else None
    tasks = [task] if task else normalize_tasks(user_ctx)
    tasks = [item for item in tasks if item]
    if not tasks:
        return "📦 暂无任务统计"
    total_runs = sum(_safe_int(item.get("total_runs", 0), 0) for item in tasks)
    total_bets = sum(_safe_int(item.get("total_bets", 0), 0) for item in tasks)
    total_profit = sum(_safe_int(item.get("total_profit", 0), 0) for item in tasks)
    active_count = sum(1 for item in tasks if item.get("status") == TASK_STATUS_RUNNING)
    paused_count = sum(1 for item in tasks if item.get("status") == TASK_STATUS_PAUSED)
    lines = [
        "📦 任务统计",
        "",
        f"任务数：{len(tasks)}",
        f"运行中：{active_count} | 已暂停：{paused_count}",
        f"累计运行轮次：{total_runs}",
        f"累计真实下注：{total_bets} 笔",
        f"累计收益：{total_profit:+,}",
    ]
    if len(tasks) == 1:
        item = tasks[0]
        lines.extend(
            [
                f"任务名称：{item.get('name', '')}",
                f"最近完成：{item.get('last_finish_at', '') or '无'}",
                f"最近一轮：{item.get('last_run_bets', 0)} 笔 / {int(item.get('last_run_profit', 0)):+,}",
            ]
        )
    return "\n".join(lines)
