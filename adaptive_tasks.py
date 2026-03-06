import math
import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import constants
from adaptive_analytics import (
    append_task_action,
    finish_task_run,
    linkage_coverage_report,
    refresh_regime_preset_stats,
    start_task_run,
)
from adaptive_strategy import PRESET_LADDER, build_recommendation, compute_regime, compute_loss_limits

TIME_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"
TASK_MODE_STATES = {"idle", "running", "paused_manual", "paused_risk"}
REGIME_TEXT_MAP = {
    "TREND_CONTINUATION": "趋势延续",
    "TREND_EXHAUSTION": "趋势衰竭",
    "REVERSAL_SETUP": "反转酝酿",
    "RANGE": "区间震荡",
    "CHAOS": "混沌震荡",
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _now() -> datetime:
    return datetime.now()


def _now_text() -> str:
    return _now().strftime(TIME_FMT)


def _today_text() -> str:
    return _now().strftime(DATE_FMT)


def _status_text(status: Any) -> str:
    text = str(status or "").strip().lower()
    mapping = {
        "idle": "待机",
        "running": "运行中",
        "paused": "已暂停",
        "paused_manual": "手动暂停",
        "paused_risk": "风控暂停",
        "completed": "已完成",
        "stopped": "已结束",
        "loss_stop": "亏损止损",
        "daily_stop": "日损止损",
        "consecutive_stop": "连输止损",
    }
    return mapping.get(text, text or "未知")


def _mode_text(mode: Any) -> str:
    text = str(mode or "").strip().lower()
    mapping = {
        "time": "定时触发",
        "regime": "盘面触发",
        "hybrid": "混合触发",
    }
    return mapping.get(text, text or "未知")


def _trigger_type_text(trigger_type: Any) -> str:
    text = str(trigger_type or "").strip().lower()
    mapping = {
        "manual": "手动",
        "time": "定时",
        "regime": "盘面",
        "hybrid": "混合",
    }
    return mapping.get(text, text or "-")


def _switch_text(enabled: bool) -> str:
    return "开启" if enabled else "关闭"


def _task_mode_state_text(state: Any) -> str:
    text = _normalize_task_mode_state(state)
    mapping = {
        "idle": "待机",
        "running": "运行中",
        "paused_manual": "手动暂停",
        "paused_risk": "风控暂停",
    }
    return mapping.get(text, "待机")


def _fmt_time_text(value: Any) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return "-"
    return parsed.strftime(TIME_FMT)


def _pct_text(value: Any, default: float = 0.0) -> str:
    return f"{_safe_float(value, default) * 100.0:.2f}%"


def regime_text(regime: Any) -> str:
    text = str(regime or "").strip().upper()
    if not text:
        return "-"
    return REGIME_TEXT_MAP.get(text, text)


def regime_catalog_text() -> str:
    return "趋势延续、趋势衰竭、区间震荡、反转酝酿、混沌震荡（共5类）"


def _freeze_remaining_text(freeze_until: Any) -> str:
    freeze_dt = _parse_time(freeze_until)
    if freeze_dt is None:
        return "-"
    delta_seconds = (freeze_dt - _now()).total_seconds()
    if delta_seconds <= 0:
        return "已到期"
    minutes = int(math.ceil(delta_seconds / 60.0))
    if minutes < 60:
        return f"约 {minutes} 分钟"
    hours = minutes // 60
    remain_minutes = minutes % 60
    if remain_minutes == 0:
        return f"约 {hours} 小时"
    return f"约 {hours} 小时 {remain_minutes} 分钟"


def _parse_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in (TIME_FMT, "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _normalize_task_mode_state(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in TASK_MODE_STATES:
        return text
    return "idle"


def _task_list(user_ctx: Any) -> List[Dict[str, Any]]:
    if hasattr(user_ctx, "get_tasks"):
        tasks = user_ctx.get_tasks()
        return tasks if isinstance(tasks, list) else []
    tasks_data = getattr(user_ctx, "tasks", {})
    tasks = tasks_data.get("tasks", []) if isinstance(tasks_data, dict) else []
    return tasks if isinstance(tasks, list) else []


def _find_task(user_ctx: Any, name: str) -> Optional[Dict[str, Any]]:
    name = str(name or "").strip()
    if not name:
        return None
    for task in _task_list(user_ctx):
        if str(task.get("name", "")).strip() == name:
            return task
    return None


def _get_task_runtime(task: Dict[str, Any]) -> Dict[str, Any]:
    rt = task.get("runtime", {}) if isinstance(task.get("runtime", {}), dict) else {}
    task["runtime"] = rt
    return rt


def _is_task_paused(task: Dict[str, Any]) -> bool:
    runtime = _get_task_runtime(task)
    status = str(runtime.get("status", "idle") or "idle").lower()
    return status in {"paused", "paused_manual", "paused_risk"}


def _persist_tasks(user_ctx: Any) -> None:
    if hasattr(user_ctx, "save_tasks"):
        user_ctx.save_tasks()


def _normalize_new_task(
    name: str,
    cron_minutes: int = 0,
    mode: str = "hybrid",
    task_loss_pct: float = 0.006,
    daily_loss_pct: float = 0.02,
    max_consecutive_losses: int = 4,
) -> Dict[str, Any]:
    trigger_mode = str(mode or "hybrid").strip().lower()
    if trigger_mode not in {"time", "regime", "hybrid"}:
        trigger_mode = "hybrid"

    task = {
        "name": str(name).strip(),
        "enabled": True,
        "trigger": {
            "mode": trigger_mode,
            "cron_minutes": max(0, _safe_int(cron_minutes, 0)),
            "min_interval_minutes": 10,
        },
        "candidate_presets": ["yc1", "yc5", "yc10", "yc20", "yc50", "yc100", "yc200"],
        "top_k_cases": 50,
        "min_rounds": 8,
        "max_rounds": 30,
        "task_loss_pct": float(task_loss_pct),
        "daily_loss_pct": float(daily_loss_pct),
        "max_consecutive_losses": int(max_consecutive_losses),
        "fund_base_ratio_limit": 0.05,
        "fund_min_base_floor": 1000,
        "fund_reserve_amount": 0,
        "high_tier_sample_min": 120,
        "high_tier_conf_min": 0.78,
        "high_tier_win_rate_min": 0.57,
        "high_tier_dd_ratio_max": 0.4,
        "policy_id": "adaptive-v1",
        "runtime": {
            "status": "idle",
            "last_trigger_at": "",
            "last_run_id": "",
            "last_run_status": "",
            "last_run_pnl": 0,
            "last_run_max_dd": 0,
            "last_reason": "",
            "next_due_at": "",
        },
    }
    return task


def create_task(
    user_ctx: Any,
    name: str,
    cron_minutes: int = 0,
    mode: str = "hybrid",
    task_loss_pct: Optional[float] = None,
    daily_loss_pct: Optional[float] = None,
    max_consecutive_losses: Optional[int] = None,
) -> Dict[str, Any]:
    name = str(name or "").strip()
    if not name:
        return {"ok": False, "error": "task name is required"}
    if _find_task(user_ctx, name) is not None:
        return {"ok": False, "error": f"task `{name}` already exists"}

    task_loss_pct_val = _safe_float(task_loss_pct, 0.006) if task_loss_pct is not None else 0.006
    daily_loss_pct_val = _safe_float(daily_loss_pct, 0.02) if daily_loss_pct is not None else 0.02
    max_consecutive_losses_val = (
        _safe_int(max_consecutive_losses, 4) if max_consecutive_losses is not None else 4
    )

    if not (0 < task_loss_pct_val <= 1):
        return {"ok": False, "error": "task_loss_pct must be in (0, 1]"}
    if not (0 < daily_loss_pct_val <= 1):
        return {"ok": False, "error": "daily_loss_pct must be in (0, 1]"}
    if max_consecutive_losses_val < 1:
        return {"ok": False, "error": "max_consecutive_losses must be >= 1"}

    new_task = _normalize_new_task(
        name,
        cron_minutes=cron_minutes,
        mode=mode,
        task_loss_pct=task_loss_pct_val,
        daily_loss_pct=daily_loss_pct_val,
        max_consecutive_losses=max_consecutive_losses_val,
    )
    if hasattr(user_ctx, "_normalize_task"):
        try:
            new_task = user_ctx._normalize_task(new_task)
        except Exception:
            pass

    if not isinstance(user_ctx.tasks, dict):
        user_ctx.tasks = {"version": 1, "tasks": []}
    tasks = user_ctx.tasks.get("tasks", [])
    if not isinstance(tasks, list):
        tasks = []
        user_ctx.tasks["tasks"] = tasks
    tasks.append(new_task)
    _persist_tasks(user_ctx)
    return {"ok": True, "task": new_task}


def set_task_enabled(user_ctx: Any, name: str, enabled: bool) -> Dict[str, Any]:
    task = _find_task(user_ctx, name)
    if task is None:
        return {"ok": False, "error": f"task `{name}` not found"}
    task["enabled"] = bool(enabled)
    runtime = _get_task_runtime(task)
    if not enabled:
        runtime["status"] = "paused"
    elif str(runtime.get("status", "")).lower() == "paused":
        runtime["status"] = "idle"
    _persist_tasks(user_ctx)
    return {"ok": True, "task": task}


def get_task_mode_state(user_ctx: Any) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    state = _task_mode_state(rt)
    run_id = _active_run_id(user_ctx)
    task_name = _active_task_name(user_ctx)

    if state == "running" and not run_id:
        state = "idle"
        _set_task_mode_state(rt, state, "")
    elif state == "idle" and run_id:
        inferred = "paused_manual" if bool(rt.get("manual_pause", False)) else "running"
        _set_task_mode_state(rt, inferred, "")
        state = inferred

    if state == "paused_risk":
        # 轻度风控已改为“暂停态”，不再使用冻结倒计时。
        rt["task_freeze_until"] = ""

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()

    return {
        "ok": True,
        "state": state,
        "task_name": task_name,
        "task_run_id": run_id,
        "pause_reason": str(rt.get("task_mode_pause_reason", "") or ""),
        "freeze_until": _fmt_time_text(rt.get("task_freeze_until", "")),
    }


def pause_task_mode(user_ctx: Any, reason: str = "manual_pause") -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    state = _task_mode_state(rt)
    if state == "paused_manual":
        return {"ok": False, "error": "already_paused", "state": state}

    run_id = _active_run_id(user_ctx)
    task_name = _active_task_name(user_ctx)
    _set_task_mode_state(rt, "paused_manual", reason)
    _sync_active_task_runtime_status(user_ctx, "paused_manual", str(reason or "manual_pause"))

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {
        "ok": True,
        "state": "paused_manual",
        "task_name": task_name,
        "task_run_id": run_id,
        "reason": str(reason or "manual_pause"),
    }


def resume_task_mode(user_ctx: Any) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    state = _task_mode_state(rt)
    if state not in {"paused_manual", "paused_risk"}:
        return {"ok": False, "error": "not_paused", "state": state}

    rt["task_freeze_until"] = ""

    run_id = _active_run_id(user_ctx)
    task_name = _active_task_name(user_ctx)
    next_state = "running" if run_id else "idle"
    _set_task_mode_state(rt, next_state, "")
    if run_id:
        _sync_active_task_runtime_status(user_ctx, "running", "manual_resume")

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {
        "ok": True,
        "state": next_state,
        "task_name": task_name,
        "task_run_id": run_id,
    }


def list_tasks(user_ctx: Any) -> List[Dict[str, Any]]:
    return _task_list(user_ctx)


def format_task_list(user_ctx: Any) -> str:
    tasks = _task_list(user_ctx)
    if not tasks:
        return "暂无任务。可用 `task new <name> [cron_minutes] [time|regime|hybrid]` 创建。"

    lines = ["🧩 任务列表："]
    for item in tasks:
        runtime = _get_task_runtime(item)
        status = str(runtime.get("status", "idle") or "idle")
        enabled = bool(item.get("enabled", True))
        trigger = item.get("trigger", {}) if isinstance(item.get("trigger"), dict) else {}
        mode = str(trigger.get("mode", "hybrid") or "hybrid")
        cron_minutes = _safe_int(trigger.get("cron_minutes", 0), 0)
        min_interval = _safe_int(trigger.get("min_interval_minutes", 10), 10)
        last = _fmt_time_text(runtime.get("last_trigger_at", ""))
        name = str(item.get("name", "-") or "-")
        risk_text = (
            f"任务止损={_pct_text(item.get('task_loss_pct', 0.006), 0.006)} | "
            f"日损={_pct_text(item.get('daily_loss_pct', 0.02), 0.02)} | "
            f"最大连输={max(1, _safe_int(item.get('max_consecutive_losses', 4), 4))}"
        )
        lines.append(
            f"- `{name}`：{'启用' if enabled else '停用'} | {_status_text(status)} | 触发={_mode_text(mode)} | "
            f"定时={cron_minutes}分钟 | 最小间隔={min_interval}分钟 | 最近触发={last}\n"
            f"  风控：{risk_text}"
        )
    return "\n".join(lines)


def format_task_detail(user_ctx: Any, name: str) -> str:
    task = _find_task(user_ctx, name)
    if task is None:
        return f"任务 `{name}` 不存在"

    runtime = _get_task_runtime(task)
    trigger = task.get("trigger", {}) if isinstance(task.get("trigger"), dict) else {}
    rt = _get_rt(user_ctx)
    current_name = str(rt.get("current_task_name", "") or "")
    current_run_id = str(rt.get("current_task_run_id", "") or "")
    task_mode_state = _task_mode_state(rt)
    is_current = bool(current_run_id and current_name == name)
    last_run_status_raw = str(runtime.get("last_run_status", "") or "")
    preset_list = task.get("candidate_presets", [])
    preset_text = ", ".join(preset_list[:12]) if isinstance(preset_list, list) else "-"
    if isinstance(preset_list, list) and len(preset_list) > 12:
        preset_text += f" ...（共 {len(preset_list)} 个）"

    lines = [
        f"🧩 任务 `{name}` 详情",
        f"- 开关状态：{'启用' if bool(task.get('enabled', True)) else '停用'} / {_status_text(runtime.get('status', 'idle'))}",
        f"- 触发模式：{_mode_text(trigger.get('mode', 'hybrid'))}",
        f"- 定时间隔：{_safe_int(trigger.get('cron_minutes', 0), 0)} 分钟（0 表示不按时间触发）",
        f"- 最小触发间隔：{_safe_int(trigger.get('min_interval_minutes', 10), 10)} 分钟",
        f"- 候选预设：{preset_text}",
        f"- 策略版本：{task.get('policy_id', 'adaptive-v1')}",
        (
            "- 风控阈值："
            f"任务止损 {_pct_text(task.get('task_loss_pct', 0.006), 0.006)} | "
            f"日损 {_pct_text(task.get('daily_loss_pct', 0.02), 0.02)} | "
            f"最大连输 {max(1, _safe_int(task.get('max_consecutive_losses', 4), 4))}"
        ),
        (
            "- 资金门控："
            f"底注≤资金×{_safe_float(task.get('fund_base_ratio_limit', 0.05), 0.05) * 100.0:.1f}%"
            f"（最低底注 {_safe_int(task.get('fund_min_base_floor', 1000), 1000)}，"
            f"预留资金 {_safe_int(task.get('fund_reserve_amount', 0), 0)}）"
        ),
        f"- 最近触发时间：{_fmt_time_text(runtime.get('last_trigger_at', ''))}",
        f"- 最近运行ID：{str(runtime.get('last_run_id', '') or '-')}",
        f"- 最近运行状态：{_status_text(last_run_status_raw) if last_run_status_raw else '-'}",
        f"- 最近运行收益：{_safe_int(runtime.get('last_run_pnl', 0), 0)}",
        f"- 最近最大回撤：{_safe_int(runtime.get('last_run_max_dd', 0), 0)}",
    ]

    if is_current:
        planned = _safe_int(rt.get("task_step_planned_rounds", 0), 0)
        executed = _safe_int(rt.get("task_step_executed_rounds", 0), 0)
        remain = _safe_int(rt.get("task_step_remaining_rounds", 0), 0)
        recheck_interval = max(1, _safe_int(rt.get("task_recheck_interval", 3), 3))
        mod = executed % recheck_interval
        recheck_left = recheck_interval if executed <= 0 or mod == 0 else (recheck_interval - mod)
        lines.extend(
            [
                "- 当前运行状态：运行中 ✅",
                f"- 任务模式状态：{_task_mode_state_text(task_mode_state)}",
                f"- 当前 run_id：{current_run_id}",
                f"- 触发来源：{_trigger_type_text(rt.get('current_task_trigger', ''))}",
                f"- 当前盘面：{regime_text(rt.get('task_regime', '-'))}",
                f"- 当前预设：{str(rt.get('current_preset_name', '') or '-')}",
                f"- 任务进度：第 {_safe_int(rt.get('task_step_no', 0), 0)} 段，已执行 {executed}/{planned}，剩余 {remain}",
                f"- 复评倒计时：约 {max(1, recheck_left)} 局",
                f"- 运行累计：收益 {_safe_int(rt.get('task_run_pnl', 0), 0)} | 最大回撤 {_safe_int(rt.get('task_run_max_dd', 0), 0)}",
            ]
        )
    elif current_run_id:
        lines.append(f"- 当前运行任务：`{current_name}`（run_id: {current_run_id}）")
    return "\n".join(lines)


def _task_min_interval_ok(task: Dict[str, Any], now: datetime) -> bool:
    runtime = _get_task_runtime(task)
    trigger = task.get("trigger", {}) if isinstance(task.get("trigger"), dict) else {}
    min_interval = max(1, _safe_int(trigger.get("min_interval_minutes", 10), 10))
    last_trigger_at = _parse_time(runtime.get("last_trigger_at", ""))
    if last_trigger_at is None:
        return True
    return (now - last_trigger_at) >= timedelta(minutes=min_interval)


def _task_time_due(task: Dict[str, Any], now: datetime) -> bool:
    trigger = task.get("trigger", {}) if isinstance(task.get("trigger"), dict) else {}
    cron_minutes = max(0, _safe_int(trigger.get("cron_minutes", 0), 0))
    if cron_minutes <= 0:
        return False
    runtime = _get_task_runtime(task)
    last_trigger_at = _parse_time(runtime.get("last_trigger_at", ""))
    if last_trigger_at is None:
        return True
    return (now - last_trigger_at) >= timedelta(minutes=cron_minutes)


def _task_regime_due(user_ctx: Any) -> bool:
    history = list(getattr(getattr(user_ctx, "state", None), "history", []) or [])
    if len(history) < 40:
        return False
    sig = compute_regime(history)
    regime = str(sig.get("regime", "") or "")
    confidence = _safe_float(sig.get("confidence", 0.0), 0.0)
    return regime != "CHAOS" and confidence >= 0.62


def _task_is_due(user_ctx: Any, task: Dict[str, Any], now: datetime) -> Tuple[bool, str]:
    if not bool(task.get("enabled", True)):
        return False, "disabled"
    if _is_task_paused(task):
        return False, "paused"
    if not _task_min_interval_ok(task, now):
        return False, "min_interval"

    trigger = task.get("trigger", {}) if isinstance(task.get("trigger"), dict) else {}
    mode = str(trigger.get("mode", "hybrid") or "hybrid").lower()
    time_due = _task_time_due(task, now)
    regime_due = _task_regime_due(user_ctx)

    if mode == "time":
        return time_due, "time"
    if mode == "regime":
        return regime_due, "regime"
    # hybrid
    due = time_due or regime_due
    trigger_type = "time" if time_due else ("regime" if regime_due else "hybrid")
    return due, trigger_type


def _run_id() -> str:
    return f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _get_rt(user_ctx: Any) -> Dict[str, Any]:
    state = getattr(user_ctx, "state", None)
    runtime = getattr(state, "runtime", None)
    return runtime if isinstance(runtime, dict) else {}


def _task_mode_state(rt: Dict[str, Any]) -> str:
    state = _normalize_task_mode_state(rt.get("task_mode_state", "idle"))
    rt["task_mode_state"] = state
    return state


def _set_task_mode_state(rt: Dict[str, Any], state: str, reason: str = "") -> None:
    rt["task_mode_state"] = _normalize_task_mode_state(state)
    rt["task_mode_pause_reason"] = str(reason or "").strip()


def _sync_active_task_runtime_status(user_ctx: Any, status: str, reason: str = "") -> None:
    task_name = _active_task_name(user_ctx)
    if not task_name:
        return
    task = _find_task(user_ctx, task_name)
    if task is None:
        return
    runtime = _get_task_runtime(task)
    runtime["status"] = str(status or "idle")
    if reason:
        runtime["last_reason"] = str(reason)


def _preset_base_amount(user_ctx: Any, preset_name: str) -> int:
    name = str(preset_name or "").strip()
    if not name:
        return 0
    presets = getattr(user_ctx, "presets", {})
    if isinstance(presets, dict):
        values = presets.get(name)
        if isinstance(values, list) and len(values) >= 7:
            return max(0, _safe_int(values[6], 0))
    values = constants.PRESETS.get(name)
    if isinstance(values, list) and len(values) >= 7:
        return max(0, _safe_int(values[6], 0))
    return 0


def normalize_preset_for_active_loss_streak(user_ctx: Any, preset_name: str) -> str:
    """
    连输阶段禁止降档：
    - `lose_floor_preset` 记录本轮连输“首注档位”并作为最低档位；
    - 仅在 lose_count > 0 时生效，连输结束后自动清空。
    """
    rt = _get_rt(user_ctx)
    lose_count = max(0, _safe_int(rt.get("lose_count", 0), 0))
    target_name = str(preset_name or "").strip()
    if not target_name:
        return ""

    if lose_count <= 0:
        rt["lose_floor_preset"] = ""
        return target_name

    floor_name = str(rt.get("lose_floor_preset", "") or "").strip()
    floor_amount = _preset_base_amount(user_ctx, floor_name)
    if floor_amount <= 0:
        current_name = str(rt.get("current_preset_name", "") or "").strip()
        current_amount = _preset_base_amount(user_ctx, current_name)
        if current_amount > 0:
            floor_name = current_name
            floor_amount = current_amount
            rt["lose_floor_preset"] = floor_name
        else:
            return target_name

    target_amount = _preset_base_amount(user_ctx, target_name)
    if target_amount <= 0:
        return target_name

    if target_amount < floor_amount:
        return floor_name
    return target_name


def _apply_preset(user_ctx: Any, preset_name: str) -> str:
    target_name = normalize_preset_for_active_loss_streak(user_ctx, preset_name)
    if not target_name:
        return ""
    presets = getattr(user_ctx, "presets", {})
    if not isinstance(presets, dict):
        return ""
    preset = presets.get(target_name)
    if not isinstance(preset, list) or len(preset) < 7:
        return ""

    rt = _get_rt(user_ctx)
    rt["continuous"] = int(preset[0])
    rt["lose_stop"] = int(preset[1])
    rt["lose_once"] = float(preset[2])
    rt["lose_twice"] = float(preset[3])
    rt["lose_three"] = float(preset[4])
    rt["lose_four"] = float(preset[5])
    rt["initial_amount"] = int(preset[6])
    rt["bet_amount"] = int(preset[6])
    rt["current_preset_name"] = target_name
    rt["switch"] = True
    rt["manual_pause"] = False
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["bet"] = False
    return target_name


def _set_current_task_runtime(rt: Dict[str, Any], task: Dict[str, Any], run_id: str, trigger_type: str, rec: Dict[str, Any], limits: Dict[str, int]) -> None:
    planned_rounds = _safe_int(rec.get("planned_rounds", 0), 0)
    rt["current_task_name"] = str(task.get("name", "") or "")
    rt["current_task_run_id"] = run_id
    rt["current_task_trigger"] = str(trigger_type or "")
    rt["task_policy_id"] = str(task.get("policy_id", "adaptive-v1") or "adaptive-v1")
    rt["task_regime"] = str(rec.get("regime", "") or "")
    rt["task_recheck_interval"] = _safe_int(rec.get("recheck_interval", 3), 3)
    rt["task_step_no"] = 1
    rt["task_step_planned_rounds"] = planned_rounds
    rt["task_step_executed_rounds"] = 0
    rt["task_step_remaining_rounds"] = planned_rounds
    rt["task_run_pnl"] = 0
    rt["task_run_peak"] = 0
    rt["task_run_max_dd"] = 0
    rt["task_run_total_rounds"] = 0
    rt["task_cycle_last_report_round"] = 0
    rt["task_run_loss_limit"] = _safe_int(limits.get("run_loss_limit", 0), 0)
    rt["task_day_loss_limit"] = _safe_int(limits.get("day_loss_limit", 0), 0)
    if str(rt.get("task_day_loss_date", "")) != _today_text():
        rt["task_day_loss_acc"] = 0
        rt["task_day_loss_date"] = _today_text()
    rt["task_consecutive_losses"] = 0
    rt["task_freeze_reason"] = ""
    rt["task_freeze_until"] = ""


def _clear_current_task_runtime(rt: Dict[str, Any], keep_freeze: bool = False) -> None:
    rt["current_task_name"] = ""
    rt["current_task_run_id"] = ""
    rt["current_task_trigger"] = ""
    rt["task_policy_id"] = ""
    rt["task_regime"] = ""
    rt["task_recheck_interval"] = 0
    rt["task_step_no"] = 0
    rt["task_step_planned_rounds"] = 0
    rt["task_step_executed_rounds"] = 0
    rt["task_step_remaining_rounds"] = 0
    rt["task_run_pnl"] = 0
    rt["task_run_peak"] = 0
    rt["task_run_max_dd"] = 0
    rt["task_run_total_rounds"] = 0
    rt["task_cycle_last_report_round"] = 0
    rt["task_run_loss_limit"] = 0
    rt["task_day_loss_limit"] = 0
    rt["task_consecutive_losses"] = 0
    if not keep_freeze:
        rt["task_freeze_reason"] = ""
        rt["task_freeze_until"] = ""


def _active_run_id(user_ctx: Any) -> str:
    rt = _get_rt(user_ctx)
    return str(rt.get("current_task_run_id", "") or "").strip()


def _active_task_name(user_ctx: Any) -> str:
    rt = _get_rt(user_ctx)
    return str(rt.get("current_task_name", "") or "").strip()


def start_task(user_ctx: Any, task_name: str, trigger_type: str = "manual") -> Dict[str, Any]:
    task = _find_task(user_ctx, task_name)
    if task is None:
        return {"ok": False, "error": f"task `{task_name}` not found"}
    if not bool(task.get("enabled", True)):
        return {"ok": False, "error": f"task `{task_name}` is disabled"}
    if _is_task_paused(task):
        return {"ok": False, "error": f"task `{task_name}` is paused"}

    rt = _get_rt(user_ctx)
    mode_state = _task_mode_state(rt)
    if mode_state == "paused_manual":
        return {"ok": False, "error": "task mode is paused manually"}
    if mode_state == "paused_risk":
        rt["task_freeze_until"] = ""
        rt["task_freeze_reason"] = ""
        _set_task_mode_state(rt, "idle", "")

    if _active_run_id(user_ctx):
        return {"ok": False, "error": f"task `{_active_task_name(user_ctx)}` is running"}

    rec = build_recommendation(user_ctx, task_cfg=task)
    rec_error = str(rec.get("error", "") or "").strip()
    if rec_error:
        return {"ok": False, "error": rec_error, "recommendation": rec}
    preset_name = str(rec.get("recommended_preset", "yc1") or "yc1")
    if preset_name not in PRESET_LADDER:
        preset_name = "yc1"
    applied_preset = _apply_preset(user_ctx, preset_name)
    if not applied_preset:
        return {"ok": False, "error": f"preset `{preset_name}` missing"}
    preset_name = applied_preset
    rec["recommended_preset"] = preset_name

    current_fund = _safe_int(rt.get("gambling_fund", 0), 0)
    limits = compute_loss_limits(current_fund=current_fund, task_cfg=task, preset_name=preset_name)

    run_id = _run_id()
    _set_current_task_runtime(rt, task, run_id, trigger_type, rec, limits)

    task_runtime = _get_task_runtime(task)
    task_runtime["status"] = "running"
    task_runtime["last_trigger_at"] = _now_text()
    task_runtime["last_run_id"] = run_id
    task_runtime["last_reason"] = f"trigger={trigger_type}"
    _set_task_mode_state(rt, "running", "")

    start_task_run(
        user_ctx,
        task_run_id=run_id,
        task_name=str(task.get("name", "") or ""),
        trigger_type=str(trigger_type or "manual"),
        policy_id=str(task.get("policy_id", "adaptive-v1") or "adaptive-v1"),
    )
    append_task_action(
        user_ctx,
        task_run_id=run_id,
        step_no=1,
        regime=str(rec.get("regime", "") or ""),
        preset=str(preset_name),
        planned_rounds=_safe_int(rt.get("task_step_planned_rounds", 0), 0),
        executed_rounds=0,
        action_type="start",
        reason=(
            f"trigger={trigger_type}, conf={_safe_float(rec.get('regime_confidence', 0.0), 0.0):.3f}, "
            f"drift={rec.get('drift_band', 'medium')}"
        ),
        pnl=0,
    )

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {
        "ok": True,
        "task_name": task.get("name", ""),
        "task_run_id": run_id,
        "recommendation": rec,
        "preset": preset_name,
        "limits": limits,
    }


def maybe_trigger_task(user_ctx: Any, force_task_name: str = "") -> Dict[str, Any]:
    if _active_run_id(user_ctx):
        return {"ok": False, "reason": "active_task_running"}

    rt = _get_rt(user_ctx)
    mode_state = _task_mode_state(rt)
    if mode_state == "running":
        _set_task_mode_state(rt, "idle", "")
    if mode_state == "paused_manual" and not force_task_name:
        return {"ok": False, "reason": "paused_manual"}
    if mode_state == "paused_risk" and not force_task_name:
        return {"ok": False, "reason": "paused_risk"}

    tasks = _task_list(user_ctx)
    if not tasks:
        return {"ok": False, "reason": "no_tasks"}

    now = _now()
    if force_task_name:
        return start_task(user_ctx, force_task_name, trigger_type="manual")

    for task in tasks:
        due, trigger_type = _task_is_due(user_ctx, task, now)
        if not due:
            continue
        return start_task(user_ctx, str(task.get("name", "") or ""), trigger_type=trigger_type)

    return {"ok": False, "reason": "no_due_task"}


def get_current_task_context(user_ctx: Any) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    return {
        "task_name": str(rt.get("current_task_name", "") or ""),
        "task_run_id": str(rt.get("current_task_run_id", "") or ""),
        "regime": str(rt.get("task_regime", "") or ""),
        "policy_id": str(rt.get("task_policy_id", "") or ""),
    }


def _end_current_task(user_ctx: Any, status: str, reason: str) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "")
    task_name = str(rt.get("current_task_name", "") or "")
    if not run_id or not task_name:
        return {"ok": False, "reason": "no_active_run"}

    task = _find_task(user_ctx, task_name)
    if task is not None:
        task_runtime = _get_task_runtime(task)
        task_runtime["status"] = status
        task_runtime["last_run_id"] = run_id
        task_runtime["last_run_status"] = status
        task_runtime["last_run_pnl"] = _safe_int(rt.get("task_run_pnl", 0), 0)
        task_runtime["last_run_max_dd"] = _safe_int(rt.get("task_run_max_dd", 0), 0)
        task_runtime["last_reason"] = reason

    finish_task_run(
        user_ctx,
        task_run_id=run_id,
        status=status,
        pnl=_safe_int(rt.get("task_run_pnl", 0), 0),
        max_dd=_safe_int(rt.get("task_run_max_dd", 0), 0),
    )
    append_task_action(
        user_ctx,
        task_run_id=run_id,
        step_no=_safe_int(rt.get("task_step_no", 0), 0),
        regime=str(rt.get("task_regime", "") or ""),
        preset=str(rt.get("current_preset_name", "") or ""),
        planned_rounds=_safe_int(rt.get("task_step_planned_rounds", 0), 0),
        executed_rounds=_safe_int(rt.get("task_step_executed_rounds", 0), 0),
        action_type="finish",
        reason=str(reason or ""),
        pnl=_safe_int(rt.get("task_run_pnl", 0), 0),
    )

    _clear_current_task_runtime(rt, keep_freeze=False)
    _set_task_mode_state(rt, "idle", "")
    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {"ok": True, "status": status, "reason": reason, "task_name": task_name, "task_run_id": run_id}


def _pause_current_task_by_risk(user_ctx: Any, stop_status: str, stop_reason: str) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "")
    task_name = str(rt.get("current_task_name", "") or "")
    if not run_id:
        return {"ok": False, "error": "no_active_run"}

    rt["task_freeze_reason"] = str(stop_reason or "risk_pause")
    rt["task_freeze_until"] = ""
    _set_task_mode_state(rt, "paused_risk", stop_reason)
    _sync_active_task_runtime_status(user_ctx, "paused_risk", stop_reason)

    append_task_action(
        user_ctx,
        task_run_id=run_id,
        step_no=_safe_int(rt.get("task_step_no", 0), 0),
        regime=str(rt.get("task_regime", "") or ""),
        preset=str(rt.get("current_preset_name", "") or ""),
        planned_rounds=_safe_int(rt.get("task_step_planned_rounds", 0), 0),
        executed_rounds=_safe_int(rt.get("task_step_executed_rounds", 0), 0),
        action_type="risk_pause",
        reason=f"{stop_status}: {stop_reason}",
        pnl=_safe_int(rt.get("task_run_pnl", 0), 0),
    )

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {
        "ok": True,
        "status": stop_status,
        "reason": stop_reason,
        "freeze_until": "",
        "task_name": task_name,
        "task_run_id": run_id,
    }


def stop_current_task(user_ctx: Any, reason: str = "manual_stop") -> Dict[str, Any]:
    return _end_current_task(user_ctx, status="stopped", reason=reason)


def reset_task_mode_state(user_ctx: Any, reset_day_loss: bool = True) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    active_run_id = _active_run_id(user_ctx)
    ended = {"ok": False}
    if active_run_id:
        ended = _end_current_task(user_ctx, status="stopped", reason="manual_task_reset")

    _clear_current_task_runtime(rt, keep_freeze=False)
    _set_task_mode_state(rt, "idle", "")
    rt["run_mode"] = "normal"

    if reset_day_loss:
        rt["task_day_loss_acc"] = 0
        rt["task_day_loss_date"] = _today_text()

    reset_task_count = 0
    for task in _task_list(user_ctx):
        task_runtime = _get_task_runtime(task)
        status = str(task_runtime.get("status", "idle") or "idle").strip().lower()
        if status != "idle":
            task_runtime["status"] = "idle"
            reset_task_count += 1

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {
        "ok": True,
        "active_run_stopped": bool(ended.get("ok", False)),
        "reset_task_count": reset_task_count,
    }


def _task_cfg_for_active(user_ctx: Any) -> Optional[Dict[str, Any]]:
    task_name = _active_task_name(user_ctx)
    if not task_name:
        return None
    return _find_task(user_ctx, task_name)


def on_bet_placed(user_ctx: Any) -> None:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "")
    if not run_id:
        return
    executed = _safe_int(rt.get("task_step_executed_rounds", 0), 0)
    remaining = _safe_int(rt.get("task_step_remaining_rounds", 0), 0)
    if remaining > 0:
        rt["task_step_remaining_rounds"] = remaining - 1
    rt["task_step_executed_rounds"] = executed + 1
    rt["task_run_total_rounds"] = _safe_int(rt.get("task_run_total_rounds", 0), 0) + 1


def _recheck_if_needed(user_ctx: Any, task_cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rt = _get_rt(user_ctx)
    recheck_interval = max(1, _safe_int(rt.get("task_recheck_interval", 3), 3))
    executed = _safe_int(rt.get("task_step_executed_rounds", 0), 0)
    if executed <= 0 or executed % recheck_interval != 0:
        return None

    rec = build_recommendation(
        user_ctx,
        task_cfg=task_cfg,
        run_max_dd=_safe_int(rt.get("task_run_max_dd", 0), 0),
        run_loss_limit=max(1, _safe_int(rt.get("task_run_loss_limit", 1), 1)),
    )
    if str(rec.get("error", "") or "").strip():
        return rec
    next_preset = str(rec.get("recommended_preset", rt.get("current_preset_name", "yc1")) or "yc1")
    if next_preset in PRESET_LADDER:
        applied_preset = _apply_preset(user_ctx, next_preset)
        if applied_preset:
            rec["recommended_preset"] = applied_preset

    rt["task_step_no"] = _safe_int(rt.get("task_step_no", 1), 1) + 1
    rt["task_step_planned_rounds"] = _safe_int(rec.get("planned_rounds", rt.get("task_step_planned_rounds", 0)), 0)
    rt["task_step_executed_rounds"] = 0
    rt["task_step_remaining_rounds"] = _safe_int(rt.get("task_step_planned_rounds", 0), 0)
    rt["task_recheck_interval"] = _safe_int(rec.get("recheck_interval", recheck_interval), recheck_interval)
    rt["task_regime"] = str(rec.get("regime", rt.get("task_regime", "")) or "")

    run_id = str(rt.get("current_task_run_id", "") or "")
    if run_id:
        append_task_action(
            user_ctx,
            task_run_id=run_id,
            step_no=_safe_int(rt.get("task_step_no", 1), 1),
            regime=str(rt.get("task_regime", "") or ""),
            preset=str(rt.get("current_preset_name", "") or ""),
            planned_rounds=_safe_int(rt.get("task_step_planned_rounds", 0), 0),
            executed_rounds=0,
            action_type="recheck",
            reason=f"conf={_safe_float(rec.get('regime_confidence', 0.0), 0.0):.3f}, drift={rec.get('drift_band', 'medium')}",
            pnl=_safe_int(rt.get("task_run_pnl", 0), 0),
        )
    return rec


def on_settle(user_ctx: Any, win: bool, profit: int) -> Dict[str, Any]:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "")
    if not run_id:
        return {"active": False}

    task_cfg = _task_cfg_for_active(user_ctx)
    if task_cfg is None:
        ended = _end_current_task(user_ctx, status="stopped", reason="task_missing")
        return {"active": False, "ended": ended}

    if str(rt.get("task_day_loss_date", "")) != _today_text():
        rt["task_day_loss_date"] = _today_text()
        rt["task_day_loss_acc"] = 0

    run_pnl = _safe_int(rt.get("task_run_pnl", 0), 0) + _safe_int(profit, 0)
    rt["task_run_pnl"] = run_pnl
    run_peak = max(_safe_int(rt.get("task_run_peak", 0), 0), run_pnl)
    rt["task_run_peak"] = run_peak
    max_dd = max(_safe_int(rt.get("task_run_max_dd", 0), 0), run_peak - run_pnl)
    rt["task_run_max_dd"] = max_dd

    if profit < 0:
        rt["task_consecutive_losses"] = _safe_int(rt.get("task_consecutive_losses", 0), 0) + 1
        rt["task_day_loss_acc"] = _safe_int(rt.get("task_day_loss_acc", 0), 0) + abs(_safe_int(profit, 0))
    else:
        rt["task_consecutive_losses"] = 0

    run_loss_limit = max(1, _safe_int(rt.get("task_run_loss_limit", 0), 0))
    day_loss_limit = max(run_loss_limit, _safe_int(rt.get("task_day_loss_limit", run_loss_limit), run_loss_limit))
    consecutive_limit = max(1, _safe_int(task_cfg.get("max_consecutive_losses", 4), 4))

    stop_reason = ""
    stop_status = ""
    if run_pnl <= -run_loss_limit:
        stop_reason = f"run loss limit reached ({run_pnl}/{-run_loss_limit})"
        stop_status = "loss_stop"
    elif _safe_int(rt.get("task_day_loss_acc", 0), 0) >= day_loss_limit:
        stop_reason = f"daily loss limit reached ({rt.get('task_day_loss_acc', 0)}/{day_loss_limit})"
        stop_status = "daily_stop"
    elif _safe_int(rt.get("task_consecutive_losses", 0), 0) >= consecutive_limit:
        stop_reason = f"consecutive losses reached ({rt.get('task_consecutive_losses', 0)}/{consecutive_limit})"
        stop_status = "consecutive_stop"

    step_remaining = _safe_int(rt.get("task_step_remaining_rounds", 0), 0)
    light_risk_enabled = bool(rt.get("risk_light_enabled", True))
    if stop_status and light_risk_enabled:
        paused = _pause_current_task_by_risk(user_ctx, stop_status=stop_status, stop_reason=stop_reason)
        return {
            "active": bool(paused.get("ok", False)),
            "risk_paused": bool(paused.get("ok", False)),
            "paused": paused,
            "reason": stop_reason,
            "status": stop_status,
        }

    rechecked = _recheck_if_needed(user_ctx, task_cfg)

    if step_remaining <= 0:
        ended = _end_current_task(user_ctx, status="completed", reason="planned rounds finished")
        return {"active": False, "ended": ended, "rechecked": rechecked}

    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    return {"active": True, "rechecked": rechecked}


def format_current_task_brief(user_ctx: Any) -> str:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "")
    if not run_id:
        return "当前无运行中任务"
    return (
        f"任务 `{rt.get('current_task_name', '-')}` 运行中\n"
        f"- run_id: {run_id}\n"
        f"- 触发：{_trigger_type_text(rt.get('current_task_trigger', ''))} | 盘面：{regime_text(rt.get('task_regime', '-'))}\n"
        f"- 进度：第 {rt.get('task_step_no', 0)} 段，剩余 {rt.get('task_step_remaining_rounds', 0)} 局\n"
        f"- 预设：{rt.get('current_preset_name', '-')} | 收益：{rt.get('task_run_pnl', 0)} | 回撤：{rt.get('task_run_max_dd', 0)}"
    )


def format_task_dashboard_hint(user_ctx: Any) -> str:
    """任务状态栏简版（独立消息）。"""
    rt = _get_rt(user_ctx)
    run_mode = str(rt.get("run_mode", "normal") or "normal").strip().lower()
    run_id = str(rt.get("current_task_run_id", "") or "").strip()
    if not run_id and run_mode != "task":
        return ""

    mode_state = _task_mode_state(rt)
    task_name = str(rt.get("current_task_name", "") or "-")
    executed = _safe_int(rt.get("task_step_executed_rounds", 0), 0)
    planned = _safe_int(rt.get("task_step_planned_rounds", 0), 0)
    remaining = _safe_int(rt.get("task_step_remaining_rounds", 0), 0)
    run_pnl = _safe_int(rt.get("task_run_pnl", 0), 0)
    max_dd = _safe_int(rt.get("task_run_max_dd", 0), 0)
    preset_name = str(rt.get("current_preset_name", "") or "-")
    regime = regime_text(rt.get("task_regime", "-"))
    lines = [
        "🧩 任务状态栏🧩",
        "",
        f"状态：{_task_mode_state_text(mode_state)}",
        f"任务：{task_name}",
        f"盘面：{regime}",
        f"预设：{preset_name}",
        f"进度：{executed}/{planned}（剩{remaining}）",
        f"累计：{run_pnl}",
        f"回撤：{max_dd}",
    ]
    return "\n".join(lines)


def format_task_runtime_panel(user_ctx: Any) -> str:
    rt = _get_rt(user_ctx)
    mode_state = _task_mode_state(rt)
    tasks = _task_list(user_ctx)
    enabled_count = sum(1 for t in tasks if bool(t.get("enabled", True)))
    auto_enabled = bool(rt.get("task_auto_enabled", True))
    run_id = str(rt.get("current_task_run_id", "") or "")
    task_name = str(rt.get("current_task_name", "") or "")
    pause_reason = str(rt.get("task_mode_pause_reason", "") or "").strip()

    if not run_id:
        title = "🧩 **任务状态栏**"
        lines = [
            title,
            f"- 模式：{_task_mode_state_text(mode_state)} | 自动触发：{_switch_text(auto_enabled)}",
            f"- 已配置任务：{len(tasks)}（启用 {enabled_count}）",
        ]
        if pause_reason:
            lines.append(f"- 暂停原因：{pause_reason}")
        if tasks:
            lines.append("- 操作：`task run <name>` / `task list` / `task show <name>`")
        else:
            lines.append("- 创建：`task new <name> [cron_minutes] [time|regime|hybrid]`")
        return "\n".join(lines)

    active_cfg = _find_task(user_ctx, task_name)
    trigger = active_cfg.get("trigger", {}) if isinstance(active_cfg, dict) else {}
    task_runtime = _get_task_runtime(active_cfg) if isinstance(active_cfg, dict) else {}
    mode = str(trigger.get("mode", "hybrid") or "hybrid")
    cron_minutes = max(0, _safe_int(trigger.get("cron_minutes", 0), 0))
    executed = _safe_int(rt.get("task_step_executed_rounds", 0), 0)
    planned = _safe_int(rt.get("task_step_planned_rounds", 0), 0)
    remaining = _safe_int(rt.get("task_step_remaining_rounds", 0), 0)
    title = "🧩 **任务状态栏**"

    lines = [
        title,
        f"- 模式：{_task_mode_state_text(mode_state)} | 任务：`{task_name}`（{run_id[:12]}{'...' if len(run_id) > 12 else ''}）",
        f"- 触发：{_trigger_type_text(rt.get('current_task_trigger', ''))} | 触发模式：{_mode_text(mode)} | 自动触发：{_switch_text(auto_enabled)}",
        f"- 盘面：{regime_text(rt.get('task_regime', '-'))} | 预设：{str(rt.get('current_preset_name', '') or '-')}",
        f"- 进度：第 {_safe_int(rt.get('task_step_no', 0), 0)} 段 {executed}/{planned}（剩 {remaining}） | 累计局数：{_safe_int(rt.get('task_run_total_rounds', 0), 0)}",
        f"- 累计：收益 {_safe_int(rt.get('task_run_pnl', 0), 0)} | 最大回撤 {_safe_int(rt.get('task_run_max_dd', 0), 0)}",
        "- 操作：`task pause` / `task resume` / `task stop` / `task show <name>`",
    ]
    if pause_reason:
        lines.append(f"- 暂停原因：{pause_reason}")
    lines.append("- 提示：详细策略与历史统计请用 `task show <name>` / `task logs <name>`。")
    return "\n".join(lines)


def format_task_cycle_report(user_ctx: Any) -> str:
    rt = _get_rt(user_ctx)
    run_id = str(rt.get("current_task_run_id", "") or "").strip()
    if not run_id:
        return ""
    task_name = str(rt.get("current_task_name", "") or "-")
    short_run_id = f"{run_id[:12]}..." if len(run_id) > 12 else run_id
    return (
        "🧩 任务运行简报（每40局）\n"
        f"- 任务：`{task_name}`（{short_run_id}） | 状态：{_task_mode_state_text(_task_mode_state(rt))}\n"
        f"- 盘面：{regime_text(rt.get('task_regime', '-'))} | 预设：{rt.get('current_preset_name', '-')}\n"
        f"- 累计局数：{_safe_int(rt.get('task_run_total_rounds', 0), 0)} | 当前段：{_safe_int(rt.get('task_step_executed_rounds', 0), 0)}/{_safe_int(rt.get('task_step_planned_rounds', 0), 0)}\n"
        f"- 累计收益：{_safe_int(rt.get('task_run_pnl', 0), 0)} | 最大回撤：{_safe_int(rt.get('task_run_max_dd', 0), 0)}\n"
        "- 详情：`task show <name>` / `task panel`"
    )


def task_logs(user_ctx: Any, task_name: str = "", limit: int = 20) -> str:
    db_path = os.path.join(str(getattr(user_ctx, "user_dir", "") or ""), "analytics.db")
    if not os.path.exists(db_path):
        return "暂无任务日志（analytics.db 不存在）"

    limit = max(1, min(200, _safe_int(limit, 20)))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if task_name:
            run_rows = conn.execute(
                """
                SELECT task_run_id, task_name, trigger_type, start_at, end_at, status, pnl, max_dd
                FROM task_runs
                WHERE task_name = ?
                ORDER BY start_at DESC
                LIMIT ?
                """,
                (str(task_name), limit),
            ).fetchall()
        else:
            run_rows = conn.execute(
                """
                SELECT task_run_id, task_name, trigger_type, start_at, end_at, status, pnl, max_dd
                FROM task_runs
                ORDER BY start_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        if not run_rows:
            return "暂无任务运行记录"

        lines = [f"任务运行日志（最近 {limit} 条）："]
        for row in run_rows:
            status_text = _status_text(row["status"])
            lines.append(
                f"- `{row['task_name']}` | run_id={row['task_run_id']} | 状态={status_text} | "
                f"收益={_safe_int(row['pnl'], 0)} | 回撤={_safe_int(row['max_dd'], 0)} | "
                f"{_fmt_time_text(row['start_at'])} -> {_fmt_time_text(row['end_at']) if row['end_at'] else '-'}"
            )
        return "\n".join(lines)
    finally:
        conn.close()


def task_stats(user_ctx: Any, task_name: str = "") -> str:
    coverage = linkage_coverage_report(user_ctx)
    refresh_regime_preset_stats(user_ctx)

    db_path = os.path.join(str(getattr(user_ctx, "user_dir", "") or ""), "analytics.db")
    if not os.path.exists(db_path):
        return "暂无统计（analytics.db 不存在）"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        params: Tuple[Any, ...]
        if task_name:
            sql = """
                SELECT COUNT(*) AS run_count,
                       SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_count,
                       SUM(CASE WHEN status IN ('loss_stop','daily_stop','consecutive_stop') THEN 1 ELSE 0 END) AS risk_stop_count,
                       SUM(pnl) AS total_pnl,
                       MAX(max_dd) AS max_dd
                FROM task_runs
                WHERE task_name = ?
            """
            params = (str(task_name),)
        else:
            sql = """
                SELECT COUNT(*) AS run_count,
                       SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_count,
                       SUM(CASE WHEN status IN ('loss_stop','daily_stop','consecutive_stop') THEN 1 ELSE 0 END) AS risk_stop_count,
                       SUM(pnl) AS total_pnl,
                       MAX(max_dd) AS max_dd
                FROM task_runs
            """
            params = ()

        row = conn.execute(sql, params).fetchone()
        run_count = _safe_int(row["run_count"] if row else 0, 0)
        completed_count = _safe_int(row["completed_count"] if row else 0, 0)
        risk_stop_count = _safe_int(row["risk_stop_count"] if row else 0, 0)
        total_pnl = _safe_int(row["total_pnl"] if row else 0, 0)
        max_dd = _safe_int(row["max_dd"] if row else 0, 0)

        lines = [
            "任务统计：",
            f"- 运行次数：{run_count}",
            f"- 正常完成：{completed_count}",
            f"- 风控结束：{risk_stop_count}",
            f"- 累计收益：{total_pnl}",
            f"- 最大回撤：{max_dd}",
            f"- 链路覆盖率：{coverage.get('coverage_pct', 0.0)}% ({coverage.get('linked', 0)}/{coverage.get('total_settled', 0)})",
        ]
        return "\n".join(lines)
    finally:
        conn.close()
