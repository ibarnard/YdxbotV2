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
    return status == "paused"


def _persist_tasks(user_ctx: Any) -> None:
    if hasattr(user_ctx, "save_tasks"):
        user_ctx.save_tasks()


def _normalize_new_task(name: str, cron_minutes: int = 0, mode: str = "hybrid") -> Dict[str, Any]:
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
        "task_loss_pct": 0.006,
        "daily_loss_pct": 0.02,
        "max_consecutive_losses": 4,
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


def create_task(user_ctx: Any, name: str, cron_minutes: int = 0, mode: str = "hybrid") -> Dict[str, Any]:
    name = str(name or "").strip()
    if not name:
        return {"ok": False, "error": "task name is required"}
    if _find_task(user_ctx, name) is not None:
        return {"ok": False, "error": f"task `{name}` already exists"}

    new_task = _normalize_new_task(name, cron_minutes=cron_minutes, mode=mode)
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


def list_tasks(user_ctx: Any) -> List[Dict[str, Any]]:
    return _task_list(user_ctx)


def format_task_list(user_ctx: Any) -> str:
    tasks = _task_list(user_ctx)
    if not tasks:
        return "暂无任务。可用 `task new <name> [cron_minutes] [time|regime|hybrid]` 创建。"

    lines = ["任务列表："]
    for item in tasks:
        runtime = _get_task_runtime(item)
        status = str(runtime.get("status", "idle") or "idle")
        enabled = bool(item.get("enabled", True))
        trigger = item.get("trigger", {}) if isinstance(item.get("trigger"), dict) else {}
        mode = str(trigger.get("mode", "hybrid") or "hybrid")
        cron_minutes = _safe_int(trigger.get("cron_minutes", 0), 0)
        min_interval = _safe_int(trigger.get("min_interval_minutes", 10), 10)
        last = str(runtime.get("last_trigger_at", "") or "-")
        lines.append(
            f"- {item.get('name', '-')}: {'ON' if enabled else 'OFF'} | {status} | mode={mode} | cron={cron_minutes}m | min={min_interval}m | last={last}"
        )
    return "\n".join(lines)


def format_task_detail(user_ctx: Any, name: str) -> str:
    task = _find_task(user_ctx, name)
    if task is None:
        return f"任务 `{name}` 不存在"

    runtime = _get_task_runtime(task)
    trigger = task.get("trigger", {}) if isinstance(task.get("trigger"), dict) else {}
    lines = [
        f"任务 `{name}`",
        f"- enabled: {bool(task.get('enabled', True))}",
        f"- status: {runtime.get('status', 'idle')}",
        f"- trigger.mode: {trigger.get('mode', 'hybrid')}",
        f"- trigger.cron_minutes: {trigger.get('cron_minutes', 0)}",
        f"- trigger.min_interval_minutes: {trigger.get('min_interval_minutes', 10)}",
        f"- candidate_presets: {', '.join(task.get('candidate_presets', []))}",
        f"- policy_id: {task.get('policy_id', 'adaptive-v1')}",
        f"- last_trigger_at: {runtime.get('last_trigger_at', '-')}",
        f"- last_run_id: {runtime.get('last_run_id', '-')}",
        f"- last_run_status: {runtime.get('last_run_status', '-')}",
        f"- last_run_pnl: {runtime.get('last_run_pnl', 0)}",
        f"- last_run_max_dd: {runtime.get('last_run_max_dd', 0)}",
    ]
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


def _apply_preset(user_ctx: Any, preset_name: str) -> bool:
    presets = getattr(user_ctx, "presets", {})
    if not isinstance(presets, dict):
        return False
    preset = presets.get(preset_name)
    if not isinstance(preset, list) or len(preset) < 7:
        return False

    rt = _get_rt(user_ctx)
    rt["continuous"] = int(preset[0])
    rt["lose_stop"] = int(preset[1])
    rt["lose_once"] = float(preset[2])
    rt["lose_twice"] = float(preset[3])
    rt["lose_three"] = float(preset[4])
    rt["lose_four"] = float(preset[5])
    rt["initial_amount"] = int(preset[6])
    rt["bet_amount"] = int(preset[6])
    rt["current_preset_name"] = preset_name
    rt["switch"] = True
    rt["manual_pause"] = False
    rt["bet_on"] = True
    rt["mode_stop"] = True
    rt["bet"] = False
    return True


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
    if _active_run_id(user_ctx):
        return {"ok": False, "error": f"task `{_active_task_name(user_ctx)}` is running"}

    rec = build_recommendation(user_ctx, task_cfg=task)
    preset_name = str(rec.get("recommended_preset", "yc1") or "yc1")
    if preset_name not in PRESET_LADDER:
        preset_name = "yc1"
    if not _apply_preset(user_ctx, preset_name):
        return {"ok": False, "error": f"preset `{preset_name}` missing"}

    current_fund = _safe_int(rt.get("gambling_fund", 0), 0)
    limits = compute_loss_limits(current_fund=current_fund, task_cfg=task, preset_name=preset_name)

    run_id = _run_id()
    _set_current_task_runtime(rt, task, run_id, trigger_type, rec, limits)

    task_runtime = _get_task_runtime(task)
    task_runtime["status"] = "running"
    task_runtime["last_trigger_at"] = _now_text()
    task_runtime["last_run_id"] = run_id
    task_runtime["last_reason"] = f"trigger={trigger_type}"

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
    freeze_until = _parse_time(rt.get("task_freeze_until", ""))
    if freeze_until and _now() < freeze_until and not force_task_name:
        return {"ok": False, "reason": "frozen", "until": freeze_until.strftime(TIME_FMT)}
    if freeze_until and _now() >= freeze_until:
        rt["task_freeze_until"] = ""
        rt["task_freeze_reason"] = ""

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

    keep_freeze = status in {"risk_stop", "daily_stop", "loss_stop", "consecutive_stop"}
    _clear_current_task_runtime(rt, keep_freeze=keep_freeze)
    if hasattr(user_ctx, "save_state"):
        user_ctx.save_state()
    _persist_tasks(user_ctx)
    return {"ok": True, "status": status, "reason": reason, "task_name": task_name, "task_run_id": run_id}


def stop_current_task(user_ctx: Any, reason: str = "manual_stop") -> Dict[str, Any]:
    return _end_current_task(user_ctx, status="stopped", reason=reason)


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
    next_preset = str(rec.get("recommended_preset", rt.get("current_preset_name", "yc1")) or "yc1")
    if next_preset in PRESET_LADDER:
        _apply_preset(user_ctx, next_preset)

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
    if stop_status:
        rt["task_freeze_reason"] = stop_reason
        rt["task_freeze_until"] = (_now() + timedelta(minutes=30)).strftime(TIME_FMT)
        ended = _end_current_task(user_ctx, status=stop_status, reason=stop_reason)
        return {"active": False, "ended": ended, "reason": stop_reason}

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
        f"- step: {rt.get('task_step_no', 0)} | remain={rt.get('task_step_remaining_rounds', 0)}\n"
        f"- preset: {rt.get('current_preset_name', '-')} | regime: {rt.get('task_regime', '-')}\n"
        f"- pnl: {rt.get('task_run_pnl', 0)} | max_dd: {rt.get('task_run_max_dd', 0)}"
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

        lines = ["任务运行日志："]
        for row in run_rows:
            lines.append(
                f"- {row['task_name']} | {row['task_run_id']} | {row['status']} | pnl={row['pnl']} | dd={row['max_dd']} | {row['start_at']} -> {row['end_at'] or '-'}"
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
            f"- run_count: {run_count}",
            f"- completed_count: {completed_count}",
            f"- risk_stop_count: {risk_stop_count}",
            f"- total_pnl: {total_pnl}",
            f"- max_dd: {max_dd}",
            f"- linkage_coverage: {coverage.get('coverage_pct', 0.0)}% ({coverage.get('linked', 0)}/{coverage.get('total_settled', 0)})",
        ]
        return "\n".join(lines)
    finally:
        conn.close()
