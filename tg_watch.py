from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

import history_analysis
import multi_account_orchestrator
import policy_engine
import risk_control
import runtime_stability
import self_learning_engine
from user_manager import get_registered_user_contexts

WATCH_EVENT_STATE_KEY = "watch_event_state"
WATCH_ALERTS_KEY = "watch_alerts"
WATCH_ALERT_LIMIT = 30


def _all_users(current_user_ctx) -> Dict[int, Any]:
    users = get_registered_user_contexts()
    if not users and current_user_ctx is not None:
        users[int(current_user_ctx.user_id)] = current_user_ctx
    return dict(sorted(users.items(), key=lambda item: item[0]))


def _status_text(rt: Dict[str, Any]) -> str:
    if bool(rt.get("manual_pause", False)):
        return "手动暂停"
    if not bool(rt.get("switch", True)):
        return "已关闭"
    if bool(rt.get("bet_on", False)):
        return "运行中"
    return "已暂停"


def _mode_text(rt: Dict[str, Any]) -> str:
    try:
        mode_code = int(rt.get("bet_mode", rt.get("mode", 1)) or 1)
    except (TypeError, ValueError):
        mode_code = 1
    return {0: "反投", 1: "预测", 2: "追投"}.get(mode_code, "未知")


def _risk_bits(rt: Dict[str, Any]) -> str:
    modes = risk_control.normalize_fk_switches(rt, apply_default=False)
    return f"{int(modes['fk1_enabled'])}{int(modes['fk2_enabled'])}{int(modes['fk3_enabled'])}"


def _temperature_text(level: str) -> str:
    mapping = {
        "very_cold": "很冷",
        "cold": "偏冷",
        "normal": "正常",
        "hot": "偏热",
        "very_hot": "很热",
    }
    return mapping.get(str(level or "").strip().lower(), str(level or "-") or "-")


def _decision_brief(rt: Dict[str, Any]) -> str:
    tag = str(rt.get("last_predict_tag", "") or "").strip()
    confidence = int(rt.get("last_predict_confidence", 0) or 0)
    source = str(rt.get("last_predict_source", "") or "").strip()
    if not tag and not source and confidence <= 0:
        return "-"
    parts = [part for part in (tag or "-", f"{confidence}%", source or "-") if part]
    return " / ".join(parts)


def _task_brief(rt: Dict[str, Any]) -> str:
    return multi_account_orchestrator._task_brief(rt)  # type: ignore[attr-defined]


def _policy_brief(user_ctx) -> str:
    return multi_account_orchestrator._policy_brief(user_ctx)  # type: ignore[attr-defined]


def _learning_brief(user_ctx) -> str:
    return multi_account_orchestrator._learning_brief(user_ctx)  # type: ignore[attr-defined]


def _build_watch_evidence(user_ctx) -> Dict[str, Any]:
    snapshot = history_analysis.build_current_analysis_snapshot(user_ctx)
    return history_analysis.build_policy_evidence_package(user_ctx, analysis_snapshot=snapshot)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_ts() -> int:
    return int(datetime.now().timestamp())


def record_watch_event(
    user_ctx,
    event_type: str,
    message: str,
    *,
    severity: str = "info",
    fingerprint: str = "",
    throttle_sec: int = 300,
    meta: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rt = user_ctx.state.runtime
    event_key = str(event_type or "").strip() or "generic"
    event_message = str(message or "").strip()
    normalized_fingerprint = str(fingerprint or "").strip()
    state = rt.get(WATCH_EVENT_STATE_KEY, {})
    if not isinstance(state, dict):
        state = {}

    current = state.get(event_key, {})
    if not isinstance(current, dict):
        current = {}

    now_text = _now_text()
    now_ts = _now_ts()
    throttle = max(0, int(throttle_sec or 0))
    last_sent_ts = int(current.get("last_sent_ts", 0) or 0)
    last_fingerprint = str(current.get("fingerprint", "") or "")
    should_notify = (
        normalized_fingerprint != last_fingerprint
        or throttle == 0
        or (now_ts - last_sent_ts) >= throttle
    )

    state[event_key] = {
        "fingerprint": normalized_fingerprint,
        "severity": severity,
        "last_message": event_message,
        "last_seen_at": now_text,
        "last_seen_ts": now_ts,
        "last_sent_at": now_text if should_notify else str(current.get("last_sent_at", "") or ""),
        "last_sent_ts": now_ts if should_notify else last_sent_ts,
    }
    rt[WATCH_EVENT_STATE_KEY] = state

    alerts = rt.get(WATCH_ALERTS_KEY, [])
    if not isinstance(alerts, list):
        alerts = []

    entry = None
    for item in reversed(alerts):
        if not isinstance(item, dict):
            continue
        if str(item.get("event_type", "") or "") == event_key and str(item.get("fingerprint", "") or "") == normalized_fingerprint:
            entry = item
            break

    if entry is None:
        entry = {
            "event_type": event_key,
            "severity": severity,
            "message": event_message,
            "fingerprint": normalized_fingerprint,
            "first_seen_at": now_text,
            "last_seen_at": now_text,
            "last_notified_at": now_text if should_notify else "",
            "count": 1,
            "meta": dict(meta or {}),
        }
        alerts.append(entry)
    else:
        entry["severity"] = severity
        entry["message"] = event_message
        entry["last_seen_at"] = now_text
        entry["count"] = int(entry.get("count", 0) or 0) + 1
        if meta:
            merged_meta = entry.get("meta", {})
            if not isinstance(merged_meta, dict):
                merged_meta = {}
            merged_meta.update(dict(meta))
            entry["meta"] = merged_meta
        if should_notify:
            entry["last_notified_at"] = now_text

    rt[WATCH_ALERTS_KEY] = alerts[-WATCH_ALERT_LIMIT:]
    return {"should_notify": should_notify, "event": entry}


def list_watch_alerts(user_ctx, limit: int = 10) -> List[Dict[str, Any]]:
    alerts = user_ctx.state.runtime.get(WATCH_ALERTS_KEY, [])
    if not isinstance(alerts, list):
        return []
    rows = [item for item in alerts if isinstance(item, dict)]
    return rows[-max(1, int(limit or 1)) :]


def build_watch_overview_text(user_ctx) -> str:
    rt = user_ctx.state.runtime
    evidence = _build_watch_evidence(user_ctx)
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    lines = [
        "👀 值守摘要",
        "",
        f"状态：{_status_text(rt)} | 模式 {_mode_text(rt)} | fk {_risk_bits(rt)}",
        f"预设：{str(rt.get('current_preset_name', '') or '未设')} | 任务 {_task_brief(rt)} | 策略 {_policy_brief(user_ctx)}",
        f"学习：{_learning_brief(user_ctx)} | 当前建议 {str(rt.get('current_fk1_action_text', '') or '未评估')}",
        f"资金：{int(rt.get('gambling_fund', 0) or 0):,} | 余额：{int(rt.get('account_balance', 0) or 0):,} | 总收益：{int(rt.get('earnings', 0) or 0):+,}",
        (
            f"24h：胜率 {float(overview.get('win_rate', 0.0) or 0.0) * 100:.1f}% | "
            f"盈亏 {int(overview.get('pnl_total', 0) or 0):+,} | "
            f"回撤 {int(overview.get('max_drawdown', 0) or 0):,} | "
            f"样本 {int(overview.get('settled_count', 0) or 0)}"
        ),
        (
            f"盘面：{str(evidence.get('current_regime', '-') or '-')} | "
            f"温度 {_temperature_text(str(evidence.get('recent_temperature', {}).get('level', 'normal') or 'normal'))} | "
            f"相似 {int(similar.get('similar_count', 0) or 0)} | "
            f"历史建议 {str(similar.get('recommended_tier_cap', '') or '-')}"
        ),
        f"最近决策：{_decision_brief(rt)}",
        "",
        "命令：`watch fleet` / `watch learn`",
    ]
    return "\n".join(lines)


def _fleet_priority(user_ctx, evidence: Dict[str, Any]) -> Tuple[int, int]:
    rt = user_ctx.state.runtime
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    temp = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    score = 0
    if _status_text(rt) != "运行中":
        score += 2
    if int(overview.get("pnl_total", 0) or 0) < 0:
        score += 1
    if str(temp.get("level", "normal") or "normal") in {"cold", "very_cold"}:
        score += 1
    return (-score, int(getattr(user_ctx, "user_id", 0) or 0))


def build_watch_fleet_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "👀 值守多账号\n\n暂无已加载账号"

    rows: List[Tuple[Tuple[int, int], str]] = []
    for user_ctx in users.values():
        rt = user_ctx.state.runtime
        evidence = _build_watch_evidence(user_ctx)
        overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
        temp = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
        rows.append(
            (
                _fleet_priority(user_ctx, evidence),
                (
                    f"- {multi_account_orchestrator._account_name(user_ctx)} ({user_ctx.user_id}) | "  # type: ignore[attr-defined]
                    f"{_status_text(rt)} | 任务 {_task_brief(rt)} | 策略 {_policy_brief(user_ctx)} | 学习 {_learning_brief(user_ctx)} | "
                    f"24h {int(overview.get('pnl_total', 0) or 0):+,}/{int(overview.get('max_drawdown', 0) or 0):,} | "
                    f"温度 {_temperature_text(str(temp.get('level', 'normal') or 'normal'))}"
                ),
            )
        )

    rows.sort(key=lambda item: item[0])
    lines = ["👀 值守多账号", ""]
    lines.extend(line for _, line in rows)
    return "\n".join(lines)


def build_watch_learn_text(user_ctx) -> str:
    center = self_learning_engine.load_learning_center(user_ctx)
    candidates = self_learning_engine._sorted_candidates(center)  # type: ignore[attr-defined]
    latest = candidates[-1] if candidates else {}
    active_shadow = self_learning_engine._find_candidate_strict(  # type: ignore[attr-defined]
        center,
        str(center.get("active_shadow_candidate_id", "") or ""),
    )
    active_gray = self_learning_engine._find_candidate_strict(  # type: ignore[attr-defined]
        center,
        str(center.get("active_gray_candidate_id", "") or ""),
    )
    promoted = self_learning_engine._find_candidate_strict(  # type: ignore[attr-defined]
        center,
        str(center.get("promoted_candidate_id", "") or ""),
    )
    active_policy = policy_engine.build_policy_prompt_context(user_ctx)
    lines = [
        "🧠 值守学习摘要",
        "",
        f"当前策略：{active_policy.get('policy_id', '')}@{active_policy.get('policy_version', '')} ({active_policy.get('policy_mode', '')})",
        f"候选：{len(candidates)} | 最新 {latest.get('candidate_version', '-') or '-'} | 规则 {latest.get('rule_name', '-') or '-'}",
        f"最近评估：{latest.get('last_evaluation_status', '-') or '-'} / {latest.get('last_score_total', '-')}",
    ]

    if active_shadow:
        metrics = self_learning_engine._shadow_metrics(user_ctx, str(active_shadow.get("candidate_id", "") or ""))  # type: ignore[attr-defined]
        lines.append(
            f"影子：{active_shadow.get('candidate_version', '-') or '-'} | "
            f"样本 {int(metrics.get('sample_size', 0) or 0)} | "
            f"Δ {int(metrics.get('delta_pnl', 0) or 0):+,} | "
            f"回撤改善 {int(metrics.get('delta_drawdown', 0) or 0):+,} | "
            f"{self_learning_engine._shadow_status_text(str(metrics.get('status', self_learning_engine.LEARNING_SHADOW_WATCH) or self_learning_engine.LEARNING_SHADOW_WATCH))}"  # type: ignore[attr-defined]
        )
    else:
        lines.append("影子：无")

    if active_gray:
        lines.append(
            f"灰度：{active_gray.get('candidate_version', '-') or '-'} | 策略 {active_gray.get('gray_policy_version', '-') or '-'}"
        )
    else:
        lines.append("灰度：无")

    lines.append(
        f"最近转正：{promoted.get('candidate_version', '-') if promoted else '-'} | 最近事件 {center.get('last_promotion_event_at', '') or '-'}"
    )
    lines.append("操作：`learn shadow` / `learn gray` / `learn promote` / `learn rollback`")
    return "\n".join(lines)


def _event_label(event_type: str) -> str:
    mapping = {
        "fund_pause": "资金不足暂停",
        "fund_resume": "资金恢复",
        "fund_resume_sync": "资金同步恢复",
        "model_timeout": "模型预测超时",
        "risk_pause_fk2": "入场风控暂停",
        "risk_pause_fk3": "连输风控暂停",
        "risk_pause_shadow_probe": "影子验证未达标",
        "risk_resume": "自动暂停恢复",
        "risk_resume_shadow_probe": "影子验证恢复",
        "task_package_switch": "任务包切换",
        "task_takeover": "任务接管",
        "learn_shadow_on": "学习影子开启",
        "learn_shadow_off": "学习影子关闭",
        "learn_gray": "学习灰度启动",
        "learn_promote": "学习候选转正",
        "learn_rollback": "学习候选回滚",
    }
    return mapping.get(str(event_type or "").strip(), str(event_type or "未知事件") or "未知事件")


def _current_watch_alerts(user_ctx) -> List[Tuple[int, str]]:
    rt = user_ctx.state.runtime
    name = multi_account_orchestrator._account_name(user_ctx)  # type: ignore[attr-defined]
    alerts: List[Tuple[int, str]] = []

    recent_fault = runtime_stability.get_recent_runtime_fault(user_ctx, max_age_sec=12 * 3600)
    if recent_fault and str(recent_fault.get("severity", "info") or "info") in {"warning", "error"}:
        alerts.append(
            (
                0,
                (
                    f"- {name} ({user_ctx.user_id}) | 运行异常 | "
                    f"{str(recent_fault.get('stage', '') or 'runtime')} | "
                    f"{runtime_stability.format_runtime_fault_brief(recent_fault)}"
                ),
            )
        )

    if bool(rt.get("fund_pause_notified", False)):
        alerts.append(
            (
                1,
                f"- {name} ({user_ctx.user_id}) | 资金不足暂停 | 资金 {int(rt.get('gambling_fund', 0) or 0):,} | 余额 {int(rt.get('account_balance', 0) or 0):,}",
            )
        )

    if int(rt.get("stop_count", 0) or 0) > 0 and not bool(rt.get("manual_pause", False)):
        remaining = max(int(rt.get("stop_count", 0) or 0) - 1, 0)
        reason = str(rt.get("pause_countdown_reason", "") or rt.get("pause_resume_pending_reason", "") or "自动暂停")
        alerts.append(
            (
                2,
                f"- {name} ({user_ctx.user_id}) | 自动暂停中 | 原因 {reason} | 剩余 {remaining} 局",
            )
        )

    center = self_learning_engine.load_learning_center(user_ctx)
    active_gray = self_learning_engine._find_candidate_strict(  # type: ignore[attr-defined]
        center,
        str(center.get("active_gray_candidate_id", "") or ""),
    )
    if active_gray:
        alerts.append(
            (
                3,
                f"- {name} ({user_ctx.user_id}) | 学习灰度中 | {active_gray.get('candidate_version', '-') or '-'} -> {active_gray.get('gray_policy_version', '-') or '-'}",
            )
        )

    evidence = _build_watch_evidence(user_ctx)
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    temp = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    temp_level = str(temp.get("level", "normal") or "normal")
    pnl24 = int(overview.get("pnl_total", 0) or 0)
    if temp_level in {"cold", "very_cold"} and pnl24 < 0:
        alerts.append(
            (
                4,
                f"- {name} ({user_ctx.user_id}) | 24h 偏冷 | 盈亏 {pnl24:+,} | 回撤 {int(overview.get('max_drawdown', 0) or 0):,} | 温度 {_temperature_text(temp_level)}",
            )
        )

    return alerts


def build_watch_alerts_text(current_user_ctx) -> str:
    users = _all_users(current_user_ctx)
    if not users:
        return "🚨 值守告警\n\n暂无已加载账号"

    current_alerts: List[Tuple[int, str]] = []
    recent_rows: List[Tuple[str, str]] = []
    for user_ctx in users.values():
        current_alerts.extend(_current_watch_alerts(user_ctx))
        name = multi_account_orchestrator._account_name(user_ctx)  # type: ignore[attr-defined]
        for item in list_watch_alerts(user_ctx, limit=10):
            severity = str(item.get("severity", "info") or "info")
            if severity not in {"warning", "error"}:
                continue
            when = str(item.get("last_notified_at", "") or item.get("last_seen_at", "") or "-")
            count = int(item.get("count", 0) or 0)
            count_text = f" x{count}" if count > 1 else ""
            recent_rows.append(
                (
                    when,
                    f"- {when} | {name} ({user_ctx.user_id}) | {_event_label(str(item.get('event_type', '') or ''))}{count_text} | {str(item.get('message', '') or '').splitlines()[0]}",
                )
            )

    current_alerts.sort(key=lambda item: item[0])
    recent_rows.sort(key=lambda item: item[0], reverse=True)

    lines = ["🚨 值守告警", ""]
    if current_alerts:
        lines.append("当前风险：")
        lines.extend(line for _, line in current_alerts[:8])
    else:
        lines.append("当前风险：")
        lines.append("- 暂无需要立即处理的事项")

    lines.append("")
    lines.append("最近播报：")
    if recent_rows:
        lines.extend(line for _, line in recent_rows[:8])
    else:
        lines.append("- 暂无近期告警播报")
    return "\n".join(lines)
