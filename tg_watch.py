from __future__ import annotations

from typing import Any, Dict, List, Tuple

import history_analysis
import multi_account_orchestrator
import policy_engine
import risk_control
import self_learning_engine
from user_manager import get_registered_user_contexts


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
