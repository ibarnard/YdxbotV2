from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
import uuid
from typing import Any, Dict, List, Optional

import history_analysis
import policy_engine
import risk_control
from user_manager import get_registered_user_contexts


LEARNING_CENTER_VERSION = 1
LEARNING_STATUS_GENERATED = "generated"
LEARNING_STATUS_EVALUATED = "evaluated"
LEARNING_STATUS_SHADOW = "shadow"
LEARNING_STATUS_GRAY = "gray"
LEARNING_STATUS_PROMOTED = "promoted"
LEARNING_STATUS_ROLLED_BACK = "rolled_back"
LEARNING_EVAL_PASS = "pass"
LEARNING_EVAL_WATCH = "watch"
LEARNING_EVAL_FAIL = "fail"
LEARNING_SHADOW_PASS = "pass"
LEARNING_SHADOW_WATCH = "watch"
LEARNING_SHADOW_FAIL = "fail"
LEARNING_EVAL_HOURS = 24 * 7


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _learning_center_path(user_ctx) -> str:
    return os.path.join(user_ctx.user_dir, "learning_center.json")


def _learning_center_id(user_ctx) -> str:
    return f"learn_{getattr(user_ctx, 'user_id', 0)}"


def _default_learning_center(user_ctx) -> Dict[str, Any]:
    return {
        "version": LEARNING_CENTER_VERSION,
        "learning_id": _learning_center_id(user_ctx),
        "sequence": 0,
        "last_generated_at": "",
        "last_generated_candidate_id": "",
        "last_shadow_recorded_at": "",
        "last_promotion_event_at": "",
        "active_shadow_candidate_id": "",
        "active_gray_candidate_id": "",
        "promoted_candidate_id": "",
        "candidates": [],
    }


def _write_learning_center(user_ctx, center: Dict[str, Any]) -> None:
    path = _learning_center_path(user_ctx)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(center, f, indent=2, ensure_ascii=False)


def _sorted_candidates(center: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [item for item in center.get("candidates", []) if isinstance(item, dict)]
    return sorted(
        candidates,
        key=lambda item: (
            int(item.get("candidate_no", 0) or 0),
            str(item.get("created_at", "") or ""),
        ),
    )


def _update_runtime_learning_snapshot(user_ctx, center: Dict[str, Any]) -> None:
    rt = user_ctx.state.runtime
    candidates = _sorted_candidates(center)
    latest = candidates[-1] if candidates else {}
    rt["learning_candidate_count"] = len(candidates)
    rt["learning_last_generated_at"] = str(center.get("last_generated_at", "") or "")
    rt["learning_last_candidate_id"] = str(center.get("last_generated_candidate_id", "") or "")
    rt["learning_last_shadow_at"] = str(center.get("last_shadow_recorded_at", "") or "")
    rt["learning_last_promotion_at"] = str(center.get("last_promotion_event_at", "") or "")
    rt["learning_shadow_candidate_id"] = str(center.get("active_shadow_candidate_id", "") or "")
    rt["learning_gray_candidate_id"] = str(center.get("active_gray_candidate_id", "") or "")
    rt["learning_last_summary"] = str(latest.get("summary", "") or "")


def load_learning_center(user_ctx) -> Dict[str, Any]:
    path = _learning_center_path(user_ctx)
    if not os.path.exists(path):
        center = _default_learning_center(user_ctx)
        _write_learning_center(user_ctx, center)
        _update_runtime_learning_snapshot(user_ctx, center)
        return center
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("learning_center.json 必须为对象")
    except Exception:
        center = _default_learning_center(user_ctx)
        _write_learning_center(user_ctx, center)
        _update_runtime_learning_snapshot(user_ctx, center)
        return center

    center = {
        "version": int(payload.get("version", LEARNING_CENTER_VERSION) or LEARNING_CENTER_VERSION),
        "learning_id": str(payload.get("learning_id", _learning_center_id(user_ctx)) or _learning_center_id(user_ctx)),
        "sequence": int(payload.get("sequence", 0) or 0),
        "last_generated_at": str(payload.get("last_generated_at", "") or ""),
        "last_generated_candidate_id": str(payload.get("last_generated_candidate_id", "") or ""),
        "last_shadow_recorded_at": str(payload.get("last_shadow_recorded_at", "") or ""),
        "last_promotion_event_at": str(payload.get("last_promotion_event_at", "") or ""),
        "active_shadow_candidate_id": str(payload.get("active_shadow_candidate_id", "") or ""),
        "active_gray_candidate_id": str(payload.get("active_gray_candidate_id", "") or ""),
        "promoted_candidate_id": str(payload.get("promoted_candidate_id", "") or ""),
        "candidates": [item for item in payload.get("candidates", []) if isinstance(item, dict)],
    }
    _update_runtime_learning_snapshot(user_ctx, center)
    return center


def _find_candidate(center: Dict[str, Any], ident: str = "") -> Optional[Dict[str, Any]]:
    candidates = _sorted_candidates(center)
    if not candidates:
        return None
    target = str(ident or "").strip().lower()
    if not target:
        return candidates[-1]
    for item in candidates:
        if str(item.get("candidate_id", "") or "").strip().lower() == target:
            return item
        if str(item.get("candidate_version", "") or "").strip().lower() == target:
            return item
    return None


def _find_candidate_strict(center: Dict[str, Any], ident: str = "") -> Optional[Dict[str, Any]]:
    target = str(ident or "").strip()
    if not target:
        return None
    return _find_candidate(center, target)


def _find_candidate_index(center: Dict[str, Any], ident: str = "") -> int:
    target = str(ident or "").strip().lower()
    if not target:
        candidates = [item for item in center.get("candidates", []) if isinstance(item, dict)]
        if not candidates:
            return -1
        latest = _sorted_candidates(center)[-1]
        target = str(latest.get("candidate_id", "") or "").strip().lower()
    for index, item in enumerate(center.get("candidates", [])):
        if not isinstance(item, dict):
            continue
        if str(item.get("candidate_id", "") or "").strip().lower() == target:
            return index
        if str(item.get("candidate_version", "") or "").strip().lower() == target:
            return index
    return -1


def _json_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _trim_evidence(evidence: Dict[str, Any]) -> Dict[str, Any]:
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    regime_24h = evidence.get("regime_24h", {}) if isinstance(evidence.get("regime_24h", {}), dict) else {}
    temperature = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    return {
        "current_regime": str(evidence.get("current_regime", "") or ""),
        "scores": evidence.get("scores", {}) if isinstance(evidence.get("scores", {}), dict) else {},
        "similar_cases": {
            "similar_count": int(similar.get("similar_count", 0) or 0),
            "evidence_strength": str(similar.get("evidence_strength", "insufficient") or "insufficient"),
            "recommended_tier_cap": str(similar.get("recommended_tier_cap", "") or ""),
            "source": str(similar.get("source", "none") or "none"),
            "tiers": similar.get("tiers", {}) if isinstance(similar.get("tiers", {}), dict) else {},
        },
        "overview_24h": {
            "settled_count": int(overview.get("settled_count", 0) or 0),
            "win_rate": float(overview.get("win_rate", 0.0) or 0.0),
            "pnl_total": int(overview.get("pnl_total", 0) or 0),
            "max_drawdown": int(overview.get("max_drawdown", 0) or 0),
            "observe_count": int(overview.get("observe_count", 0) or 0),
            "blocked_count": int(overview.get("blocked_count", 0) or 0),
        },
        "regime_24h": {
            "best_regime": str(regime_24h.get("best_regime", "") or ""),
            "worst_regime": str(regime_24h.get("worst_regime", "") or ""),
            "sample_rounds": int(regime_24h.get("sample_rounds", 0) or 0),
        },
        "recent_temperature": {
            "level": str(temperature.get("level", "normal") or "normal"),
            "settled_10": int(temperature.get("settled_10", 0) or 0),
            "win_rate_10": float(temperature.get("win_rate_10", 0.0) or 0.0),
            "drawdown_10": int(temperature.get("drawdown_10", 0) or 0),
        },
        "tier_24h": evidence.get("tier_24h", {}) if isinstance(evidence.get("tier_24h", {}), dict) else {},
    }


def _render_candidate_prompt(base_fragment: str, overlay_lines: List[str]) -> str:
    clean_lines = [str(item or "").strip() for item in overlay_lines if str(item or "").strip()]
    overlay_text = "\n".join(f"- {line}" for line in clean_lines[:6])
    if not overlay_text:
        return str(base_fragment or "")
    base = str(base_fragment or "").strip()
    if base:
        return f"{base}\n[Learning Candidate Overlay]\n{overlay_text}"
    return f"[Learning Candidate Overlay]\n{overlay_text}"


def _candidate_summary(prefix: str, overlay_lines: List[str]) -> str:
    first_line = str(overlay_lines[0] if overlay_lines else "").strip()
    if not first_line:
        return prefix
    return f"{prefix}：{first_line}"


def _build_observe_guard_candidate(policy_context: Dict[str, Any], evidence: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    current_regime = str(evidence.get("current_regime", "") or "")
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    regime_24h = evidence.get("regime_24h", {}) if isinstance(evidence.get("regime_24h", {}), dict) else {}
    temperature = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    recommended_cap = str(similar.get("recommended_tier_cap", "") or "")
    temp_level = str(temperature.get("level", "normal") or "normal")
    worst_regime = str(regime_24h.get("worst_regime", "") or "")
    pnl24 = int(overview.get("pnl_total", 0) or 0)

    need_guard = (
        current_regime in {history_analysis.REGIME_CHAOS, history_analysis.REGIME_REVERSAL}
        or recommended_cap == "observe"
        or temp_level in {"cold", "very_cold"}
        or (worst_regime in {history_analysis.REGIME_CHAOS, history_analysis.REGIME_REVERSAL} and pnl24 < 0)
    )
    if not need_guard:
        return None

    overlay_lines = [
        "当盘面为混乱盘、反转盘或近期实盘偏冷时，优先观望，不要用模糊证据强行给出下注方向。",
        "当相似历史已经提示 observe 优于强打时，直接输出观望，不再为了出手而勉强下注。",
        "当长期最差盘面落在混乱盘或反转盘且 24h 收益为负时，提高保守权重，证据接近时观望优先。",
    ]
    return {
        "rule_id": "observe_guard",
        "rule_name": "混乱/偏冷观望加强",
        "summary": _candidate_summary("候选-观望加强", overlay_lines),
        "tags": ["observe_guard", current_regime or "-", temp_level],
        "overlay": {
            "mode": "prompt_overlay",
            "prompt_lines": overlay_lines,
            "fk1_overlay": {
                "observe_bias_regimes": [history_analysis.REGIME_CHAOS, history_analysis.REGIME_REVERSAL],
                "temperature_bias": {"cold": "observe", "very_cold": "observe"},
                "respect_history_observe": True,
            },
        },
    }


def _build_tier_cap_candidate(policy_context: Dict[str, Any], evidence: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    tiers = similar.get("tiers", {}) if isinstance(similar.get("tiers", {}), dict) else {}
    high_stats = tiers.get("high", {}) if isinstance(tiers.get("high", {}), dict) else {}
    low_stats = tiers.get("low", {}) if isinstance(tiers.get("low", {}), dict) else {}
    recommended_cap = str(similar.get("recommended_tier_cap", "") or "")
    high_avg = float(high_stats.get("avg_pnl", 0.0) or 0.0)
    low_avg = float(low_stats.get("avg_pnl", 0.0) or 0.0)

    need_cap = recommended_cap in {"yc1", "yc5", "yc10"} or high_avg < 0
    if not need_cap:
        return None

    overlay_lines = [
        "当高档位历史均收益为负或相似历史已经给出低档上限时，优先把执行限制在 yc5 或 yc10，不要继续放大。",
        "如果低档还能做而高档明显更差，保留低档试探，不要为了追回收益而强行抬高档位。",
    ]
    if low_avg > 0:
        overlay_lines.append("当低档历史为正而高档为负时，默认保留低档验证路径，除非新证据明显推翻历史事实。")
    return {
        "rule_id": "tier_cap_guard",
        "rule_name": "高档位收紧",
        "summary": _candidate_summary("候选-高档收紧", overlay_lines),
        "tags": ["tier_cap_guard", recommended_cap or "-", "high_negative" if high_avg < 0 else "history_cap"],
        "overlay": {
            "mode": "fk1_overlay",
            "prompt_lines": overlay_lines,
            "fk1_overlay": {
                "prefer_low_tier_when_high_negative": high_avg < 0,
                "tier_cap_bias": {
                    history_analysis.REGIME_CHAOS: "yc1",
                    history_analysis.REGIME_REVERSAL: "yc5",
                    history_analysis.REGIME_RANGE: "yc5",
                    history_analysis.REGIME_EXHAUSTION: "yc10",
                },
                "respect_history_cap": recommended_cap or "yc5",
            },
        },
    }


def _build_continuation_candidate(policy_context: Dict[str, Any], evidence: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    regime_24h = evidence.get("regime_24h", {}) if isinstance(evidence.get("regime_24h", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    temperature = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    tiers = evidence.get("tier_24h", {}) if isinstance(evidence.get("tier_24h", {}), dict) else {}
    best_regime = str(regime_24h.get("best_regime", "") or "")
    temp_level = str(temperature.get("level", "normal") or "normal")
    pnl24 = int(overview.get("pnl_total", 0) or 0)
    low_group = tiers.get("low", {}) if isinstance(tiers.get("low", {}), dict) else {}
    mid_group = tiers.get("mid", {}) if isinstance(tiers.get("mid", {}), dict) else {}
    low_avg = float(low_group.get("avg_pnl", 0.0) or 0.0)
    mid_avg = float(mid_group.get("avg_pnl", 0.0) or 0.0)

    if not (
        best_regime == history_analysis.REGIME_CONTINUATION
        and temp_level == "normal"
        and pnl24 >= 0
        and (low_avg > 0 or mid_avg > 0)
    ):
        return None

    overlay_lines = [
        "当延续盘是最近 24h 最优盘面且近期温度正常时，优先顺势判断，不要在证据接近时为了抄底或摸顶而逆势下注。",
        "延续盘可给明确方向，但非延续盘仍保持保守，避免把局部顺势经验泛化到所有盘面。",
    ]
    return {
        "rule_id": "continuation_focus",
        "rule_name": "延续盘顺势优先",
        "summary": _candidate_summary("候选-延续盘优先", overlay_lines),
        "tags": ["continuation_focus", best_regime, temp_level],
        "overlay": {
            "mode": "prompt_overlay",
            "prompt_lines": overlay_lines,
            "fk1_overlay": {
                "follow_bias_regimes": [history_analysis.REGIME_CONTINUATION],
                "keep_conservative_regimes": [
                    history_analysis.REGIME_CHAOS,
                    history_analysis.REGIME_REVERSAL,
                    history_analysis.REGIME_RANGE,
                ],
            },
        },
    }


def _candidate_hash(rule_id: str, based_on_version: str, overlay: Dict[str, Any], evidence: Dict[str, Any]) -> str:
    return _json_hash(
        {
            "rule_id": rule_id,
            "based_on_version": based_on_version,
            "overlay": overlay,
            "evidence": _trim_evidence(evidence),
        }
    )


def propose_candidates_from_evidence(user_ctx, analysis_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    policy_context = policy_engine.build_policy_prompt_context(user_ctx, analysis_snapshot)
    evidence = policy_context.get("evidence_package", {}) if isinstance(policy_context.get("evidence_package", {}), dict) else {}
    builders = [
        _build_observe_guard_candidate,
        _build_tier_cap_candidate,
        _build_continuation_candidate,
    ]
    proposals: List[Dict[str, Any]] = []
    seen_hashes = set()
    for builder in builders:
        payload = builder(policy_context, evidence)
        if not payload:
            continue
        overlay = payload.get("overlay", {}) if isinstance(payload.get("overlay", {}), dict) else {}
        candidate_hash = _candidate_hash(
            str(payload.get("rule_id", "") or ""),
            str(policy_context.get("policy_version", "") or ""),
            overlay,
            evidence,
        )
        if candidate_hash in seen_hashes:
            continue
        seen_hashes.add(candidate_hash)
        payload["candidate_hash"] = candidate_hash
        payload["based_on_policy_id"] = str(policy_context.get("policy_id", "") or "")
        payload["based_on_policy_version"] = str(policy_context.get("policy_version", "") or "")
        payload["based_on_policy_mode"] = str(policy_context.get("policy_mode", "") or "")
        payload["source"] = "rule_generator"
        payload["evidence_package"] = _trim_evidence(evidence)
        payload["prompt_fragment"] = _render_candidate_prompt(
            str(policy_context.get("prompt_fragment", "") or ""),
            overlay.get("prompt_lines", []) if isinstance(overlay.get("prompt_lines", []), list) else [],
        )
        proposals.append(payload)
    return {
        "policy_context": policy_context,
        "evidence_package": evidence,
        "proposals": proposals,
    }


def generate_candidates_from_evidence(user_ctx, analysis_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    proposed = propose_candidates_from_evidence(user_ctx, analysis_snapshot)
    proposals = proposed.get("proposals", []) if isinstance(proposed.get("proposals", []), list) else []
    if not proposals:
        return {
            "ok": True,
            "created_count": 0,
            "candidates": [],
            "message": "🧠 本轮未生成新的学习候选\n\n当前复盘证据不足以提出新的可审计候选策略。",
        }

    existing_hashes = {
        str(item.get("candidate_hash", "") or "")
        for item in center.get("candidates", [])
        if isinstance(item, dict) and str(item.get("candidate_hash", "") or "")
    }
    created: List[Dict[str, Any]] = []
    now_text = _now_text()
    for proposal in proposals:
        candidate_hash = str(proposal.get("candidate_hash", "") or "")
        if candidate_hash in existing_hashes:
            continue
        center["sequence"] = int(center.get("sequence", 0) or 0) + 1
        candidate_no = int(center.get("sequence", 0) or 0)
        candidate = {
            "candidate_id": f"lc_{getattr(user_ctx, 'user_id', 0)}_{candidate_no:03d}",
            "candidate_version": f"c{candidate_no}",
            "candidate_no": candidate_no,
            "status": LEARNING_STATUS_GENERATED,
            "created_at": now_text,
            "updated_at": now_text,
            **proposal,
        }
        center.setdefault("candidates", []).append(candidate)
        existing_hashes.add(candidate_hash)
        created.append(candidate)
        try:
            history_analysis.record_learning_candidate(user_ctx, candidate)
        except Exception:
            pass

    if not created:
        _update_runtime_learning_snapshot(user_ctx, center)
        return {
            "ok": True,
            "created_count": 0,
            "candidates": [],
            "message": "🧠 学习候选无变化\n\n当前证据下的新候选已存在，未重复生成。",
        }

    center["last_generated_at"] = now_text
    center["last_generated_candidate_id"] = str(created[-1].get("candidate_id", "") or "")
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "created_count": len(created),
        "candidates": created,
        "message": _build_generation_message(created),
    }


def _build_generation_message(candidates: List[Dict[str, Any]]) -> str:
    lines = ["🧠 已生成学习候选", ""]
    for item in candidates:
        lines.append(
            f"- {item.get('candidate_version', '')} | {item.get('rule_name', '')} | {item.get('summary', '')}"
        )
    lines.extend(
        [
            "",
            "说明：当前已完成 H1/H2/H3/H4/H5（候选中心 / 规则生成 / 离线评估 / 影子验证 / 灰度转正回滚）。",
            "命令：`learn eval` / `learn shadow` / `learn gray` / `learn promote` / `learn rollback`",
        ]
    )
    return "\n".join(lines)


def _tier_amount(name: str) -> int:
    normalized = str(name or "").strip().lower()
    return int(risk_control.TIER_AMOUNT.get(normalized, 0) or 0)


def _tier_rank(name: str) -> int:
    try:
        return risk_control.TIER_ORDER.index(str(name or "").strip().lower())
    except ValueError:
        return -1


def _normalize_tier(name: Any) -> str:
    value = str(name or "").strip().lower()
    return value if value in risk_control.TIER_ORDER else ""


def _derive_temperature_level(settled_pnls: List[int]) -> str:
    recent10 = settled_pnls[-10:]
    if not recent10:
        return "normal"
    wins = sum(1 for item in recent10 if int(item) > 0)
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for pnl in recent10:
        cumulative += int(pnl)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    win_rate = wins / len(recent10)
    total_pnl = sum(int(item) for item in recent10)
    if len(recent10) >= 5 and (win_rate <= 0.30 or (win_rate <= 0.40 and total_pnl < 0 and max_drawdown > 0)):
        return "very_cold"
    if len(recent10) >= 5 and (win_rate <= 0.45 or total_pnl < 0):
        return "cold"
    return "normal"


def _clip_ratio(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


def _evaluation_context(user_ctx, hours: int = LEARNING_EVAL_HOURS) -> Dict[str, Any]:
    analytics = history_analysis._recent_analytics_context(user_ctx, hours=hours)
    rounds = [item for item in analytics.get("rounds", []) if isinstance(item, dict)]
    rounds.sort(key=lambda item: (str(item.get("captured_at", "") or ""), str(item.get("round_key", "") or "")))
    return {
        "analytics": analytics,
        "rounds": rounds,
        "decisions_by_key": analytics.get("decisions_by_key", {}),
        "executions_by_key": analytics.get("executions_by_key", {}),
        "settlements_by_key": analytics.get("settlements_by_key", {}),
        "regimes_by_key": analytics.get("regimes_by_key", {}),
    }


def _candidate_target_regimes(candidate: Dict[str, Any]) -> set:
    overlay = candidate.get("overlay", {}) if isinstance(candidate.get("overlay", {}), dict) else {}
    fk1_overlay = overlay.get("fk1_overlay", {}) if isinstance(overlay.get("fk1_overlay", {}), dict) else {}
    target_regimes = set()
    for key in ("observe_bias_regimes", "follow_bias_regimes", "keep_conservative_regimes"):
        values = fk1_overlay.get(key, [])
        if isinstance(values, list):
            target_regimes.update(str(item or "") for item in values if str(item or "").strip())
    tier_cap_bias = fk1_overlay.get("tier_cap_bias", {}) if isinstance(fk1_overlay.get("tier_cap_bias", {}), dict) else {}
    target_regimes.update(str(key or "") for key in tier_cap_bias.keys() if str(key or "").strip())
    return {item for item in target_regimes if item}


def _simulate_candidate_round(
    candidate: Dict[str, Any],
    round_row: Dict[str, Any],
    regime_row: Dict[str, Any],
    decision_row: Dict[str, Any],
    execution_row: Dict[str, Any],
    settlement_row: Dict[str, Any],
    rolling_temp: str,
) -> Dict[str, Any]:
    overlay = candidate.get("overlay", {}) if isinstance(candidate.get("overlay", {}), dict) else {}
    fk1_overlay = overlay.get("fk1_overlay", {}) if isinstance(overlay.get("fk1_overlay", {}), dict) else {}
    regime_label = str(regime_row.get("regime_label", history_analysis.REGIME_RANGE) or history_analysis.REGIME_RANGE)
    direction_code = str(decision_row.get("direction_code", "") or "")
    is_observe = int(decision_row.get("is_observe", 0) or 0) == 1 or direction_code == "observe"
    base_action = str(execution_row.get("action_type", "") or "")
    if not base_action:
        base_action = "observe" if is_observe else "blocked"
    base_tier = _normalize_tier(execution_row.get("preset_name", ""))
    base_pnl = int(settlement_row.get("profit", 0) or 0) if base_action == "bet" else 0
    candidate_action = base_action
    candidate_tier = base_tier
    candidate_pnl = base_pnl
    modified = False
    capped = False
    targeted = False
    reasons: List[str] = []

    temp_bias = fk1_overlay.get("temperature_bias", {}) if isinstance(fk1_overlay.get("temperature_bias", {}), dict) else {}
    observe_bias_regimes = fk1_overlay.get("observe_bias_regimes", []) if isinstance(fk1_overlay.get("observe_bias_regimes", []), list) else []
    tier_cap_bias = fk1_overlay.get("tier_cap_bias", {}) if isinstance(fk1_overlay.get("tier_cap_bias", {}), dict) else {}
    respect_history_cap = _normalize_tier(fk1_overlay.get("respect_history_cap", ""))

    if not is_observe and str(temp_bias.get(rolling_temp, "") or "") == "observe":
        candidate_action = "observe"
        candidate_tier = ""
        candidate_pnl = 0
        modified = True
        targeted = True
        reasons.append(f"温度 {rolling_temp} -> 观望")
    elif not is_observe and regime_label in {str(item or "") for item in observe_bias_regimes}:
        candidate_action = "observe"
        candidate_tier = ""
        candidate_pnl = 0
        modified = True
        targeted = True
        reasons.append(f"{regime_label} -> 观望")
    elif candidate_action == "bet":
        cap_tier = ""
        if respect_history_cap:
            cap_tier = respect_history_cap
        regime_cap = _normalize_tier(tier_cap_bias.get(regime_label, ""))
        if regime_cap:
            cap_tier = risk_control.stricter_tier_cap(cap_tier, regime_cap) if cap_tier else regime_cap
            targeted = True
        if fk1_overlay.get("prefer_low_tier_when_high_negative") and _tier_rank(base_tier) > _tier_rank("yc10"):
            cap_tier = risk_control.stricter_tier_cap(cap_tier, "yc10") if cap_tier else "yc10"
        cap_tier = _normalize_tier(cap_tier)
        if cap_tier and base_tier:
            applied_tier = risk_control.stricter_tier_cap(base_tier, cap_tier)
            if applied_tier != base_tier:
                candidate_tier = applied_tier
                modified = True
                capped = True
                reasons.append(f"{base_tier} -> {applied_tier}")
                base_amount = _tier_amount(base_tier)
                candidate_amount = _tier_amount(applied_tier)
                if base_amount > 0 and candidate_amount > 0:
                    candidate_pnl = int(round(base_pnl * (candidate_amount / base_amount)))

    if base_action != "bet" and candidate_action == "bet":
        candidate_action = base_action
        candidate_tier = ""
        candidate_pnl = 0
        modified = False
        capped = False

    result_side = str(round_row.get("result_side", "") or "")
    signal_hit = 1 if (candidate_action != "observe" and direction_code in {"big", "small"} and direction_code == result_side) else 0
    bet_hit = 1 if (candidate_action == "bet" and int(settlement_row.get("is_win", 0) or 0) == 1) else 0
    return {
        "candidate_action": candidate_action,
        "candidate_tier": candidate_tier,
        "candidate_pnl": candidate_pnl,
        "base_action": base_action,
        "base_tier": base_tier,
        "base_pnl": base_pnl,
        "signal_hit": signal_hit,
        "bet_hit": bet_hit,
        "modified": modified,
        "capped": capped,
        "targeted": targeted,
        "reasons": reasons,
        "regime_label": regime_label,
    }


def _summarize_evaluation(candidate: Dict[str, Any], metrics: Dict[str, Any]) -> str:
    status = str(metrics.get("status", LEARNING_EVAL_WATCH) or LEARNING_EVAL_WATCH)
    delta_pnl = int(metrics.get("delta_pnl", 0) or 0)
    delta_drawdown = int(metrics.get("baseline_drawdown", 0) or 0) - int(metrics.get("candidate_drawdown", 0) or 0)
    if status == LEARNING_EVAL_PASS:
        return f"离线评估通过：收益变化 {delta_pnl:+,}，回撤改善 {delta_drawdown:+,}"
    if status == LEARNING_EVAL_FAIL:
        return f"离线评估偏弱：收益变化 {delta_pnl:+,}，回撤改善 {delta_drawdown:+,}"
    return f"离线评估观察：收益变化 {delta_pnl:+,}，回撤改善 {delta_drawdown:+,}"


def evaluate_candidate_offline(user_ctx, ident: str = "", hours: int = LEARNING_EVAL_HOURS) -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate(center, ident)
    if not candidate:
        return {"ok": False, "message": "❌ 未找到对应学习候选"}

    context = _evaluation_context(user_ctx, hours=hours)
    rounds = context["rounds"]
    decisions_by_key = context["decisions_by_key"]
    executions_by_key = context["executions_by_key"]
    settlements_by_key = context["settlements_by_key"]
    regimes_by_key = context["regimes_by_key"]

    sample_size = 0
    scored_size = 0
    signal_total = 0
    signal_hits = 0
    candidate_bet_count = 0
    candidate_bet_hits = 0
    candidate_observe_count = 0
    tier_cap_count = 0
    modified_count = 0
    targeted_changes = 0
    baseline_observe_count = 0
    baseline_pnls: List[int] = []
    candidate_pnls: List[int] = []
    settled_history: List[int] = []
    target_regimes = _candidate_target_regimes(candidate)
    regime_counter: Dict[str, int] = {}

    for round_row in rounds:
        round_key = str(round_row.get("round_key", "") or "")
        decision_row = decisions_by_key.get(round_key, {})
        if not decision_row:
            continue
        regime_row = regimes_by_key.get(round_key, {})
        execution_row = executions_by_key.get(round_key, {})
        settlement_row = settlements_by_key.get(round_key, {})
        rolling_temp = _derive_temperature_level(settled_history)
        sample_size += 1

        if int(decision_row.get("is_observe", 0) or 0) == 1:
            baseline_observe_count += 1

        simulated = _simulate_candidate_round(
            candidate,
            round_row,
            regime_row,
            decision_row,
            execution_row,
            settlement_row,
            rolling_temp,
        )
        regime_label = str(simulated.get("regime_label", history_analysis.REGIME_RANGE) or history_analysis.REGIME_RANGE)
        regime_counter[regime_label] = regime_counter.get(regime_label, 0) + 1
        scored_size += 1

        baseline_pnls.append(int(simulated.get("base_pnl", 0) or 0))
        candidate_pnls.append(int(simulated.get("candidate_pnl", 0) or 0))

        if simulated["candidate_action"] == "observe":
            candidate_observe_count += 1
        if simulated["capped"]:
            tier_cap_count += 1
        if simulated["modified"]:
            modified_count += 1
            if simulated["targeted"] or (target_regimes and regime_label in target_regimes):
                targeted_changes += 1
        if simulated["candidate_action"] != "observe":
            signal_total += 1
            signal_hits += int(simulated["signal_hit"] or 0)
        if simulated["candidate_action"] == "bet":
            candidate_bet_count += 1
            candidate_bet_hits += int(simulated["bet_hit"] or 0)

        if str(simulated.get("base_action", "") or "") == "bet":
            settled_history.append(int(simulated.get("base_pnl", 0) or 0))

    if sample_size <= 0:
        return {"ok": False, "message": "❌ 当前历史资产不足，无法进行离线评估"}

    baseline_total_pnl = sum(int(item) for item in baseline_pnls)
    candidate_total_pnl = sum(int(item) for item in candidate_pnls)
    baseline_drawdown = history_analysis._max_drawdown(baseline_pnls)
    candidate_drawdown = history_analysis._max_drawdown(candidate_pnls)
    coverage_rate = round(scored_size / sample_size, 4) if sample_size else 0.0
    signal_hit_rate = round(signal_hits / signal_total, 4) if signal_total else 0.0
    bet_hit_rate = round(candidate_bet_hits / candidate_bet_count, 4) if candidate_bet_count else 0.0
    observe_rate = round(candidate_observe_count / sample_size, 4) if sample_size else 0.0
    baseline_observe_rate = round(baseline_observe_count / sample_size, 4) if sample_size else 0.0
    tier_cap_rate = round(tier_cap_count / sample_size, 4) if sample_size else 0.0
    regime_stability = round(targeted_changes / modified_count, 4) if modified_count else 1.0
    delta_pnl = int(candidate_total_pnl - baseline_total_pnl)
    delta_drawdown = int(baseline_drawdown - candidate_drawdown)

    baseline_pnl_den = max(abs(baseline_total_pnl), 1000)
    baseline_dd_den = max(int(baseline_drawdown), 1000)
    score_total = round(
        50.0
        + 20.0 * _clip_ratio(delta_drawdown / baseline_dd_den)
        + 15.0 * _clip_ratio(delta_pnl / baseline_pnl_den)
        + 10.0 * float(regime_stability)
        + 5.0 * float(coverage_rate)
        - 10.0 * max(0.0, float(observe_rate - baseline_observe_rate)),
        2,
    )

    if sample_size < 12 or coverage_rate < 0.8:
        eval_status = LEARNING_EVAL_FAIL
    elif score_total >= 58 and (candidate_drawdown <= baseline_drawdown or candidate_total_pnl >= baseline_total_pnl):
        eval_status = LEARNING_EVAL_PASS
    elif score_total >= 48:
        eval_status = LEARNING_EVAL_WATCH
    else:
        eval_status = LEARNING_EVAL_FAIL

    evaluation_id = f"le_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}"
    created_at = _now_text()
    metrics = {
        "sample_size": sample_size,
        "coverage_rate": coverage_rate,
        "signal_hit_rate": signal_hit_rate,
        "bet_hit_rate": bet_hit_rate,
        "observe_rate": observe_rate,
        "baseline_observe_rate": baseline_observe_rate,
        "tier_cap_rate": tier_cap_rate,
        "avg_pnl": round(candidate_total_pnl / candidate_bet_count, 2) if candidate_bet_count else 0.0,
        "total_pnl": candidate_total_pnl,
        "baseline_total_pnl": baseline_total_pnl,
        "delta_pnl": delta_pnl,
        "max_drawdown": candidate_drawdown,
        "baseline_drawdown": baseline_drawdown,
        "delta_drawdown": delta_drawdown,
        "regime_stability": regime_stability,
        "score_total": score_total,
        "modified_count": modified_count,
        "targeted_changes": targeted_changes,
        "regime_counter": regime_counter,
        "window_hours": int(hours),
    }
    evaluation = {
        "evaluation_id": evaluation_id,
        "candidate_id": str(candidate.get("candidate_id", "") or ""),
        "candidate_version": str(candidate.get("candidate_version", "") or ""),
        "status": eval_status,
        "sample_size": sample_size,
        "score_total": score_total,
        "metrics": metrics,
        "created_at": created_at,
    }
    history_analysis.record_learning_evaluation(user_ctx, evaluation)

    idx = _find_candidate_index(center, ident or str(candidate.get("candidate_id", "") or ""))
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_EVALUATED
        center["candidates"][idx]["updated_at"] = created_at
        center["candidates"][idx]["last_evaluation_id"] = evaluation_id
        center["candidates"][idx]["last_evaluated_at"] = created_at
        center["candidates"][idx]["last_evaluation_status"] = eval_status
        center["candidates"][idx]["last_score_total"] = score_total
        center["candidates"][idx]["last_evaluation_summary"] = _summarize_evaluation(candidate, {**metrics, "status": eval_status})
        _write_learning_center(user_ctx, center)
        _update_runtime_learning_snapshot(user_ctx, center)

    return {
        "ok": True,
        "evaluation": evaluation,
        "metrics": metrics,
        "message": build_learning_evaluation_text(candidate, evaluation),
    }


def _normalize_result_side(result_num: Any, result_type: Any) -> str:
    text = str(result_type or "").strip().lower()
    if text in {"大", "big", "1"}:
        return "big"
    if text in {"小", "small", "0"}:
        return "small"
    try:
        return "big" if int(result_num) == 1 else "small"
    except (TypeError, ValueError):
        return ""


def _shadow_status_text(status: str) -> str:
    mapping = {
        LEARNING_SHADOW_PASS: "通过",
        LEARNING_SHADOW_WATCH: "观察",
        LEARNING_SHADOW_FAIL: "偏弱",
    }
    return mapping.get(str(status or LEARNING_SHADOW_WATCH), str(status or LEARNING_SHADOW_WATCH))


def _parse_shadow_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("payload_json", {})
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _shadow_diff_type(
    base_action: str,
    candidate_action: str,
    base_tier: str,
    candidate_tier: str,
    base_direction: str,
    candidate_direction: str,
) -> str:
    if (
        base_action == candidate_action
        and base_tier == candidate_tier
        and str(base_direction or "") == str(candidate_direction or "")
    ):
        return "same"
    if base_action == "bet" and candidate_action == "observe":
        return "observe_vs_bet"
    if base_action == "bet" and candidate_action == "bet":
        base_rank = _tier_rank(base_tier)
        candidate_rank = _tier_rank(candidate_tier)
        if base_rank >= 0 and candidate_rank >= 0:
            if candidate_rank < base_rank:
                return "tier_more_conservative"
            if candidate_rank > base_rank:
                return "tier_more_aggressive"
    if (
        base_action == "bet"
        and candidate_action == "bet"
        and base_direction
        and candidate_direction
        and base_direction != candidate_direction
    ):
        return "direction_diff"
    return "same"


def _shadow_rows(user_ctx, candidate_id: str) -> List[Dict[str, Any]]:
    return history_analysis._analytics_rows(
        user_ctx,
        "SELECT * FROM learning_shadows WHERE candidate_id = ? ORDER BY created_at ASC",
        (str(candidate_id or ""),),
    )


def _shadow_status_from_metrics(metrics: Dict[str, Any]) -> str:
    sample_size = int(metrics.get("sample_size", 0) or 0)
    aggressive_count = int(metrics.get("aggressive_count", 0) or 0)
    base_total_pnl = int(metrics.get("base_total_pnl", 0) or 0)
    candidate_total_pnl = int(metrics.get("candidate_total_pnl", 0) or 0)
    base_drawdown = int(metrics.get("base_drawdown", 0) or 0)
    candidate_drawdown = int(metrics.get("candidate_drawdown", 0) or 0)
    tolerance = max(2000, int(abs(base_total_pnl) * 0.2))

    if sample_size < 12:
        return LEARNING_SHADOW_WATCH
    if aggressive_count > max(2, sample_size // 4) and candidate_total_pnl < base_total_pnl:
        return LEARNING_SHADOW_FAIL
    if candidate_drawdown > base_drawdown and candidate_total_pnl + tolerance < base_total_pnl:
        return LEARNING_SHADOW_FAIL
    if (
        aggressive_count <= max(1, sample_size // 10)
        and candidate_drawdown <= base_drawdown
        and candidate_total_pnl + tolerance >= base_total_pnl
    ):
        return LEARNING_SHADOW_PASS
    return LEARNING_SHADOW_WATCH


def _summarize_shadow_metrics(metrics: Dict[str, Any]) -> str:
    status = str(metrics.get("status", LEARNING_SHADOW_WATCH) or LEARNING_SHADOW_WATCH)
    sample_size = int(metrics.get("sample_size", 0) or 0)
    diff_count = int(metrics.get("diff_count", 0) or 0)
    conservative_count = int(metrics.get("conservative_count", 0) or 0)
    aggressive_count = int(metrics.get("aggressive_count", 0) or 0)
    delta_pnl = int(metrics.get("delta_pnl", 0) or 0)
    delta_drawdown = int(metrics.get("delta_drawdown", 0) or 0)
    return (
        f"影子{_shadow_status_text(status)}：样本 {sample_size}，差异 {diff_count}，"
        f"保守 {conservative_count} / 激进 {aggressive_count}，"
        f"收益变化 {delta_pnl:+,}，回撤改善 {delta_drawdown:+,}"
    )


def _shadow_metrics_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    base_pnls: List[int] = []
    candidate_pnls: List[int] = []
    diff_count = 0
    same_count = 0
    conservative_count = 0
    aggressive_count = 0
    observe_vs_bet_count = 0
    direction_diff_count = 0
    signal_total = 0
    signal_hits = 0
    bet_total = 0
    bet_hits = 0
    last_recorded_at = ""

    for row in rows:
        payload = _parse_shadow_payload(row)
        base_decision = payload.get("base_decision", {}) if isinstance(payload.get("base_decision", {}), dict) else {}
        candidate_decision = (
            payload.get("candidate_decision", {})
            if isinstance(payload.get("candidate_decision", {}), dict)
            else {}
        )
        diff_type = str(row.get("diff_type", payload.get("decision_diff_type", "same")) or "same")
        base_action = str(base_decision.get("action", "") or "")
        candidate_action = str(candidate_decision.get("action", "") or "")
        base_pnls.append(int(base_decision.get("pnl", 0) or 0))
        candidate_pnls.append(int(candidate_decision.get("pnl", 0) or 0))
        last_recorded_at = str(row.get("created_at", last_recorded_at) or last_recorded_at)

        if diff_type == "same":
            same_count += 1
        else:
            diff_count += 1
        if diff_type in {"observe_vs_bet", "tier_more_conservative"}:
            conservative_count += 1
        if diff_type == "observe_vs_bet":
            observe_vs_bet_count += 1
        if diff_type == "tier_more_aggressive":
            aggressive_count += 1
        if diff_type == "direction_diff":
            direction_diff_count += 1

        if candidate_action != "observe":
            signal_total += 1
            signal_hits += 1 if int(candidate_decision.get("signal_hit", 0) or 0) == 1 else 0
        if candidate_action == "bet":
            bet_total += 1
            bet_hits += 1 if int(candidate_decision.get("bet_hit", 0) or 0) == 1 else 0
        if base_action == "bet" and candidate_action == "observe":
            aggressive_count += 0

    base_total_pnl = sum(base_pnls)
    candidate_total_pnl = sum(candidate_pnls)
    base_drawdown = history_analysis._max_drawdown(base_pnls)
    candidate_drawdown = history_analysis._max_drawdown(candidate_pnls)
    metrics = {
        "sample_size": len(rows),
        "same_count": same_count,
        "diff_count": diff_count,
        "conservative_count": conservative_count,
        "aggressive_count": aggressive_count,
        "observe_vs_bet_count": observe_vs_bet_count,
        "direction_diff_count": direction_diff_count,
        "candidate_signal_hit_rate": round(signal_hits / signal_total, 4) if signal_total else 0.0,
        "candidate_bet_hit_rate": round(bet_hits / bet_total, 4) if bet_total else 0.0,
        "base_total_pnl": base_total_pnl,
        "candidate_total_pnl": candidate_total_pnl,
        "delta_pnl": int(candidate_total_pnl - base_total_pnl),
        "base_drawdown": base_drawdown,
        "candidate_drawdown": candidate_drawdown,
        "delta_drawdown": int(base_drawdown - candidate_drawdown),
        "last_recorded_at": last_recorded_at,
    }
    metrics["status"] = _shadow_status_from_metrics(metrics)
    metrics["summary"] = _summarize_shadow_metrics(metrics)
    return metrics


def _shadow_metrics(user_ctx, candidate_id: str) -> Dict[str, Any]:
    return _shadow_metrics_from_rows(_shadow_rows(user_ctx, candidate_id))


def _apply_shadow_metrics(candidate: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    candidate["last_shadow_sample_size"] = int(metrics.get("sample_size", 0) or 0)
    candidate["last_shadow_diff_count"] = int(metrics.get("diff_count", 0) or 0)
    candidate["last_shadow_conservative_count"] = int(metrics.get("conservative_count", 0) or 0)
    candidate["last_shadow_aggressive_count"] = int(metrics.get("aggressive_count", 0) or 0)
    candidate["last_shadow_delta_pnl"] = int(metrics.get("delta_pnl", 0) or 0)
    candidate["last_shadow_delta_drawdown"] = int(metrics.get("delta_drawdown", 0) or 0)
    candidate["last_shadow_status"] = str(metrics.get("status", LEARNING_SHADOW_WATCH) or LEARNING_SHADOW_WATCH)
    candidate["last_shadow_summary"] = str(metrics.get("summary", "") or "")
    candidate["last_shadowed_at"] = str(metrics.get("last_recorded_at", candidate.get("last_shadowed_at", "")) or "")


def activate_candidate_shadow(user_ctx, ident: str = "") -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate(center, ident)
    if not candidate:
        return {"ok": False, "message": "❌ 未找到对应学习候选"}

    eval_status = str(candidate.get("last_evaluation_status", "") or "")
    if not eval_status:
        return {"ok": False, "message": "❌ 请先执行 `learn eval`，再开启影子验证"}
    if eval_status == LEARNING_EVAL_FAIL:
        return {"ok": False, "message": "❌ 当前候选离线评估偏弱，暂不允许开启影子验证"}

    candidate_id = str(candidate.get("candidate_id", "") or "")
    if str(center.get("active_shadow_candidate_id", "") or "") == candidate_id:
        return {
            "ok": True,
            "candidate": candidate,
            "message": (
                "🧠 影子验证已在运行\n\n"
                f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
                "查看详情：`learn shadow`"
            ),
        }

    now_text = _now_text()
    center["active_shadow_candidate_id"] = candidate_id
    idx = _find_candidate_index(center, candidate_id)
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_SHADOW
        center["candidates"][idx]["updated_at"] = now_text
        center["candidates"][idx]["shadow_started_at"] = now_text
        metrics = _shadow_metrics(user_ctx, candidate_id)
        _apply_shadow_metrics(center["candidates"][idx], metrics)
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass

    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "candidate": _find_candidate(center, candidate_id),
        "message": (
            "🧠 已开启影子验证\n\n"
            f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
            f"离线状态：{candidate.get('last_evaluation_status', '-') or '-'}\n"
            "后续：系统会在每次结算时记录同盘面对比，不影响真钱下注。"
        ),
    }


def deactivate_candidate_shadow(user_ctx, ident: str = "") -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    active_id = str(center.get("active_shadow_candidate_id", "") or "")
    if not active_id:
        return {"ok": True, "message": "🧠 当前没有运行中的影子验证"}

    candidate = _find_candidate(center, ident or active_id)
    if not candidate:
        center["active_shadow_candidate_id"] = ""
        _write_learning_center(user_ctx, center)
        _update_runtime_learning_snapshot(user_ctx, center)
        return {"ok": True, "message": "🧠 已清理失效的影子验证标记"}

    candidate_id = str(candidate.get("candidate_id", "") or "")
    if candidate_id != active_id:
        return {"ok": False, "message": "❌ 指定候选当前不是运行中的影子验证对象"}

    now_text = _now_text()
    metrics = _shadow_metrics(user_ctx, candidate_id)
    idx = _find_candidate_index(center, candidate_id)
    if idx >= 0:
        center["candidates"][idx]["updated_at"] = now_text
        center["candidates"][idx]["shadow_stopped_at"] = now_text
        _apply_shadow_metrics(center["candidates"][idx], metrics)
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass
    center["active_shadow_candidate_id"] = ""
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "candidate": candidate,
        "metrics": metrics,
        "message": (
            "🧠 已关闭影子验证\n\n"
            f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
            f"摘要：{metrics.get('summary', '暂无影子样本') or '暂无影子样本'}"
        ),
    }


def _shadow_round_context(user_ctx, round_key: str) -> Dict[str, Dict[str, Any]]:
    rounds = history_analysis._rows_by_round_key(user_ctx, "rounds", [round_key])
    decisions = history_analysis._rows_by_round_key(user_ctx, "decisions", [round_key])
    executions = history_analysis._rows_by_round_key(user_ctx, "execution_records", [round_key])
    settlements = history_analysis._rows_by_round_key(user_ctx, "settlements", [round_key])
    regimes = history_analysis._rows_by_round_key(user_ctx, "regime_features", [round_key])
    return {
        "round_row": history_analysis._latest_row_map(rounds).get(round_key, {}),
        "decision_row": history_analysis._latest_row_map(decisions).get(round_key, {}),
        "execution_row": history_analysis._latest_row_map(executions).get(round_key, {}),
        "settlement_row": history_analysis._latest_row_map(settlements).get(round_key, {}),
        "regime_row": history_analysis._latest_row_map(regimes).get(round_key, {}),
    }


def _fallback_execution_row(user_ctx, round_key: str) -> Dict[str, Any]:
    rt = user_ctx.state.runtime
    action_type = str(rt.get("last_execution_action", "") or "")
    if action_type not in {"bet", "blocked", "observe", "strategy_observe"}:
        return {}
    normalized_action = "observe" if action_type == "strategy_observe" else action_type
    return {
        "round_key": round_key,
        "action_type": normalized_action,
        "blocked_by": str(rt.get("last_blocked_by", "") or ""),
        "preset_name": str(rt.get("current_dynamic_tier", rt.get("current_preset_name", "")) or ""),
        "bet_amount": int(rt.get("bet_amount", 0) or 0),
    }


def _synthetic_shadow_settlement(
    user_ctx,
    round_key: str,
    result_side: str,
    decision_row: Dict[str, Any],
    execution_row: Dict[str, Any],
) -> Dict[str, Any]:
    action_type = str(execution_row.get("action_type", "") or "")
    if action_type != "bet":
        return {"round_key": round_key, "profit": 0, "is_win": 0}

    bet_amount = int(execution_row.get("bet_amount", 0) or 0)
    if bet_amount <= 0:
        for item in reversed(list(getattr(user_ctx.state, "bet_sequence_log", []) or [])):
            if not isinstance(item, dict):
                continue
            if str(item.get("round_key", "") or "") != round_key:
                continue
            bet_amount = int(item.get("amount", 0) or 0)
            break
    direction_code = str(decision_row.get("direction_code", "") or "")
    is_win = 1 if direction_code in {"big", "small"} and direction_code == result_side else 0
    profit = int(round(bet_amount * 0.99)) if is_win and bet_amount > 0 else -bet_amount
    return {
        "round_key": round_key,
        "profit": profit,
        "is_win": is_win,
    }


def _shadow_temperature_level(user_ctx, round_key: str) -> str:
    rows = history_analysis._analytics_rows(
        user_ctx,
        "SELECT round_key, profit FROM settlements ORDER BY settled_at ASC",
    )
    pnls = [
        int(row.get("profit", 0) or 0)
        for row in rows
        if str(row.get("round_key", "") or "") != str(round_key or "")
    ]
    return _derive_temperature_level(pnls)


def record_active_shadow_round(
    user_ctx,
    round_key: str = "",
    result_num: Any = None,
    result_type: Any = "",
) -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate_strict(center, str(center.get("active_shadow_candidate_id", "") or ""))
    if not candidate:
        if str(center.get("active_shadow_candidate_id", "") or ""):
            center["active_shadow_candidate_id"] = ""
            _write_learning_center(user_ctx, center)
            _update_runtime_learning_snapshot(user_ctx, center)
        return {"ok": True, "recorded": False}

    round_key = str(round_key or getattr(user_ctx.state, "runtime", {}).get("current_round_key", "") or "")
    if not round_key:
        return {"ok": True, "recorded": False}

    context = _shadow_round_context(user_ctx, round_key)
    decision_row = context.get("decision_row", {}) if isinstance(context.get("decision_row", {}), dict) else {}
    if not decision_row:
        return {"ok": True, "recorded": False}

    execution_row = context.get("execution_row", {}) if isinstance(context.get("execution_row", {}), dict) else {}
    if not execution_row:
        execution_row = _fallback_execution_row(user_ctx, round_key)
    if not execution_row:
        return {"ok": True, "recorded": False}

    round_row = context.get("round_row", {}) if isinstance(context.get("round_row", {}), dict) else {}
    result_side = str(round_row.get("result_side", "") or "") or _normalize_result_side(result_num, result_type)
    if not round_row:
        round_row = {"round_key": round_key, "result_side": result_side}
    elif result_side and not str(round_row.get("result_side", "") or ""):
        round_row["result_side"] = result_side

    settlement_row = (
        context.get("settlement_row", {})
        if isinstance(context.get("settlement_row", {}), dict)
        else {}
    )
    if not settlement_row:
        settlement_row = _synthetic_shadow_settlement(user_ctx, round_key, result_side, decision_row, execution_row)
    regime_row = context.get("regime_row", {}) if isinstance(context.get("regime_row", {}), dict) else {}
    rolling_temp = _shadow_temperature_level(user_ctx, round_key)
    simulated = _simulate_candidate_round(
        candidate,
        round_row,
        regime_row,
        decision_row,
        execution_row,
        settlement_row,
        rolling_temp,
    )
    base_direction = str(decision_row.get("direction_code", "") or "")
    candidate_direction = base_direction
    diff_type = _shadow_diff_type(
        str(simulated.get("base_action", "") or ""),
        str(simulated.get("candidate_action", "") or ""),
        str(simulated.get("base_tier", "") or ""),
        str(simulated.get("candidate_tier", "") or ""),
        base_direction,
        candidate_direction,
    )
    created_at = _now_text()
    base_bet_amount = int(execution_row.get("bet_amount", 0) or 0) if simulated.get("base_action") == "bet" else 0
    candidate_tier = str(simulated.get("candidate_tier", "") or "")
    candidate_bet_amount = _tier_amount(candidate_tier) if candidate_tier else 0
    payload = {
        "round_key": round_key,
        "result_side": result_side,
        "regime_label": str(simulated.get("regime_label", "") or ""),
        "decision_diff_type": diff_type,
        "base_policy_version": str(
            decision_row.get("policy_version", candidate.get("based_on_policy_version", "")) or ""
        ),
        "candidate_version": str(candidate.get("candidate_version", "") or ""),
        "base_decision": {
            "action": str(simulated.get("base_action", "") or ""),
            "direction_code": base_direction,
            "tier": str(simulated.get("base_tier", "") or ""),
            "bet_amount": base_bet_amount,
            "pnl": int(simulated.get("base_pnl", 0) or 0),
            "blocked_by": str(execution_row.get("blocked_by", "") or ""),
            "signal_hit": 1
            if str(simulated.get("base_action", "") or "") != "observe"
            and base_direction in {"big", "small"}
            and base_direction == result_side
            else 0,
            "bet_hit": int(settlement_row.get("is_win", 0) or 0)
            if str(simulated.get("base_action", "") or "") == "bet"
            else 0,
        },
        "candidate_decision": {
            "action": str(simulated.get("candidate_action", "") or ""),
            "direction_code": candidate_direction,
            "tier": candidate_tier,
            "bet_amount": candidate_bet_amount,
            "pnl": int(simulated.get("candidate_pnl", 0) or 0),
            "signal_hit": int(simulated.get("signal_hit", 0) or 0),
            "bet_hit": int(simulated.get("bet_hit", 0) or 0),
            "reasons": list(simulated.get("reasons", []) or []),
        },
    }
    shadow_record = {
        "shadow_id": f"{str(candidate.get('candidate_id', '') or '')}:{round_key}",
        "candidate_id": str(candidate.get("candidate_id", "") or ""),
        "candidate_version": str(candidate.get("candidate_version", "") or ""),
        "round_key": round_key,
        "status": "recorded",
        "diff_type": diff_type,
        "payload": payload,
        "created_at": created_at,
    }
    history_analysis.record_learning_shadow(user_ctx, shadow_record)

    center["last_shadow_recorded_at"] = created_at
    idx = _find_candidate_index(center, str(candidate.get("candidate_id", "") or ""))
    metrics: Dict[str, Any] = {}
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_SHADOW
        center["candidates"][idx]["updated_at"] = created_at
        center["candidates"][idx]["last_shadow_diff_type"] = diff_type
        metrics = _shadow_metrics(user_ctx, str(candidate.get("candidate_id", "") or ""))
        _apply_shadow_metrics(center["candidates"][idx], metrics)
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "recorded": True,
        "shadow": shadow_record,
        "metrics": metrics,
    }


def _dedupe_lines(lines: List[str]) -> List[str]:
    deduped: List[str] = []
    for line in lines:
        text = str(line or "").strip()
        if text and text not in deduped:
            deduped.append(text)
    return deduped


def _resolve_gray_target_user(user_ctx, target: str = "") -> Dict[str, Any]:
    target_text = str(target or "").strip()
    current_name = str(getattr(getattr(user_ctx, "config", None), "name", "") or "")
    current_id = str(getattr(user_ctx, "user_id", "") or "")
    if not target_text or target_text.lower() in {current_name.lower(), current_id.lower()}:
        return {"ok": True, "user_ctx": user_ctx}

    for candidate_ctx in get_registered_user_contexts().values():
        if not candidate_ctx:
            continue
        candidate_name = str(getattr(getattr(candidate_ctx, "config", None), "name", "") or "")
        candidate_id = str(getattr(candidate_ctx, "user_id", "") or "")
        if target_text.lower() not in {candidate_name.lower(), candidate_id.lower()}:
            continue
        if str(candidate_id) == current_id:
            return {"ok": True, "user_ctx": user_ctx}
        return {
            "ok": False,
            "message": "❌ H5 只允许当前账号单账户灰度，不支持把候选直接下发到其他账号",
        }
    return {"ok": False, "message": f"❌ 未找到目标账号 `{target_text}`"}


def _ensure_candidate_policy_version(
    user_ctx,
    candidate: Dict[str, Any],
    *,
    activation_mode: str,
    policy_field: str,
    summary_prefix: str,
    based_on_version: str = "",
) -> Dict[str, Any]:
    store = policy_engine.load_policy_store(user_ctx)
    existing_version = str(candidate.get(policy_field, "") or "")
    if existing_version:
        existing = policy_engine._find_policy_version(store, existing_version)
        if existing:
            return {"ok": True, "created": False, "policy": existing}

    policies = policy_engine._sorted_policies(store)
    base_version = str(
        based_on_version
        or candidate.get("gray_base_policy_version", "")
        or candidate.get("based_on_policy_version", "")
        or store.get("active_version", "")
        or ""
    )
    base_policy = (
        policy_engine._find_policy_version(store, base_version)
        or policy_engine._find_policy_version(store, store.get("active_version", ""))
        or (policies[-1] if policies else None)
    )
    if not base_policy:
        return {"ok": False, "message": "❌ 当前没有可用的策略版本作为灰度/转正基线"}

    overlay = candidate.get("overlay", {}) if isinstance(candidate.get("overlay", {}), dict) else {}
    overlay_lines = overlay.get("prompt_lines", []) if isinstance(overlay.get("prompt_lines", []), list) else []
    writeback_lines = _dedupe_lines(list(base_policy.get("writeback_lines", []) or []) + list(overlay_lines or []))[:8]
    created_at = _now_text()
    summary = f"{summary_prefix} {candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}"
    evidence_package = {
        "kind": "learning_candidate",
        "candidate_id": str(candidate.get("candidate_id", "") or ""),
        "candidate_version": str(candidate.get("candidate_version", "") or ""),
        "rule_id": str(candidate.get("rule_id", "") or ""),
        "rule_name": str(candidate.get("rule_name", "") or ""),
        "activation_mode": activation_mode,
        "overlay": overlay,
        "candidate_evidence": candidate.get("evidence_package", {}) if isinstance(candidate.get("evidence_package", {}), dict) else {},
    }
    policy = {
        "policy_id": str(base_policy.get("policy_id", store.get("policy_id", "")) or ""),
        "policy_version": policy_engine._next_policy_version(store),
        "source": "learning_candidate",
        "activation_mode": activation_mode,
        "status": "ready",
        "created_at": created_at,
        "activated_at": "",
        "based_on_version": str(base_policy.get("policy_version", "") or ""),
        "summary": summary,
        "writeback_lines": writeback_lines,
        "prompt_fragment": policy_engine._render_prompt_fragment(summary, writeback_lines),
        "evidence_hash": str(candidate.get("candidate_hash", "") or f"learn_{uuid.uuid4().hex[:8]}"),
        "evidence_package": evidence_package,
    }
    store.setdefault("policies", []).append(policy)
    policy_engine._write_policy_store(user_ctx, store)
    try:
        history_analysis.record_policy_version(user_ctx, policy)
        history_analysis.record_policy_event(
            user_ctx,
            policy_id=str(policy.get("policy_id", "") or ""),
            policy_version=str(policy.get("policy_version", "") or ""),
            event_type=f"learning_prepare_{activation_mode}",
            reason=summary,
            previous_version=str(base_policy.get("policy_version", "") or ""),
            payload=evidence_package,
        )
    except Exception:
        pass
    return {"ok": True, "created": True, "policy": policy}


def _record_learning_promotion_event(
    user_ctx,
    candidate: Dict[str, Any],
    *,
    event_type: str,
    target_policy_version: str,
    reason: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    history_analysis.record_learning_promotion(
        user_ctx,
        {
            "promotion_id": f"lp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
            "candidate_id": str(candidate.get("candidate_id", "") or ""),
            "candidate_version": str(candidate.get("candidate_version", "") or ""),
            "event_type": str(event_type or ""),
            "target_policy_version": str(target_policy_version or ""),
            "reason": str(reason or ""),
            "payload": payload or {},
            "created_at": created_at,
        },
    )


def gray_candidate(user_ctx, ident: str = "", target: str = "") -> Dict[str, Any]:
    target_result = _resolve_gray_target_user(user_ctx, target)
    if not target_result.get("ok", False):
        return {"ok": False, "message": str(target_result.get("message", "❌ 无法解析灰度目标账号"))}

    center = load_learning_center(user_ctx)
    candidate = _find_candidate(center, ident)
    if not candidate:
        return {"ok": False, "message": "❌ 未找到对应学习候选"}
    if str(candidate.get("last_evaluation_status", "") or "") in {"", LEARNING_EVAL_FAIL}:
        return {"ok": False, "message": "❌ 当前候选尚未通过离线评估，不能进入灰度"}
    if int(candidate.get("last_shadow_sample_size", 0) or 0) <= 0:
        return {"ok": False, "message": "❌ 当前候选还没有影子样本，不能直接进入灰度"}
    if str(candidate.get("last_shadow_status", "") or "") == LEARNING_SHADOW_FAIL:
        return {"ok": False, "message": "❌ 当前候选影子结果偏弱，暂不允许进入灰度"}

    target_ctx = target_result.get("user_ctx", user_ctx)
    target_name = str(getattr(getattr(target_ctx, "config", None), "name", "") or "")
    target_user_id = str(getattr(target_ctx, "user_id", "") or "")
    target_store = policy_engine.load_policy_store(target_ctx)
    active_policy = (
        policy_engine._find_policy_version(target_store, target_store.get("active_version", ""))
        or policy_engine._sorted_policies(target_store)[-1]
    )
    current_active_version = str(active_policy.get("policy_version", "") or "")

    ensure_result = _ensure_candidate_policy_version(
        target_ctx,
        candidate,
        activation_mode="gray",
        policy_field="gray_policy_version",
        summary_prefix="学习候选灰度",
        based_on_version=current_active_version,
    )
    if not ensure_result.get("ok", False):
        return {"ok": False, "message": str(ensure_result.get("message", "❌ 无法生成灰度策略版本"))}
    gray_policy = ensure_result.get("policy", {}) if isinstance(ensure_result.get("policy", {}), dict) else {}
    gray_version = str(gray_policy.get("policy_version", "") or "")
    result = policy_engine.activate_policy_version(
        target_ctx,
        gray_version,
        reason=f"学习候选灰度 {candidate.get('candidate_version', '')}",
    )
    if not result.get("ok", False):
        return result

    now_text = _now_text()
    candidate_id = str(candidate.get("candidate_id", "") or "")
    center["active_gray_candidate_id"] = candidate_id
    if str(center.get("active_shadow_candidate_id", "") or "") == candidate_id:
        center["active_shadow_candidate_id"] = ""
    center["last_promotion_event_at"] = now_text
    idx = _find_candidate_index(center, candidate_id)
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_GRAY
        center["candidates"][idx]["updated_at"] = now_text
        center["candidates"][idx]["gray_started_at"] = now_text
        center["candidates"][idx]["gray_target_user_id"] = target_user_id
        center["candidates"][idx]["gray_target_user_name"] = target_name
        center["candidates"][idx]["gray_policy_version"] = gray_version
        center["candidates"][idx]["gray_base_policy_version"] = current_active_version
        center["candidates"][idx]["rollback_policy_version"] = current_active_version
        center["candidates"][idx]["last_gray_summary"] = (
            f"灰度中：{target_name or target_user_id} -> {gray_version}（基线 {current_active_version}）"
        )
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass

    _record_learning_promotion_event(
        user_ctx,
        candidate,
        event_type="gray_start",
        target_policy_version=gray_version,
        reason="学习候选开始单账号灰度",
        payload={
            "target_user_id": target_user_id,
            "target_user_name": target_name,
            "base_policy_version": current_active_version,
            "gray_policy_version": gray_version,
            "shadow_status": str(candidate.get("last_shadow_status", "") or ""),
            "shadow_samples": int(candidate.get("last_shadow_sample_size", 0) or 0),
        },
    )
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "policy_version": gray_version,
        "message": (
            "🧠 已启动学习候选灰度\n\n"
            f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
            f"账号：{target_name or target_user_id}\n"
            f"策略版本：{gray_version}（基于 {current_active_version}）\n"
            "说明：当前只允许单账号灰度，且不覆盖硬风控。"
        ),
    }


def promote_candidate(user_ctx, ident: str = "") -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate(center, ident)
    if not candidate:
        return {"ok": False, "message": "❌ 未找到对应学习候选"}

    candidate_id = str(candidate.get("candidate_id", "") or "")
    if str(center.get("active_gray_candidate_id", "") or "") != candidate_id:
        return {"ok": False, "message": "❌ 当前候选未处于灰度运行中，不能直接转正"}

    gray_policy_version = str(candidate.get("gray_policy_version", "") or "")
    if not gray_policy_version:
        return {"ok": False, "message": "❌ 当前候选还没有灰度策略版本，不能转正"}

    rollback_version = str(
        candidate.get("rollback_policy_version", "")
        or candidate.get("gray_base_policy_version", "")
        or candidate.get("based_on_policy_version", "")
        or ""
    )
    ensure_result = _ensure_candidate_policy_version(
        user_ctx,
        candidate,
        activation_mode="baseline",
        policy_field="promoted_policy_version",
        summary_prefix="学习候选转正",
        based_on_version=gray_policy_version,
    )
    if not ensure_result.get("ok", False):
        return {"ok": False, "message": str(ensure_result.get("message", "❌ 无法生成转正策略版本"))}
    promoted_policy = ensure_result.get("policy", {}) if isinstance(ensure_result.get("policy", {}), dict) else {}
    promoted_version = str(promoted_policy.get("policy_version", "") or "")
    result = policy_engine.activate_policy_version(
        user_ctx,
        promoted_version,
        reason=f"学习候选转正 {candidate.get('candidate_version', '')}",
    )
    if not result.get("ok", False):
        return result

    now_text = _now_text()
    center["active_gray_candidate_id"] = ""
    center["promoted_candidate_id"] = candidate_id
    center["last_promotion_event_at"] = now_text
    idx = _find_candidate_index(center, candidate_id)
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_PROMOTED
        center["candidates"][idx]["updated_at"] = now_text
        center["candidates"][idx]["promoted_at"] = now_text
        center["candidates"][idx]["promoted_policy_version"] = promoted_version
        center["candidates"][idx]["rollback_policy_version"] = rollback_version
        center["candidates"][idx]["last_gray_summary"] = (
            f"已转正：{promoted_version}（可回滚到 {rollback_version or '-' }）"
        )
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass

    _record_learning_promotion_event(
        user_ctx,
        candidate,
        event_type="promote",
        target_policy_version=promoted_version,
        reason="学习候选人工确认转正",
        payload={
            "gray_policy_version": gray_policy_version,
            "promoted_policy_version": promoted_version,
            "rollback_policy_version": rollback_version,
        },
    )
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "policy_version": promoted_version,
        "message": (
            "🧠 已转正学习候选\n\n"
            f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
            f"正式策略：{promoted_version}\n"
            f"回滚锚点：{rollback_version or '-'}"
        ),
    }


def rollback_candidate(user_ctx) -> Dict[str, Any]:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate_strict(center, str(center.get("active_gray_candidate_id", "") or ""))
    if not candidate:
        candidate = _find_candidate_strict(center, str(center.get("promoted_candidate_id", "") or ""))
    if not candidate:
        for item in reversed(_sorted_candidates(center)):
            if str(item.get("status", "") or "") in {LEARNING_STATUS_GRAY, LEARNING_STATUS_PROMOTED}:
                candidate = item
                break
    if not candidate:
        return {"ok": False, "message": "❌ 当前没有可回滚的学习候选"}

    rollback_version = str(
        candidate.get("rollback_policy_version", "")
        or candidate.get("gray_base_policy_version", "")
        or candidate.get("based_on_policy_version", "")
        or ""
    )
    if not rollback_version:
        return {"ok": False, "message": "❌ 当前候选缺少回滚锚点，无法执行回滚"}

    current_policy_version = str(user_ctx.state.runtime.get("policy_active_version", "") or "")
    result = policy_engine.activate_policy_version(
        user_ctx,
        rollback_version,
        reason=f"学习候选回滚 {candidate.get('candidate_version', '')}",
    )
    if not result.get("ok", False):
        return result

    now_text = _now_text()
    candidate_id = str(candidate.get("candidate_id", "") or "")
    if str(center.get("active_gray_candidate_id", "") or "") == candidate_id:
        center["active_gray_candidate_id"] = ""
    if str(center.get("promoted_candidate_id", "") or "") == candidate_id:
        center["promoted_candidate_id"] = ""
    center["last_promotion_event_at"] = now_text
    idx = _find_candidate_index(center, candidate_id)
    if idx >= 0:
        center["candidates"][idx]["status"] = LEARNING_STATUS_ROLLED_BACK
        center["candidates"][idx]["updated_at"] = now_text
        center["candidates"][idx]["rolled_back_at"] = now_text
        center["candidates"][idx]["rolled_back_to_version"] = rollback_version
        center["candidates"][idx]["last_gray_summary"] = (
            f"已回滚：{current_policy_version or '-'} -> {rollback_version}"
        )
        try:
            history_analysis.record_learning_candidate(user_ctx, center["candidates"][idx])
        except Exception:
            pass

    _record_learning_promotion_event(
        user_ctx,
        candidate,
        event_type="rollback",
        target_policy_version=rollback_version,
        reason="学习候选手动回滚",
        payload={
            "from_policy_version": current_policy_version,
            "rollback_policy_version": rollback_version,
        },
    )
    _write_learning_center(user_ctx, center)
    _update_runtime_learning_snapshot(user_ctx, center)
    return {
        "ok": True,
        "policy_version": rollback_version,
        "message": (
            "🧠 已回滚学习候选\n\n"
            f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
            f"回滚到：{rollback_version}"
        ),
    }


def build_learning_evaluation_text(candidate: Dict[str, Any], evaluation: Dict[str, Any]) -> str:
    metrics = evaluation.get("metrics", {}) if isinstance(evaluation.get("metrics", {}), dict) else {}
    status = str(evaluation.get("status", LEARNING_EVAL_WATCH) or LEARNING_EVAL_WATCH)
    status_text = {
        LEARNING_EVAL_PASS: "通过",
        LEARNING_EVAL_WATCH: "观察",
        LEARNING_EVAL_FAIL: "偏弱",
    }.get(status, status)
    return (
        "🧠 学习候选离线评估\n\n"
        f"候选：{candidate.get('candidate_version', '')} | {candidate.get('rule_name', '')}\n"
        f"状态：{status_text}\n"
        f"样本：{int(metrics.get('sample_size', 0) or 0)} | 覆盖率：{float(metrics.get('coverage_rate', 0.0) or 0.0) * 100:.1f}%\n"
        f"信号命中：{float(metrics.get('signal_hit_rate', 0.0) or 0.0) * 100:.1f}% | 实盘命中：{float(metrics.get('bet_hit_rate', 0.0) or 0.0) * 100:.1f}%\n"
        f"观望率：{float(metrics.get('observe_rate', 0.0) or 0.0) * 100:.1f}% | 限档率：{float(metrics.get('tier_cap_rate', 0.0) or 0.0) * 100:.1f}%\n"
        f"收益：{int(metrics.get('total_pnl', 0) or 0):+,} | 基线：{int(metrics.get('baseline_total_pnl', 0) or 0):+,} | Δ {int(metrics.get('delta_pnl', 0) or 0):+,}\n"
        f"回撤：{int(metrics.get('max_drawdown', 0) or 0):,} | 基线：{int(metrics.get('baseline_drawdown', 0) or 0):,} | 改善 {int(metrics.get('delta_drawdown', 0) or 0):+,}\n"
        f"稳定性：{float(metrics.get('regime_stability', 0.0) or 0.0) * 100:.1f}% | 评分：{float(metrics.get('score_total', 0.0) or 0.0):.2f}\n"
        f"摘要：{_summarize_evaluation(candidate, {'status': status, **metrics})}"
    )


def build_learning_overview_text(user_ctx) -> str:
    center = load_learning_center(user_ctx)
    candidates = _sorted_candidates(center)
    latest = candidates[-1] if candidates else {}
    active_shadow = _find_candidate_strict(center, str(center.get("active_shadow_candidate_id", "") or ""))
    active_gray = _find_candidate_strict(center, str(center.get("active_gray_candidate_id", "") or ""))
    promoted = _find_candidate_strict(center, str(center.get("promoted_candidate_id", "") or ""))
    active_policy = policy_engine.build_policy_prompt_context(user_ctx)
    status_counter: Dict[str, int] = {}
    for item in candidates:
        status = str(item.get("status", LEARNING_STATUS_GENERATED) or LEARNING_STATUS_GENERATED)
        status_counter[status] = status_counter.get(status, 0) + 1
    status_text = " / ".join(f"{key}:{value}" for key, value in sorted(status_counter.items())) or "无"
    latest_label = str(latest.get("candidate_version", "") or latest.get("candidate_id", "") or "-")
    latest_summary = str(latest.get("summary", "") or "-")
    active_shadow_text = active_shadow.get("candidate_version", "-") if active_shadow else "-"
    active_gray_text = active_gray.get("candidate_version", "-") if active_gray else "-"
    promoted_text = promoted.get("candidate_version", "-") if promoted else "-"
    lines = [
        "🧠 受控自学习中心",
        f"当前策略：{active_policy.get('policy_id', '')}@{active_policy.get('policy_version', '')} ({active_policy.get('policy_mode', '')})",
        f"学习状态：候选 {len(candidates)} 个 | 影子 {active_shadow_text} | 灰度 {active_gray_text} | 最近转正 {promoted_text}",
        f"状态分布：{status_text}",
        (
            f"最近动作：生成 {center.get('last_generated_at', '') or '-'} | "
            f"影子 {center.get('last_shadow_recorded_at', '') or '-'} | "
            f"晋退 {center.get('last_promotion_event_at', '') or '-'}"
        ),
        f"最近候选：{latest_label} | {latest_summary}",
        f"最近评估：{latest.get('last_evaluation_status', '') or '-'} / {latest.get('last_score_total', '-')}",
        "",
        "建议：先看 `watch learn`，再用 `learn list` / `learn show <id|cX>` 深看。",
        "命令：`learn` / `learn gen` / `learn list` / `learn show <id|cX>` / `learn eval [id|cX]`",
        "影子：`learn shadow` / `learn shadow <id|cX> on` / `learn shadow off`",
        "灰度：`learn gray <id|cX> [当前账号名|ID]` / `learn promote <id|cX>` / `learn rollback`",
    ]
    return "\n".join(lines)


def build_learning_list_text(user_ctx) -> str:
    center = load_learning_center(user_ctx)
    candidates = _sorted_candidates(center)
    if not candidates:
        return "🧠 学习候选列表\n\n暂无学习候选\n\n生成：`learn gen`"
    lines = ["🧠 学习候选列表", ""]
    for item in candidates[-10:]:
        score_text = item.get("last_score_total", "-")
        shadow_text = ""
        shadow_samples = int(item.get("last_shadow_sample_size", 0) or 0)
        if shadow_samples > 0:
            shadow_text = f" | shadow {shadow_samples}/{item.get('last_shadow_status', '-')}"
        lines.append(
            f"- {item.get('candidate_version', '')} | {item.get('rule_name', '')} | {item.get('status', '')} | score {score_text}{shadow_text} | {item.get('summary', '')}"
        )
    return "\n".join(lines)


def build_learning_detail_text(user_ctx, ident: str = "") -> str:
    center = load_learning_center(user_ctx)
    candidate = _find_candidate(center, ident)
    if not candidate:
        return "🧠 学习候选详情\n\n未找到对应候选"
    overlay = candidate.get("overlay", {}) if isinstance(candidate.get("overlay", {}), dict) else {}
    evidence = candidate.get("evidence_package", {}) if isinstance(candidate.get("evidence_package", {}), dict) else {}
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    prompt_lines = overlay.get("prompt_lines", []) if isinstance(overlay.get("prompt_lines", []), list) else []
    lines = [
        "🧠 学习候选详情",
        "",
        f"候选ID：{candidate.get('candidate_id', '')}",
        f"候选版本：{candidate.get('candidate_version', '')}",
        f"状态：{candidate.get('status', '')}",
        f"来源：{candidate.get('source', '')}",
        f"规则：{candidate.get('rule_name', '')} ({candidate.get('rule_id', '')})",
        f"基于策略：{candidate.get('based_on_policy_id', '')}@{candidate.get('based_on_policy_version', '')}",
        f"创建时间：{candidate.get('created_at', '')}",
        f"最近评估：{candidate.get('last_evaluation_status', '') or '-'} / {candidate.get('last_score_total', '-')}",
        f"影子状态：{candidate.get('last_shadow_status', '-') or '-'} | 样本 {int(candidate.get('last_shadow_sample_size', 0) or 0)}",
        f"灰度状态：{candidate.get('gray_policy_version', '-') or '-'} | 转正版本 {candidate.get('promoted_policy_version', '-') or '-'}",
        f"摘要：{candidate.get('summary', '')}",
        f"标签：{', '.join(candidate.get('tags', []) or []) or '-'}",
        "",
        f"当前盘面：{evidence.get('current_regime', '-')}",
        f"相似样本：{similar.get('similar_count', 0)} | 强度 {similar.get('evidence_strength', 'insufficient')} | 历史建议 {similar.get('recommended_tier_cap', '') or '-'}",
        f"24h：样本 {overview.get('settled_count', 0)} | 收益 {int(overview.get('pnl_total', 0) or 0):+,} | 回撤 {int(overview.get('max_drawdown', 0) or 0):,}",
        "",
        "候选覆盖：",
    ]
    if prompt_lines:
        lines.extend(f"- {line}" for line in prompt_lines)
    else:
        lines.append("- 暂无覆盖规则")
    lines.extend(
        [
            "",
            "Prompt 片段：",
            str(candidate.get("prompt_fragment", "") or "-"),
        ]
    )
    if candidate.get("last_evaluation_summary"):
        lines.extend(["", "离线评估摘要：", str(candidate.get("last_evaluation_summary", "") or "-")])
    if candidate.get("last_shadow_summary"):
        lines.extend(["", "影子验证摘要：", str(candidate.get("last_shadow_summary", "") or "-")])
    if candidate.get("last_gray_summary"):
        lines.extend(["", "灰度/转正摘要：", str(candidate.get("last_gray_summary", "") or "-")])
    return "\n".join(lines)


def build_learning_shadow_text(user_ctx, ident: str = "") -> str:
    center = load_learning_center(user_ctx)
    active_candidate = _find_candidate_strict(center, str(center.get("active_shadow_candidate_id", "") or ""))
    target = _find_candidate(center, ident) if ident else active_candidate
    lines = [
        "🧠 学习候选影子验证",
        "",
        f"当前运行：{active_candidate.get('candidate_version', '-') if active_candidate else '-'}",
        f"最近记录：{center.get('last_shadow_recorded_at', '') or '-'}",
    ]
    if not target:
        lines.extend(
            [
                "",
                "当前没有运行中的影子候选。",
                "启用：`learn shadow <id|cX> on`",
                "关闭：`learn shadow off`",
            ]
        )
        return "\n".join(lines)

    metrics = _shadow_metrics(user_ctx, str(target.get("candidate_id", "") or ""))
    running = active_candidate and str(active_candidate.get("candidate_id", "") or "") == str(target.get("candidate_id", "") or "")
    lines.extend(
        [
            "",
            f"候选：{target.get('candidate_version', '')} | {target.get('rule_name', '')}",
            f"运行状态：{'运行中' if running else '未运行'}",
            f"影子结论：{_shadow_status_text(str(metrics.get('status', LEARNING_SHADOW_WATCH) or LEARNING_SHADOW_WATCH))}",
            f"样本：{int(metrics.get('sample_size', 0) or 0)} | 差异：{int(metrics.get('diff_count', 0) or 0)} | 相同：{int(metrics.get('same_count', 0) or 0)}",
            f"保守：{int(metrics.get('conservative_count', 0) or 0)} | 激进：{int(metrics.get('aggressive_count', 0) or 0)} | 方向差异：{int(metrics.get('direction_diff_count', 0) or 0)}",
            f"候选命中：{float(metrics.get('candidate_signal_hit_rate', 0.0) or 0.0) * 100:.1f}% | 候选实盘命中：{float(metrics.get('candidate_bet_hit_rate', 0.0) or 0.0) * 100:.1f}%",
            f"收益：{int(metrics.get('candidate_total_pnl', 0) or 0):+,} | 基线：{int(metrics.get('base_total_pnl', 0) or 0):+,} | Δ {int(metrics.get('delta_pnl', 0) or 0):+,}",
            f"回撤：{int(metrics.get('candidate_drawdown', 0) or 0):,} | 基线：{int(metrics.get('base_drawdown', 0) or 0):,} | 改善 {int(metrics.get('delta_drawdown', 0) or 0):+,}",
            f"摘要：{metrics.get('summary', '暂无影子样本') or '暂无影子样本'}",
            "",
            "命令：`learn shadow <id|cX> on` / `learn gray <id|cX>` / `learn shadow off`",
        ]
    )
    return "\n".join(lines)


def build_learning_pending_text(subcmd: str) -> str:
    mapping = {
        "gray": "单账号灰度",
        "promote": "转正",
        "rollback": "回滚",
    }
    stage_text = mapping.get(str(subcmd or "").strip().lower(), "后续阶段")
    return (
        f"🧠 `{subcmd}` 尚未实现\n\n"
        f"该命令属于 {stage_text}，当前分支尚未提供对应实现。"
    )
