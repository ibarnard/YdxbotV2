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
            "说明：当前仅完成 H1/H2（候选中心 / 规则生成）。",
            "后续阶段：`learn eval` / `learn shadow` / `learn gray` / `learn promote` / `learn rollback`",
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
    active_policy = policy_engine.build_policy_prompt_context(user_ctx)
    status_counter: Dict[str, int] = {}
    for item in candidates:
        status = str(item.get("status", LEARNING_STATUS_GENERATED) or LEARNING_STATUS_GENERATED)
        status_counter[status] = status_counter.get(status, 0) + 1
    status_text = " / ".join(f"{key}:{value}" for key, value in sorted(status_counter.items())) or "无"
    lines = [
        "🧠 受控自学习中心",
        "",
        "当前实现：H1/H2/H3 已启用（候选中心 / 规则生成 / 离线评估）",
        f"当前策略：{active_policy.get('policy_id', '')}@{active_policy.get('policy_version', '')} ({active_policy.get('policy_mode', '')})",
        f"候选总数：{len(candidates)}",
        f"状态分布：{status_text}",
        f"最近生成：{center.get('last_generated_at', '') or '-'}",
        f"最近候选：{latest.get('candidate_id', '') or '-'}",
        f"候选摘要：{latest.get('summary', '') or '-'}",
        f"最近评估：{latest.get('last_evaluation_status', '') or '-'} / {latest.get('last_score_total', '-')}",
        "",
        "命令：`learn` / `learn gen` / `learn list` / `learn show <id|cX>` / `learn eval [id|cX]`",
        "后续：`learn shadow` / `learn gray` / `learn promote` / `learn rollback`",
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
        lines.append(
            f"- {item.get('candidate_version', '')} | {item.get('rule_name', '')} | {item.get('status', '')} | score {score_text} | {item.get('summary', '')}"
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
    return "\n".join(lines)


def build_learning_pending_text(subcmd: str) -> str:
    mapping = {
        "shadow": "H4 影子验证器",
        "gray": "H5 单账号灰度",
        "promote": "H5 转正",
        "rollback": "H5 回滚",
    }
    stage_text = mapping.get(str(subcmd or "").strip().lower(), "后续阶段")
    return (
        f"🧠 `{subcmd}` 尚未实现\n\n"
        f"该命令属于 {stage_text}，当前分支已完成 H1/H2，接下来会按顺序继续实现。"
    )
