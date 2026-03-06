from __future__ import annotations

from typing import Any, Dict

import constants
import risk_control


REGIME_TIER_CAP = {
    "延续盘": "",
    "衰竭盘": "yc10",
    "反转盘": "yc5",
    "震荡盘": "yc5",
    "混乱盘": "yc1",
}


def clear_dynamic_decision(rt: Dict[str, Any]) -> None:
    rt["current_dynamic_base_tier"] = ""
    rt["current_dynamic_tier"] = ""
    rt["current_dynamic_reason"] = ""
    rt["current_dynamic_action_text"] = ""
    rt["current_dynamic_floor_tier"] = ""
    rt["current_dynamic_ceiling_tier"] = ""


def reset_dynamic_sequence(rt: Dict[str, Any]) -> None:
    clear_dynamic_decision(rt)
    rt["dynamic_sequence_start_tier"] = ""


def _normalize_tier_name(name: Any) -> str:
    value = str(name or "").strip().lower()
    return value if value in risk_control.TIER_ORDER else ""


def _tier_rank(name: str) -> int:
    try:
        return risk_control.TIER_ORDER.index(name)
    except ValueError:
        return -1


def _stricter_tier(current: str, fallback: str) -> str:
    return risk_control.stricter_tier_cap(current, fallback)


def _looser_tier(current: str, fallback: str) -> str:
    current = _normalize_tier_name(current)
    fallback = _normalize_tier_name(fallback)
    if not current:
        return fallback
    if not fallback:
        return current
    return current if _tier_rank(current) >= _tier_rank(fallback) else fallback


def _tier_from_amount(amount: Any) -> str:
    try:
        amount_value = int(amount or 0)
    except (TypeError, ValueError):
        amount_value = 0
    matched_tier = ""
    matched_amount = -1
    for tier_name in risk_control.TIER_ORDER:
        tier_amount = int(risk_control.TIER_AMOUNT.get(tier_name, 0) or 0)
        if tier_amount <= 0:
            continue
        if amount_value >= tier_amount and tier_amount > matched_amount:
            matched_tier = tier_name
            matched_amount = tier_amount
    if matched_tier:
        return matched_tier
    return "yc05"


def _base_tier(rt: Dict[str, Any]) -> str:
    current_preset = _normalize_tier_name(rt.get("current_preset_name", ""))
    if current_preset:
        return current_preset
    return _tier_from_amount(rt.get("initial_amount", constants.MIN_BET_AMOUNT))


def _sequence_floor_tier(rt: Dict[str, Any], base_tier: str) -> str:
    start_tier = _normalize_tier_name(rt.get("dynamic_sequence_start_tier", ""))
    if start_tier:
        return start_tier
    if int(rt.get("lose_count", 0) or 0) > 0:
        return base_tier
    return ""


def evaluate_dynamic_bet(snapshot: Dict[str, Any], rt: Dict[str, Any]) -> Dict[str, Any]:
    base_tier = _base_tier(rt)
    fk1_cap = _normalize_tier_name(rt.get("current_fk1_tier_cap", ""))
    ceiling_tier = _stricter_tier(base_tier, fk1_cap) if fk1_cap else base_tier
    if not ceiling_tier:
        ceiling_tier = base_tier or "yc1"

    regime_label = str(snapshot.get("regime_label", "震荡盘") or "震荡盘")
    similar = snapshot.get("similar_cases", {}) if isinstance(snapshot.get("similar_cases", {}), dict) else {}
    temperature = str(snapshot.get("recent_temperature", {}).get("level", "normal") or "normal")
    tiers = similar.get("tiers", {}) if isinstance(similar.get("tiers", {}), dict) else {}

    suggested_tier = ceiling_tier
    reasons = [f"基准预设 {base_tier}"]
    if fk1_cap and fk1_cap != base_tier:
        reasons.append(f"盘面风控上限 {fk1_cap}")

    regime_cap = _normalize_tier_name(REGIME_TIER_CAP.get(regime_label, ""))
    if regime_cap:
        new_tier = _stricter_tier(suggested_tier, regime_cap)
        if new_tier != suggested_tier:
            suggested_tier = new_tier
            reasons.append(f"{regime_label} 默认收敛至 {regime_cap}")

    recommended_cap = str(similar.get("recommended_tier_cap", "") or "")
    if recommended_cap == "observe":
        suggested_tier = _stricter_tier(suggested_tier, "yc1")
        reasons.append("相似历史整体偏弱，仅保留最低档")
    else:
        history_cap = _normalize_tier_name(recommended_cap)
        if history_cap:
            new_tier = _stricter_tier(suggested_tier, history_cap)
            if new_tier != suggested_tier:
                suggested_tier = new_tier
                reasons.append(f"历史证据建议上限 {history_cap}")

    evidence_strength = str(similar.get("evidence_strength", "insufficient") or "insufficient")
    if evidence_strength != "strong" and _tier_rank(suggested_tier) > _tier_rank("yc20"):
        suggested_tier = _stricter_tier(suggested_tier, "yc20")
        reasons.append("历史证据未达强证据，高档位先收至 yc20")

    high_stats = tiers.get("high", {}) if isinstance(tiers.get("high", {}), dict) else {}
    if high_stats and float(high_stats.get("avg_pnl", 0.0) or 0.0) < 0 and _tier_rank(suggested_tier) > _tier_rank("yc10"):
        suggested_tier = _stricter_tier(suggested_tier, "yc10")
        reasons.append("高档历史收益为负，收至 yc10")

    low_stats = tiers.get("low", {}) if isinstance(tiers.get("low", {}), dict) else {}
    if low_stats and float(low_stats.get("avg_pnl", 0.0) or 0.0) <= 0 and regime_label in {"反转盘", "震荡盘", "混乱盘"}:
        suggested_tier = _stricter_tier(suggested_tier, "yc1")
        reasons.append("低档历史也偏弱，进一步收至 yc1")

    lose_count = int(rt.get("lose_count", 0) or 0)
    if temperature == "cold":
        if _tier_rank(suggested_tier) > _tier_rank("yc10"):
            suggested_tier = _stricter_tier(suggested_tier, "yc10")
            reasons.append("近期实盘偏冷，收至 yc10")
    if lose_count >= 3 and _tier_rank(suggested_tier) > _tier_rank("yc10"):
        suggested_tier = _stricter_tier(suggested_tier, "yc10")
        reasons.append("连输已到 3 次以上，收至 yc10")
    elif lose_count >= 1 and _tier_rank(suggested_tier) > _tier_rank("yc20"):
        suggested_tier = _stricter_tier(suggested_tier, "yc20")
        reasons.append("连输进行中，先不放高于 yc20")

    floor_tier = _sequence_floor_tier(rt, base_tier)
    applied_tier = suggested_tier
    floor_locked = False
    if lose_count > 0 and floor_tier:
        locked_tier = _looser_tier(applied_tier, floor_tier)
        if locked_tier != applied_tier:
            applied_tier = locked_tier
            floor_locked = True
            reasons.append(f"连输中保持首注档位下限 {floor_tier}")

    if not applied_tier:
        applied_tier = ceiling_tier or base_tier or "yc1"

    adjusted = applied_tier != base_tier
    if floor_locked:
        action_text = f"动态档位保持连输首注下限 {applied_tier}"
    elif adjusted:
        action_text = f"动态档位调整为 {applied_tier}（基准 {base_tier}）"
    else:
        action_text = f"动态档位保持 {applied_tier}"

    return {
        "enabled": True,
        "base_tier": base_tier,
        "ceiling_tier": ceiling_tier,
        "floor_tier": floor_tier,
        "suggested_tier": suggested_tier,
        "applied_tier": applied_tier,
        "adjusted": adjusted,
        "floor_locked": floor_locked,
        "reason_text": "；".join(item for item in reasons if item),
        "action_text": action_text,
        "evidence_strength": evidence_strength,
    }


def build_dynamic_summary(result: Dict[str, Any]) -> str:
    applied_tier = str(result.get("applied_tier", "") or "")
    base_tier = str(result.get("base_tier", "") or "")
    action_text = str(result.get("action_text", "") or "")
    reason_text = str(result.get("reason_text", "") or "")
    if not applied_tier:
        return ""
    if applied_tier == base_tier:
        return f"⚖️ 动态档位：{applied_tier}\n🧠 依据：{reason_text or action_text}"
    return f"⚖️ 动态档位：{base_tier} -> {applied_tier}\n🧠 依据：{reason_text or action_text}"
