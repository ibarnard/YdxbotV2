from __future__ import annotations

from typing import Any, Dict, List, Tuple

import constants


FK1 = "fk1"
FK2 = "fk2"
FK3 = "fk3"
FUND = "fund"

FK_LABELS = {
    FK1: "盘面风控",
    FK2: "入场风控",
    FK3: "连输风控",
    FUND: "资金风控",
}

TIER_ORDER = ["yc05", "yc1", "yc5", "yc10", "yc20", "yc50", "yc100", "yc200"]
TIER_AMOUNT = {
    name: int(values[6])
    for name, values in constants.PRESETS.items()
    if isinstance(values, (list, tuple)) and len(values) >= 7
}


def _switch_label(enabled: bool) -> str:
    return "ON" if enabled else "OFF"


def _to_bool_switch(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "on", "yes", "y", "enable", "enabled", "开", "开启"}:
            return True
        if normalized in {"0", "false", "off", "no", "n", "disable", "disabled", "关", "关闭"}:
            return False
    return bool(default)


def normalize_fk_switches(rt: Dict[str, Any], apply_default: bool = False) -> Dict[str, bool]:
    legacy_base = _to_bool_switch(rt.get("risk_base_enabled", True), True)
    legacy_deep = _to_bool_switch(rt.get("risk_deep_enabled", True), True)
    legacy_base_default = _to_bool_switch(rt.get("risk_base_default_enabled", legacy_base), legacy_base)
    legacy_deep_default = _to_bool_switch(rt.get("risk_deep_default_enabled", legacy_deep), legacy_deep)

    fk1_enabled = _to_bool_switch(rt.get("fk1_enabled", legacy_base), legacy_base)
    fk2_enabled = _to_bool_switch(rt.get("fk2_enabled", legacy_deep), legacy_deep)
    fk3_enabled = _to_bool_switch(rt.get("fk3_enabled", legacy_deep), legacy_deep)
    fk1_default = _to_bool_switch(rt.get("fk1_default_enabled", legacy_base_default), legacy_base_default)
    fk2_default = _to_bool_switch(rt.get("fk2_default_enabled", legacy_deep_default), legacy_deep_default)
    fk3_default = _to_bool_switch(rt.get("fk3_default_enabled", legacy_deep_default), legacy_deep_default)

    # Legacy compatibility: if old switches are explicitly turned off, keep fk switches off too.
    if not legacy_base and _to_bool_switch(rt.get("fk1_enabled", True), True):
        fk1_enabled = False
    if not legacy_deep:
        if _to_bool_switch(rt.get("fk2_enabled", True), True):
            fk2_enabled = False
        if _to_bool_switch(rt.get("fk3_enabled", True), True):
            fk3_enabled = False
    if not legacy_base_default and _to_bool_switch(rt.get("fk1_default_enabled", True), True):
        fk1_default = False
    if not legacy_deep_default:
        if _to_bool_switch(rt.get("fk2_default_enabled", True), True):
            fk2_default = False
        if _to_bool_switch(rt.get("fk3_default_enabled", True), True):
            fk3_default = False

    if apply_default:
        fk1_enabled = fk1_default
        fk2_enabled = fk2_default
        fk3_enabled = fk3_default

    rt["fk1_enabled"] = fk1_enabled
    rt["fk2_enabled"] = fk2_enabled
    rt["fk3_enabled"] = fk3_enabled
    rt["fk1_default_enabled"] = fk1_default
    rt["fk2_default_enabled"] = fk2_default
    rt["fk3_default_enabled"] = fk3_default

    rt["risk_base_enabled"] = fk1_enabled
    rt["risk_base_default_enabled"] = fk1_default
    rt["risk_deep_enabled"] = fk2_enabled
    rt["risk_deep_default_enabled"] = fk2_default

    return {
        "fk1_enabled": fk1_enabled,
        "fk2_enabled": fk2_enabled,
        "fk3_enabled": fk3_enabled,
        "fk1_default_enabled": fk1_default,
        "fk2_default_enabled": fk2_default,
        "fk3_default_enabled": fk3_default,
    }


def apply_account_fk_default_mode(rt: Dict[str, Any]) -> Dict[str, bool]:
    return normalize_fk_switches(rt, apply_default=True)


def build_fk_state_text(rt: Dict[str, Any], include_usage: bool = True) -> str:
    modes = normalize_fk_switches(rt, apply_default=False)
    message = (
        "🛡 当前风控开关（账户默认模式）\n"
        f"- fk 1 盘面风控：{_switch_label(modes['fk1_enabled'])}（默认：{_switch_label(modes['fk1_default_enabled'])}）\n"
        f"- fk 2 入场风控：{_switch_label(modes['fk2_enabled'])}（默认：{_switch_label(modes['fk2_default_enabled'])}）\n"
        f"- fk 3 连输风控：{_switch_label(modes['fk3_enabled'])}（默认：{_switch_label(modes['fk3_default_enabled'])}）\n"
        "- 资金风控：常开\n\n"
        "说明：脚本重启后会按账户默认模式恢复。"
    )
    if include_usage:
        message += "\n用法：`fk` / `fk 1 on|off` / `fk 2 on|off` / `fk 3 on|off`"
    return message


def _tier_rank(name: str) -> int:
    try:
        return TIER_ORDER.index(name)
    except ValueError:
        return -1


def stricter_tier_cap(current_cap: str, fallback_cap: str) -> str:
    if not current_cap:
        return fallback_cap
    if not fallback_cap:
        return current_cap
    current_rank = _tier_rank(current_cap)
    fallback_rank = _tier_rank(fallback_cap)
    if current_rank < 0:
        return fallback_cap
    if fallback_rank < 0:
        return current_cap
    return current_cap if current_rank <= fallback_rank else fallback_cap


def clamp_bet_amount_by_tier_cap(bet_amount: int, tier_cap: str) -> Tuple[int, str]:
    if bet_amount <= 0 or not tier_cap:
        return int(bet_amount), ""
    cap_amount = int(TIER_AMOUNT.get(tier_cap, 0) or 0)
    if cap_amount <= 0:
        return int(bet_amount), ""
    return min(int(bet_amount), cap_amount), tier_cap


def _default_action_for_regime(regime_label: str) -> Tuple[str, str]:
    if regime_label == "延续盘":
        return "allow", ""
    if regime_label == "衰竭盘":
        return "cap", "yc10"
    if regime_label == "反转盘":
        return "cap", "yc1"
    if regime_label == "震荡盘":
        return "cap", "yc5"
    return "cap", "yc1"


def _history_profile(snapshot: Dict[str, Any]) -> str:
    similar = snapshot.get("similar_cases", {})
    count = int(similar.get("similar_count", 0) or 0)
    evidence_strength = str(similar.get("evidence_strength", "insufficient") or "insufficient")
    tiers = similar.get("tiers", {}) if isinstance(similar.get("tiers", {}), dict) else {}
    low = tiers.get("low", {})
    mid = tiers.get("mid", {})
    high = tiers.get("high", {})
    weighted_hit_rate = float(similar.get("weighted_signal_hit_rate", 0.0) or 0.0)

    if count < 15 or evidence_strength == "insufficient":
        return "insufficient"
    if count >= 40:
        if (
            weighted_hit_rate < 0.49
            and (not low or float(low.get("avg_pnl", 0.0)) <= 0)
            and (not mid or float(mid.get("avg_pnl", 0.0)) <= 0)
            and (not high or float(high.get("avg_pnl", 0.0)) <= 0)
        ):
            return "overall_negative"
        if high and (
            float(high.get("avg_pnl", 0.0)) < 0
            or (low and float(high.get("win_rate", 0.0)) + 0.03 < float(low.get("win_rate", 0.0)))
        ):
            return "high_risk"
        if low and float(low.get("avg_pnl", 0.0)) > 0 and weighted_hit_rate >= 0.50:
            return "low_ok"
        if weighted_hit_rate >= 0.54 and (not high or float(high.get("avg_pnl", 0.0)) >= 0):
            return "stable"
    if low and float(low.get("avg_pnl", 0.0)) > 0:
        return "low_ok"
    return "weak"


def _apply_temperature_downgrade(action: str, tier_cap: str, temperature_level: str) -> Tuple[str, str]:
    if temperature_level == "very_cold":
        return "observe", ""
    if temperature_level != "cold":
        return action, tier_cap
    if action == "allow":
        return "cap", stricter_tier_cap(tier_cap, "yc10")
    if action == "cap":
        stricter = stricter_tier_cap(tier_cap, "yc5")
        if stricter in {"yc05", "yc1"}:
            return "observe", ""
        return "cap", stricter
    return action, tier_cap


def evaluate_fk1(snapshot: Dict[str, Any], rt: Dict[str, Any]) -> Dict[str, Any]:
    normalize_fk_switches(rt, apply_default=False)
    if not bool(rt.get("fk1_enabled", True)):
        return {
            "layer": FK1,
            "enabled": False,
            "action": "allow",
            "tier_cap": "",
            "reason_code": "fk1_disabled",
            "reason_text": "盘面风控已关闭",
            "action_text": "盘面风控关闭，按当前策略执行",
        }

    regime_label = str(snapshot.get("regime_label", "震荡盘") or "震荡盘")
    similar = snapshot.get("similar_cases", {})
    temperature = snapshot.get("recent_temperature", {})
    default_action, default_cap = _default_action_for_regime(regime_label)
    history_profile = _history_profile(snapshot)

    action = default_action
    tier_cap = default_cap
    reasons: List[str] = [f"当前为{regime_label}"]

    if history_profile == "overall_negative":
        action = "observe"
        tier_cap = ""
        reasons.append("相似历史整体表现偏弱")
    elif history_profile == "high_risk":
        action = "cap"
        tier_cap = stricter_tier_cap(tier_cap, "yc5")
        reasons.append("相似历史高档位回撤偏大")
    elif history_profile == "low_ok":
        if regime_label == "混乱盘":
            action = "cap"
            tier_cap = "yc1"
        elif regime_label in {"反转盘", "震荡盘"}:
            action = "cap"
            tier_cap = stricter_tier_cap(tier_cap, "yc5")
        reasons.append("相似历史低档位可做")
    elif history_profile == "stable" and regime_label == "延续盘":
        action = "allow"
        tier_cap = ""
        reasons.append("相似历史表现稳定")
    elif history_profile == "insufficient":
        reasons.append("相似历史样本不足，按盘面默认动作")
    else:
        reasons.append("相似历史仅提供弱证据")

    action, tier_cap = _apply_temperature_downgrade(action, tier_cap, str(temperature.get("level", "normal") or "normal"))
    if str(temperature.get("level", "normal")) == "cold":
        reasons.append("近期实盘偏冷，动作降一级")
    elif str(temperature.get("level", "normal")) == "very_cold":
        reasons.append("近期实盘很冷，转为观望")

    if action == "allow":
        action_text = "盘面风控通过，按当前策略执行"
        reason_code = "fk1_allow"
    elif action == "cap":
        action_text = f"盘面风控限档，最高 {tier_cap or '当前低档'}"
        reason_code = "fk1_cap"
    else:
        action_text = "盘面风控建议观望，本局不下注"
        reason_code = "fk1_observe"

    reason_text = "；".join(item for item in reasons if item)
    return {
        "layer": FK1,
        "enabled": True,
        "action": action,
        "tier_cap": tier_cap,
        "reason_code": reason_code,
        "reason_text": reason_text,
        "action_text": action_text,
        "regime_label": regime_label,
        "similar_count": int(similar.get("similar_count", 0) or 0),
    }


def build_fk1_message(result: Dict[str, Any], snapshot: Dict[str, Any]) -> str:
    if result.get("action") == "observe":
        return (
            "🧭 盘面风控：观望\n"
            f"盘面：{result.get('regime_label', snapshot.get('regime_label', '震荡盘'))}\n"
            f"依据：{result.get('reason_text', '当前盘面证据不足')}\n"
            "动作：本局不下注"
        )
    if result.get("action") == "cap":
        return (
            "🧭 盘面风控：限档\n"
            f"盘面：{result.get('regime_label', snapshot.get('regime_label', '震荡盘'))}\n"
            f"依据：{result.get('reason_text', '当前盘面需降温处理')}\n"
            f"动作：允许下注，最高 {result.get('tier_cap', 'yc5')}"
        )
    return (
        "🧭 盘面风控：通过\n"
        f"盘面：{result.get('regime_label', snapshot.get('regime_label', '震荡盘'))}\n"
        f"依据：{result.get('reason_text', '当前盘面可正常执行')}\n"
        "动作：按当前策略执行"
    )
