from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import uuid

import dynamic_betting
import risk_control


REGIME_CONTINUATION = "延续盘"
REGIME_EXHAUSTION = "衰竭盘"
REGIME_REVERSAL = "反转盘"
REGIME_RANGE = "震荡盘"
REGIME_CHAOS = "混乱盘"

REGIME_NEIGHBORS = {
    REGIME_CONTINUATION: {REGIME_EXHAUSTION},
    REGIME_EXHAUSTION: {REGIME_CONTINUATION, REGIME_REVERSAL},
    REGIME_REVERSAL: {REGIME_EXHAUSTION},
    REGIME_RANGE: {REGIME_CHAOS},
    REGIME_CHAOS: {REGIME_RANGE},
}

LOW_TIERS = {"yc05", "yc1", "yc5"}
MID_TIERS = {"yc10", "yc20"}
HIGH_TIERS = {"yc50", "yc100", "yc200"}
REGIME_ORDER = [
    REGIME_CONTINUATION,
    REGIME_EXHAUSTION,
    REGIME_REVERSAL,
    REGIME_RANGE,
    REGIME_CHAOS,
]
HAND_BUCKET_ORDER = ["第1手", "第2手", "第3手", "第4手", "第5手+"]
BLOCK_REPORT_ORDER = ["strategy_observe", "fk1", "fk2", "fk3", "fund"]
BLOCK_REPORT_LABELS = {
    "strategy_observe": "策略观望",
    "fk1": "fk1 盘面风控",
    "fk2": "fk2 入场风控",
    "fk3": "fk3 连输风控",
    "fund": "资金风控",
}
TIER_SEQUENCE = ["yc05", "yc1", "yc5", "yc10", "yc20", "yc50", "yc100", "yc200"]


def _to_side_text(value: int) -> str:
    return "大" if int(value) == 1 else "小"


def _normalize_history(history: List[Any]) -> List[int]:
    normalized: List[int] = []
    for item in history or []:
        if item in (0, 1):
            normalized.append(int(item))
    return normalized


def _window(history: List[int], size: int) -> List[int]:
    if size <= 0:
        return []
    return history[-size:]


def _board_text(values: List[int]) -> str:
    if not values:
        return "-"
    return "".join(_to_side_text(item) for item in values)


def _switch_rate(values: List[int]) -> float:
    if len(values) <= 1:
        return 0.0
    switches = 0
    for idx in range(1, len(values)):
        if values[idx] != values[idx - 1]:
            switches += 1
    return round(switches / max(1, len(values) - 1), 4)


def _big_ratio(values: List[int]) -> float:
    if not values:
        return 0.5
    return round(sum(values) / len(values), 4)


def _tail_streak(values: List[int]) -> Tuple[int, str]:
    if not values:
        return 0, "neutral"
    side = values[-1]
    streak = 1
    for idx in range(len(values) - 2, -1, -1):
        if values[idx] != side:
            break
        streak += 1
    return streak, ("big" if side == 1 else "small")


def _dominance(ratio: float) -> float:
    return round(abs(float(ratio) - 0.5) * 2, 4)


def _clip_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def build_round_key(rt: Dict[str, Any], history_index: int) -> str:
    current_round = int(rt.get("current_round", 1) or 1)
    date_text = datetime.now().strftime("%Y%m%d")
    return f"rk:{date_text}:{max(history_index, 0)}:{current_round}"


def compute_regime_features(history: List[Any]) -> Dict[str, Any]:
    normalized = _normalize_history(history)
    w5 = _window(normalized, 5)
    w10 = _window(normalized, 10)
    w20 = _window(normalized, 20)
    w40 = _window(normalized, 40)

    w5_switch = _switch_rate(w5)
    w10_switch = _switch_rate(w10)
    w20_switch = _switch_rate(w20)
    w40_switch = _switch_rate(w40)
    w5_ratio = _big_ratio(w5)
    w10_ratio = _big_ratio(w10)
    w40_ratio = _big_ratio(w40)
    tail_streak_len, tail_streak_side = _tail_streak(normalized)
    gap_big_small = int(sum(w40) - (len(w40) - sum(w40))) if w40 else 0
    dominance40 = _dominance(w40_ratio)
    dominance10 = _dominance(w10_ratio)

    if w40_ratio >= 0.55:
        long_side = "big"
    elif w40_ratio <= 0.45:
        long_side = "small"
    else:
        long_side = "neutral"

    if w10_ratio >= 0.60:
        short_side = "big"
    elif w10_ratio <= 0.40:
        short_side = "small"
    else:
        short_side = "neutral"

    tail_align_long = int(long_side != "neutral" and tail_streak_side == long_side)
    long_short_conflict = int(long_side != "neutral" and short_side != "neutral" and long_side != short_side)
    tail_counter_long = 0.0
    if long_side != "neutral" and tail_streak_side != long_side:
        tail_counter_long = min(float(tail_streak_len) / 3.0, 1.0)

    trend_score = _clip_score(
        100.0 * (
            0.35 * (1.0 - w10_switch)
            + 0.25 * (1.0 - w40_switch)
            + 0.25 * dominance40
            + 0.15 * (tail_align_long * min(float(tail_streak_len) / 4.0, 1.0))
        )
    )
    chaos_score = _clip_score(
        100.0 * (
            0.45 * w5_switch
            + 0.35 * w10_switch
            + 0.20 * (1.0 - dominance10)
        )
    )
    reversal_score = _clip_score(
        100.0 * (
            0.40 * long_short_conflict
            + 0.35 * tail_counter_long
            + 0.15 * min(abs(gap_big_small) / 8.0, 1.0)
            + 0.10 * min(float(tail_streak_len) / 3.0, 1.0)
        )
    )

    features = {
        "w5_switch_rate": w5_switch,
        "w10_switch_rate": w10_switch,
        "w20_switch_rate": w20_switch,
        "w40_switch_rate": w40_switch,
        "w5_big_ratio": w5_ratio,
        "w10_big_ratio": w10_ratio,
        "w40_big_ratio": w40_ratio,
        "tail_streak_len": tail_streak_len,
        "tail_streak_side": tail_streak_side,
        "gap_big_small": gap_big_small,
        "dominance40": dominance40,
        "dominance10": dominance10,
        "long_side": long_side,
        "short_side": short_side,
        "tail_align_long": tail_align_long,
        "long_short_conflict": long_short_conflict,
        "tail_counter_long": round(tail_counter_long, 4),
        "trend_score": trend_score,
        "chaos_score": chaos_score,
        "reversal_score": reversal_score,
        "board_5": _board_text(w5),
        "board_10": _board_text(w10),
        "board_20": _board_text(w20),
        "board_40": _board_text(w40),
    }
    features["regime_label"] = classify_regime(features)
    return features


def classify_regime(features: Dict[str, Any]) -> str:
    chaos_score = int(features.get("chaos_score", 0) or 0)
    reversal_score = int(features.get("reversal_score", 0) or 0)
    trend_score = int(features.get("trend_score", 0) or 0)
    w5_switch = float(features.get("w5_switch_rate", 0.0) or 0.0)
    w10_switch = float(features.get("w10_switch_rate", 0.0) or 0.0)
    dominance40 = float(features.get("dominance40", 0.0) or 0.0)
    tail_streak_len = int(features.get("tail_streak_len", 0) or 0)
    long_side = str(features.get("long_side", "neutral") or "neutral")
    short_side = str(features.get("short_side", "neutral") or "neutral")
    tail_align_long = int(features.get("tail_align_long", 0) or 0)
    tail_counter_long = float(features.get("tail_counter_long", 0.0) or 0.0)
    gap_big_small = int(features.get("gap_big_small", 0) or 0)

    if (
        chaos_score >= 78
        or (w5_switch >= 0.75 and w10_switch >= 0.67 and dominance40 <= 0.20)
        or (long_side == "neutral" and short_side == "neutral" and tail_streak_len <= 2 and chaos_score >= 70)
    ):
        return REGIME_CHAOS

    if (
        (reversal_score >= 68 and tail_streak_len >= 2)
        or (int(features.get("long_short_conflict", 0) or 0) == 1 and tail_counter_long >= 0.66)
        or (
            abs(gap_big_small) >= 8
            and str(features.get("tail_streak_side", "neutral")) != long_side
            and tail_streak_len >= 2
        )
    ):
        return REGIME_REVERSAL

    if (
        (dominance40 >= 0.35 and tail_streak_len >= 3 and w5_switch >= 0.50)
        or (45 <= trend_score <= 65 and abs(gap_big_small) >= 6)
        or (long_side != "neutral" and tail_align_long == 1 and w10_switch >= 0.55)
    ):
        return REGIME_EXHAUSTION

    if (
        trend_score >= 65
        and chaos_score <= 40
        and dominance40 >= 0.25
        and tail_align_long == 1
        and tail_streak_len >= 2
    ):
        return REGIME_CONTINUATION

    return REGIME_RANGE


def _safe_parse_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _is_settled_entry(entry: Dict[str, Any]) -> bool:
    if not isinstance(entry, dict):
        return False
    if str(entry.get("status", "")).strip().lower() == "settled":
        return True
    if entry.get("settled_at"):
        return True
    profit = entry.get("profit", None)
    return isinstance(profit, (int, float))


def _entry_pnl(entry: Dict[str, Any]) -> int:
    try:
        return int(entry.get("profit", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _entry_win(entry: Dict[str, Any]) -> int:
    return 1 if _entry_pnl(entry) > 0 else 0


def _infer_preset_name(entry: Dict[str, Any], amount: int) -> str:
    preset_name = str(entry.get("preset_name", "") or "").strip().lower()
    if preset_name:
        return preset_name
    if amount >= 200000:
        return "yc200"
    if amount >= 100000:
        return "yc100"
    if amount >= 50000:
        return "yc50"
    if amount >= 20000:
        return "yc20"
    if amount >= 10000:
        return "yc10"
    if amount >= 5000:
        return "yc5"
    if amount >= 1000:
        return "yc1"
    return "yc05"


def _tier_group(preset_name: str) -> str:
    if preset_name in HIGH_TIERS:
        return "high"
    if preset_name in MID_TIERS:
        return "mid"
    return "low"


def _time_weight(settled_at: Optional[datetime], now: Optional[datetime] = None) -> float:
    if settled_at is None:
        return 0.55
    now = now or datetime.now()
    hours = max(0.0, (now - settled_at).total_seconds() / 3600.0)
    if hours <= 24:
        return 1.00
    if hours <= 72:
        return 0.85
    if hours <= 24 * 7:
        return 0.70
    if hours <= 24 * 30:
        return 0.55
    return 0.40


def _distance(current: Dict[str, Any], candidate: Dict[str, Any]) -> float:
    tail_side_diff = 0.0 if current.get("tail_streak_side") == candidate.get("tail_streak_side") else 1.0
    current_gap = min(abs(int(current.get("gap_big_small", 0) or 0)) / 10.0, 1.0)
    candidate_gap = min(abs(int(candidate.get("gap_big_small", 0) or 0)) / 10.0, 1.0)
    current_trend = min(float(current.get("trend_score", 0) or 0) / 100.0, 1.0)
    candidate_trend = min(float(candidate.get("trend_score", 0) or 0) / 100.0, 1.0)
    current_chaos = min(float(current.get("chaos_score", 0) or 0) / 100.0, 1.0)
    candidate_chaos = min(float(candidate.get("chaos_score", 0) or 0) / 100.0, 1.0)
    current_tail_len = min(float(current.get("tail_streak_len", 0) or 0) / 5.0, 1.0)
    candidate_tail_len = min(float(candidate.get("tail_streak_len", 0) or 0) / 5.0, 1.0)
    return (
        0.18 * abs(float(current.get("w10_switch_rate", 0.0)) - float(candidate.get("w10_switch_rate", 0.0)))
        + 0.12 * abs(float(current.get("w40_switch_rate", 0.0)) - float(candidate.get("w40_switch_rate", 0.0)))
        + 0.12 * abs(float(current.get("w10_big_ratio", 0.5)) - float(candidate.get("w10_big_ratio", 0.5)))
        + 0.12 * abs(float(current.get("w40_big_ratio", 0.5)) - float(candidate.get("w40_big_ratio", 0.5)))
        + 0.10 * abs(current_tail_len - candidate_tail_len)
        + 0.08 * tail_side_diff
        + 0.10 * abs(current_gap - candidate_gap)
        + 0.08 * abs(current_trend - candidate_trend)
        + 0.10 * abs(current_chaos - candidate_chaos)
    )


def _recent_24h_settled_entries(state) -> List[Dict[str, Any]]:
    entries = [item for item in (state.bet_sequence_log or []) if _is_settled_entry(item)]
    now = datetime.now()
    recent: List[Dict[str, Any]] = []
    for entry in entries:
        settled_at = _safe_parse_datetime(entry.get("settled_at"))
        if settled_at is None or (now - settled_at).total_seconds() <= 24 * 3600:
            recent.append(entry)
    return recent


def compute_recent_temperature(state) -> Dict[str, Any]:
    settled = [item for item in (state.bet_sequence_log or []) if _is_settled_entry(item)]
    recent10 = settled[-10:]
    if not recent10:
        return {
            "level": "normal",
            "win_rate_10": 0.0,
            "pnl_10": 0,
            "drawdown_10": 0,
            "sample_size": 0,
        }

    wins = sum(_entry_win(item) for item in recent10)
    pnl_series = [_entry_pnl(item) for item in recent10]
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for pnl in pnl_series:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    win_rate = wins / len(recent10)
    total_pnl = sum(pnl_series)

    if len(recent10) >= 5 and (win_rate <= 0.30 or (win_rate <= 0.40 and total_pnl < 0 and max_drawdown > 0)):
        level = "very_cold"
    elif len(recent10) >= 5 and (win_rate <= 0.45 or total_pnl < 0):
        level = "cold"
    else:
        level = "normal"

    return {
        "level": level,
        "win_rate_10": round(win_rate, 4),
        "pnl_10": total_pnl,
        "drawdown_10": max_drawdown,
        "sample_size": len(recent10),
    }


def _empty_similar_summary() -> Dict[str, Any]:
    return {
        "similar_count": 0,
        "evidence_strength": "insufficient",
        "weighted_signal_hit_rate": 0.0,
        "recommended_tier_cap": "",
        "tiers": {},
        "source": "none",
    }


def _build_similar_summary(candidates: List[Dict[str, Any]], limit: int = 40, source: str = "none") -> Dict[str, Any]:
    candidates.sort(key=lambda item: (item["distance"], -item["weight"]))
    topk = candidates[: max(1, limit)]
    if not topk:
        return _empty_similar_summary()
    total_weight = sum(item["weight"] for item in topk) or 1.0
    weighted_hit_rate = sum(item["win"] * item["weight"] for item in topk) / total_weight
    tiers: Dict[str, Dict[str, Any]] = {}
    for group in ("low", "mid", "high"):
        group_items = [item for item in topk if item["tier_group"] == group]
        if not group_items:
            continue
        group_weight = sum(item["weight"] for item in group_items) or 1.0
        avg_pnl = sum(item["pnl"] * item["weight"] for item in group_items) / group_weight
        win_rate = sum(item["win"] * item["weight"] for item in group_items) / group_weight
        min_pnl = min(item["pnl"] for item in group_items)
        tiers[group] = {
            "count": len(group_items),
            "avg_pnl": round(avg_pnl, 2),
            "win_rate": round(win_rate, 4),
            "min_pnl": int(min_pnl),
        }

    recommended_tier_cap = ""
    low_stats = tiers.get("low", {})
    mid_stats = tiers.get("mid", {})
    high_stats = tiers.get("high", {})
    if len(topk) >= 40:
        high_bad = bool(high_stats) and (
            float(high_stats.get("avg_pnl", 0.0)) < 0
            or (bool(low_stats) and float(high_stats.get("win_rate", 0.0)) + 0.03 < float(low_stats.get("win_rate", 0.0)))
        )
        all_bad = (
            weighted_hit_rate < 0.49
            and (not low_stats or float(low_stats.get("avg_pnl", 0.0)) <= 0)
            and (not mid_stats or float(mid_stats.get("avg_pnl", 0.0)) <= 0)
            and (not high_stats or float(high_stats.get("avg_pnl", 0.0)) <= 0)
        )
        if all_bad:
            recommended_tier_cap = "observe"
        elif high_bad:
            recommended_tier_cap = "yc5"
        elif low_stats and float(low_stats.get("avg_pnl", 0.0)) > 0 and high_stats and float(high_stats.get("avg_pnl", 0.0)) <= 0:
            recommended_tier_cap = "yc5"

    if len(topk) >= 40:
        evidence_strength = "strong"
    elif len(topk) >= 15:
        evidence_strength = "weak"
    else:
        evidence_strength = "insufficient"

    return {
        "similar_count": len(topk),
        "evidence_strength": evidence_strength,
        "weighted_signal_hit_rate": round(weighted_hit_rate, 4),
        "recommended_tier_cap": recommended_tier_cap,
        "tiers": tiers,
        "source": source,
    }


def _summarize_similar_cases_from_state(state, current_features: Dict[str, Any], limit: int = 40) -> Dict[str, Any]:
    history = _normalize_history(getattr(state, "history", []))
    settled_entries = [item for item in (state.bet_sequence_log or []) if _is_settled_entry(item)]
    if not history or not settled_entries:
        return _empty_similar_summary()

    current_label = str(current_features.get("regime_label", REGIME_RANGE))
    neighbor_labels = REGIME_NEIGHBORS.get(current_label, set())
    candidates = []
    for entry in settled_entries[-300:]:
        try:
            history_index = int(entry.get("settle_history_index", -1))
        except (TypeError, ValueError):
            history_index = -1
        if history_index < 4 or history_index >= len(history):
            continue
        candidate_history = history[: history_index + 1]
        candidate_features = compute_regime_features(candidate_history)
        candidate_label = str(candidate_features.get("regime_label", REGIME_RANGE))
        if candidate_label != current_label and candidate_label not in neighbor_labels:
            continue
        distance = _distance(current_features, candidate_features)
        weight = max(0.0, 1.0 - distance)
        if weight <= 0:
            continue
        settled_at = _safe_parse_datetime(entry.get("settled_at"))
        final_weight = round(weight * _time_weight(settled_at), 6)
        try:
            amount = int(entry.get("amount", 0) or 0)
        except (TypeError, ValueError):
            amount = 0
        preset_name = _infer_preset_name(entry, amount)
        candidates.append(
            {
                "distance": distance,
                "weight": final_weight,
                "preset_name": preset_name,
                "tier_group": _tier_group(preset_name),
                "pnl": _entry_pnl(entry),
                "win": _entry_win(entry),
            }
        )
    return _build_similar_summary(candidates, limit=limit, source="state")


def _summarize_similar_cases_from_analytics(user_ctx, current_features: Dict[str, Any], limit: int = 40) -> Dict[str, Any]:
    db_path = _analytics_db_path(user_ctx)
    if not os.path.exists(db_path):
        return _empty_similar_summary()

    settlement_rows = _analytics_rows(
        user_ctx,
        "SELECT * FROM settlements ORDER BY settled_at DESC LIMIT 800",
    )
    if not settlement_rows:
        return _empty_similar_summary()

    round_keys = [str(row.get("round_key", "") or "") for row in settlement_rows if str(row.get("round_key", "") or "")]
    feature_rows = _rows_by_round_key(user_ctx, "regime_features", round_keys)
    execution_rows = _rows_by_round_key(user_ctx, "execution_records", round_keys)
    features_by_key = _latest_row_map(feature_rows)
    executions_by_key = _latest_row_map(execution_rows)

    current_label = str(current_features.get("regime_label", REGIME_RANGE))
    neighbor_labels = REGIME_NEIGHBORS.get(current_label, set())
    candidates = []
    for settlement in settlement_rows:
        round_key = str(settlement.get("round_key", "") or "")
        candidate_features = features_by_key.get(round_key)
        if not candidate_features:
            continue
        candidate_label = str(candidate_features.get("regime_label", REGIME_RANGE) or REGIME_RANGE)
        if candidate_label != current_label and candidate_label not in neighbor_labels:
            continue
        distance = _distance(current_features, candidate_features)
        weight = max(0.0, 1.0 - distance)
        if weight <= 0:
            continue
        settled_at = _safe_parse_datetime(settlement.get("settled_at"))
        final_weight = round(weight * _time_weight(settled_at), 6)
        execution = executions_by_key.get(round_key, {})
        amount = _safe_int_value(execution.get("bet_amount", 0), 0)
        preset_name = str(execution.get("preset_name", "") or "").strip().lower()
        if not preset_name:
            preset_name = _infer_preset_name({}, amount)
        candidates.append(
            {
                "distance": distance,
                "weight": final_weight,
                "preset_name": preset_name,
                "tier_group": _tier_group(preset_name),
                "pnl": _safe_int_value(settlement.get("profit", 0), 0),
                "win": _safe_int_value(settlement.get("is_win", 0), 0),
            }
        )
    return _build_similar_summary(candidates, limit=limit, source="analytics")


def summarize_similar_cases(user_ctx, current_features: Dict[str, Any], limit: int = 40) -> Dict[str, Any]:
    analytics_summary = _summarize_similar_cases_from_analytics(user_ctx, current_features, limit=limit)
    state_summary = _summarize_similar_cases_from_state(user_ctx.state, current_features, limit=limit)
    if int(analytics_summary.get("similar_count", 0) or 0) >= 15:
        return analytics_summary
    if int(state_summary.get("similar_count", 0) or 0) > int(analytics_summary.get("similar_count", 0) or 0):
        return state_summary
    return analytics_summary


def build_current_analysis_snapshot(user_ctx) -> Dict[str, Any]:
    state = user_ctx.state
    rt = state.runtime
    history = _normalize_history(state.history)
    history_index = len(history) - 1
    features = compute_regime_features(history)
    similar_cases = summarize_similar_cases(user_ctx, features)
    recent_temperature = compute_recent_temperature(state)
    round_key = build_round_key(rt, history_index)
    snapshot = {
        "round_key": round_key,
        "history_index": history_index,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "regime_label": features.get("regime_label", REGIME_RANGE),
        "board_5": features.get("board_5", "-"),
        "board_10": features.get("board_10", "-"),
        "board_20": features.get("board_20", "-"),
        "board_40": features.get("board_40", "-"),
        "features": features,
        "similar_cases": similar_cases,
        "recent_temperature": recent_temperature,
    }
    rt["current_round_key"] = round_key
    rt["current_analysis_snapshot"] = snapshot
    return snapshot


def _format_pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def build_fp_overview(user_ctx) -> str:
    state = user_ctx.state
    rt = state.runtime
    snapshot = build_current_analysis_snapshot(user_ctx)
    analytics = _recent_analytics_context(user_ctx, hours=24)
    settlements = analytics.get("settlements", [])
    executions = analytics.get("executions", [])
    risks = analytics.get("risks", [])
    if settlements or executions or risks:
        pnl24 = sum(int(item.get("profit", 0) or 0) for item in settlements)
        wins24 = sum(int(item.get("is_win", 0) or 0) for item in settlements)
        win_rate24 = (wins24 / len(settlements)) if settlements else 0.0
        max_drawdown = _max_drawdown([int(item.get("profit", 0) or 0) for item in settlements])
        observe_count = sum(1 for item in executions if str(item.get("action_type", "") or "") == "observe")
        blocked_count = sum(1 for item in executions if str(item.get("action_type", "") or "") == "blocked")
        cap_count = sum(
            1
            for item in risks
            if str(item.get("layer_code", "") or "") == "fk1" and str(item.get("action", "") or "") == "cap"
        )
        settled_count = len(settlements)
    else:
        settled = [item for item in (state.bet_sequence_log or []) if _is_settled_entry(item)]
        recent24 = _recent_24h_settled_entries(state)
        pnl24 = sum(_entry_pnl(item) for item in recent24)
        wins24 = sum(_entry_win(item) for item in recent24)
        win_rate24 = (wins24 / len(recent24)) if recent24 else 0.0
        max_drawdown = compute_recent_temperature(state).get("drawdown_10", 0)
        observe_count = 0
        blocked_count = 0
        cap_count = 0
        settled_count = len(settled)
    return (
        "📊 24小时复盘总览\n\n"
        f"当前盘面：{snapshot.get('regime_label', REGIME_RANGE)}\n"
        f"当前结论：{str(rt.get('current_fk1_action_text', '未评估') or '未评估')}\n"
        f"相似样本：{snapshot.get('similar_cases', {}).get('similar_count', 0)} 组\n\n"
        f"24h 实盘：\n胜率 {_format_pct(win_rate24)}\n总盈亏 {pnl24:+,}\n最大回撤 {int(max_drawdown):,}\n\n"
        f"24h 风控：\n观望 {observe_count} 次\n阻断 {blocked_count} 次\n限档 {cap_count} 次\n\n"
        f"已结算样本：{settled_count}\n"
        f"当前开关：fk1 {'开' if bool(rt.get('fk1_enabled', True)) else '关'} / "
        f"fk2 {'开' if bool(rt.get('fk2_enabled', True)) else '关'} / "
        f"fk3 {'开' if bool(rt.get('fk3_enabled', True)) else '关'}"
    )


def build_fp_current_evidence(user_ctx) -> str:
    snapshot = build_current_analysis_snapshot(user_ctx)
    features = snapshot.get("features", {})
    similar = snapshot.get("similar_cases", {})
    fk1_result = risk_control.evaluate_fk1(snapshot, user_ctx.state.runtime)
    dynamic_runtime = dict(user_ctx.state.runtime)
    dynamic_runtime["current_fk1_tier_cap"] = str(fk1_result.get("tier_cap", "") or "")
    dynamic_result = dynamic_betting.evaluate_dynamic_bet(snapshot, dynamic_runtime)
    low = similar.get("tiers", {}).get("low", {})
    mid = similar.get("tiers", {}).get("mid", {})
    high = similar.get("tiers", {}).get("high", {})
    source_map = {"analytics": "analytics.db", "state": "内存日志", "none": "暂无"}
    temperature_map = {
        "normal": "正常",
        "cold": "偏冷",
        "very_cold": "很冷",
    }
    evidence_strength = str(similar.get("evidence_strength", "insufficient") or "insufficient")
    strength_text = {"strong": "强证据", "weak": "弱证据", "insufficient": "样本不足"}.get(evidence_strength, evidence_strength)
    temperature_text = temperature_map.get(
        str(snapshot.get("recent_temperature", {}).get("level", "normal") or "normal"),
        "正常",
    )
    recommended_cap = str(similar.get("recommended_tier_cap", "") or "")
    def _tier_line(title: str, tier_stats: Dict[str, Any]) -> str:
        if not tier_stats:
            return f"{title}：样本不足"
        avg_pnl = float(tier_stats.get("avg_pnl", 0.0) or 0.0)
        risk_text = "稳"
        if avg_pnl < 0:
            risk_text = "偏弱"
        if avg_pnl < -5000:
            risk_text = "高风险"
        return (
            f"{title}：胜率 {_format_pct(tier_stats.get('win_rate', 0.0))} | "
            f"均盈亏 {_format_signed_int(int(round(avg_pnl)))} | 评价 {risk_text}"
        )
    return (
        "📌 当前盘面证据\n\n"
        f"盘面：{snapshot.get('regime_label', REGIME_RANGE)}\n"
        f"当前建议：{fk1_result.get('action_text', '未评估')}\n"
        f"依据：{fk1_result.get('reason_text', '当前盘面证据不足')}\n"
        f"趋势分：{features.get('trend_score', 0)}\n"
        f"混乱分：{features.get('chaos_score', 0)}\n"
        f"反转分：{features.get('reversal_score', 0)}\n"
        f"近期温度：{temperature_text}\n"
        f"相似样本：{similar.get('similar_count', 0)} 组 | 证据强度 {strength_text} | 来源 {source_map.get(str(similar.get('source', 'none') or 'none'), '暂无')}\n"
        f"历史建议：{('观望' if recommended_cap == 'observe' else (recommended_cap or '未额外收紧'))}\n\n"
        f"动态档位：{dynamic_result.get('base_tier', '')} -> {dynamic_result.get('applied_tier', '')}\n"
        f"动态依据：{dynamic_result.get('reason_text', '按当前预设执行')}\n\n"
        f"{_tier_line('低档', low)}\n"
        f"{_tier_line('中档', mid)}\n"
        f"{_tier_line('高档', high)}"
    )


def _recent_cutoff_text(hours: int = 24) -> str:
    return (datetime.now() - timedelta(hours=max(1, int(hours or 24)))).strftime("%Y-%m-%d %H:%M:%S")


def _analytics_rows(user_ctx, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    db_path = _analytics_db_path(user_ctx)
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_analytics_schema(conn)
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _rows_by_round_key(user_ctx, table_name: str, round_keys: List[str]) -> List[Dict[str, Any]]:
    keys = [str(item or "").strip() for item in round_keys if str(item or "").strip()]
    if not keys:
        return []
    placeholders = ", ".join("?" for _ in keys)
    sql = f"SELECT * FROM {table_name} WHERE round_key IN ({placeholders})"
    return _analytics_rows(user_ctx, sql, tuple(keys))


def _latest_row_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        round_key = str(row.get("round_key", "") or "")
        if round_key:
            mapping[round_key] = row
    return mapping


def _group_rows_by_round(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        round_key = str(row.get("round_key", "") or "")
        if not round_key:
            continue
        grouped.setdefault(round_key, []).append(row)
    return grouped


def _recent_analytics_context(user_ctx, hours: int = 24) -> Dict[str, Any]:
    cutoff = _recent_cutoff_text(hours)
    rounds = _analytics_rows(
        user_ctx,
        "SELECT * FROM rounds WHERE captured_at >= ? ORDER BY captured_at ASC",
        (cutoff,),
    )
    decisions = _analytics_rows(
        user_ctx,
        "SELECT * FROM decisions WHERE decision_time >= ? ORDER BY decision_time ASC",
        (cutoff,),
    )
    executions = _analytics_rows(
        user_ctx,
        "SELECT * FROM execution_records WHERE created_at >= ? ORDER BY created_at ASC",
        (cutoff,),
    )
    settlements = _analytics_rows(
        user_ctx,
        "SELECT * FROM settlements WHERE settled_at >= ? ORDER BY settled_at ASC",
        (cutoff,),
    )
    risks = _analytics_rows(
        user_ctx,
        "SELECT * FROM risk_records WHERE created_at >= ? ORDER BY created_at ASC",
        (cutoff,),
    )

    round_keys = {
        str(row.get("round_key", "") or "")
        for row in rounds + decisions + executions + settlements + risks
        if str(row.get("round_key", "") or "")
    }
    existing_round_keys = {str(row.get("round_key", "") or "") for row in rounds if str(row.get("round_key", "") or "")}
    missing_round_keys = sorted(round_keys - existing_round_keys)
    if missing_round_keys:
        rounds.extend(_rows_by_round_key(user_ctx, "rounds", missing_round_keys))

    regimes = _rows_by_round_key(user_ctx, "regime_features", sorted(round_keys))
    return {
        "rounds": rounds,
        "decisions": decisions,
        "executions": executions,
        "settlements": settlements,
        "risks": risks,
        "regimes": regimes,
        "rounds_by_key": _latest_row_map(rounds),
        "decisions_by_key": _latest_row_map(decisions),
        "executions_by_key": _latest_row_map(executions),
        "settlements_by_key": _latest_row_map(settlements),
        "risks_by_round": _group_rows_by_round(risks),
        "regimes_by_key": _latest_row_map(regimes),
    }


def _format_signed_int(value: Any) -> str:
    try:
        return f"{int(value):+,}"
    except (TypeError, ValueError):
        return "0"


def _max_drawdown(pnls: List[int]) -> int:
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for pnl in pnls:
        cumulative += int(pnl)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return int(max_drawdown)


def _most_common_label(counter: Counter) -> str:
    if not counter:
        return "-"
    return str(counter.most_common(1)[0][0] or "-")


def _hand_bucket(value: Any) -> str:
    try:
        hand_no = int(value or 0)
    except (TypeError, ValueError):
        hand_no = 0
    if hand_no <= 1:
        return "第1手"
    if hand_no == 2:
        return "第2手"
    if hand_no == 3:
        return "第3手"
    if hand_no == 4:
        return "第4手"
    return "第5手+"


def _execution_block_category(execution: Dict[str, Any]) -> str:
    action_type = str(execution.get("action_type", "") or "")
    if action_type == "observe":
        return "strategy_observe"
    if action_type != "blocked":
        return ""
    blocked_by = str(execution.get("blocked_by", "") or "")
    return blocked_by if blocked_by in {"fk1", "fk2", "fk3", "fund"} else ""


def _tier_sort_key(preset_name: str) -> int:
    normalized = str(preset_name or "").strip().lower()
    try:
        return TIER_SEQUENCE.index(normalized)
    except ValueError:
        return len(TIER_SEQUENCE) + 1


def build_fp_regime_report(user_ctx) -> str:
    analytics = _recent_analytics_context(user_ctx, hours=24)
    rounds_by_key = analytics["rounds_by_key"]
    if not rounds_by_key:
        return "📊 按盘面复盘（24h）\n\n暂无 24 小时结构化复盘数据"

    regime_by_key = analytics["regimes_by_key"]
    decisions_by_key = analytics["decisions_by_key"]
    executions_by_key = analytics["executions_by_key"]
    settlements_by_key = analytics["settlements_by_key"]
    risks_by_round = analytics["risks_by_round"]

    stats: Dict[str, Dict[str, Any]] = {}
    for regime_label in REGIME_ORDER:
        stats[regime_label] = {
            "samples": 0,
            "signal_total": 0,
            "signal_hits": 0,
            "bet_count": 0,
            "observe_count": 0,
            "blocked_count": 0,
            "cap_count": 0,
            "settled_count": 0,
            "settle_wins": 0,
            "pnl_total": 0,
        }

    for round_key, round_row in rounds_by_key.items():
        regime_label = str(
            regime_by_key.get(round_key, {}).get("regime_label", REGIME_RANGE) or REGIME_RANGE
        )
        bucket = stats.setdefault(
            regime_label,
            {
                "samples": 0,
                "signal_total": 0,
                "signal_hits": 0,
                "bet_count": 0,
                "observe_count": 0,
                "blocked_count": 0,
                "cap_count": 0,
                "settled_count": 0,
                "settle_wins": 0,
                "pnl_total": 0,
            },
        )
        bucket["samples"] += 1

        decision = decisions_by_key.get(round_key)
        if decision and int(decision.get("is_observe", 0) or 0) == 0:
            bucket["signal_total"] += 1
            if str(decision.get("direction_code", "") or "") == str(round_row.get("result_side", "") or ""):
                bucket["signal_hits"] += 1

        execution = executions_by_key.get(round_key)
        if execution:
            action_type = str(execution.get("action_type", "") or "")
            if action_type == "bet":
                bucket["bet_count"] += 1
            elif action_type == "observe":
                bucket["observe_count"] += 1
            elif action_type == "blocked":
                bucket["blocked_count"] += 1

        for risk in risks_by_round.get(round_key, []):
            if str(risk.get("layer_code", "") or "") == "fk1" and str(risk.get("action", "") or "") == "cap":
                bucket["cap_count"] += 1

        settlement = settlements_by_key.get(round_key)
        if settlement:
            bucket["settled_count"] += 1
            bucket["settle_wins"] += int(settlement.get("is_win", 0) or 0)
            bucket["pnl_total"] += int(settlement.get("profit", 0) or 0)

    lines = ["📊 按盘面复盘（24h）", ""]
    for regime_label in REGIME_ORDER:
        item = stats.get(regime_label, {})
        if int(item.get("samples", 0) or 0) <= 0:
            continue
        signal_rate = (
            float(item["signal_hits"]) / float(item["signal_total"])
            if int(item.get("signal_total", 0) or 0) > 0
            else 0.0
        )
        settle_rate = (
            float(item["settle_wins"]) / float(item["settled_count"])
            if int(item.get("settled_count", 0) or 0) > 0
            else 0.0
        )
        lines.append(
            f"{regime_label}：样本 {item['samples']} | 信号命中 {_format_pct(signal_rate)} | "
            f"实盘胜率 {_format_pct(settle_rate)} | 盈亏 {_format_signed_int(item['pnl_total'])} | "
            f"动作 下注{item['bet_count']}/观望{item['observe_count']}/阻断{item['blocked_count']}/限档{item['cap_count']}"
        )
    return "\n".join(lines)


def build_fp_tier_report(user_ctx) -> str:
    analytics = _recent_analytics_context(user_ctx, hours=24)
    bet_rows = [row for row in analytics["executions"] if str(row.get("action_type", "") or "") == "bet"]
    if not bet_rows:
        return "📊 按档位复盘（24h）\n\n暂无 24 小时真实下注数据"

    settlements_by_key = analytics["settlements_by_key"]
    regime_by_key = analytics["regimes_by_key"]
    stats: Dict[str, Dict[str, Any]] = {}
    for execution in bet_rows:
        preset_name = str(execution.get("preset_name", "") or "").strip().lower()
        if not preset_name:
            preset_name = _infer_preset_name({}, _safe_int_value(execution.get("bet_amount", 0), 0))
        item = stats.setdefault(
            preset_name,
            {
                "count": 0,
                "settled_count": 0,
                "wins": 0,
                "pnl_total": 0,
                "pnls": [],
                "regimes": Counter(),
            },
        )
        item["count"] += 1
        regime_label = str(
            regime_by_key.get(str(execution.get("round_key", "") or ""), {}).get("regime_label", "-") or "-"
        )
        item["regimes"][regime_label] += 1
        settlement = settlements_by_key.get(str(execution.get("round_key", "") or ""))
        if settlement:
            pnl_value = int(settlement.get("profit", 0) or 0)
            item["settled_count"] += 1
            item["wins"] += int(settlement.get("is_win", 0) or 0)
            item["pnl_total"] += pnl_value
            item["pnls"].append(pnl_value)

    lines = ["📊 按档位复盘（24h）", ""]
    for preset_name in sorted(stats.keys(), key=_tier_sort_key):
        item = stats[preset_name]
        win_rate = (
            float(item["wins"]) / float(item["settled_count"])
            if int(item.get("settled_count", 0) or 0) > 0
            else 0.0
        )
        avg_pnl = (
            int(round(float(item["pnl_total"]) / float(item["settled_count"])))
            if int(item.get("settled_count", 0) or 0) > 0
            else 0
        )
        lines.append(
            f"{preset_name}：使用 {item['count']} | 胜率 {_format_pct(win_rate)} | "
            f"均盈亏 {_format_signed_int(avg_pnl)} | 最大回撤 {_format_signed_int(_max_drawdown(item['pnls']))} | "
            f"常见盘面 {_most_common_label(item['regimes'])}"
        )
    return "\n".join(lines)


def build_fp_hand_report(user_ctx) -> str:
    analytics = _recent_analytics_context(user_ctx, hours=24)
    execution_rows = analytics["executions"]
    if not execution_rows:
        return "📊 按手位复盘（24h）\n\n暂无 24 小时执行数据"

    rounds_by_key = analytics["rounds_by_key"]
    decisions_by_key = analytics["decisions_by_key"]
    settlements_by_key = analytics["settlements_by_key"]
    regime_by_key = analytics["regimes_by_key"]
    latest_executions = analytics["executions_by_key"].values()
    stats: Dict[str, Dict[str, Any]] = {
        bucket: {
            "samples": 0,
            "signal_total": 0,
            "signal_hits": 0,
            "settled_count": 0,
            "settle_wins": 0,
            "pnl_total": 0,
            "regimes": Counter(),
            "blocks": Counter(),
        }
        for bucket in HAND_BUCKET_ORDER
    }

    for execution in latest_executions:
        round_key = str(execution.get("round_key", "") or "")
        bucket_name = _hand_bucket(execution.get("bet_hand_index", 0))
        item = stats[bucket_name]
        item["samples"] += 1
        regime_label = str(regime_by_key.get(round_key, {}).get("regime_label", "-") or "-")
        item["regimes"][regime_label] += 1

        blocked_by = str(execution.get("blocked_by", "") or "")
        if blocked_by:
            item["blocks"][blocked_by] += 1

        decision = decisions_by_key.get(round_key)
        round_row = rounds_by_key.get(round_key, {})
        if decision and int(decision.get("is_observe", 0) or 0) == 0:
            item["signal_total"] += 1
            if str(decision.get("direction_code", "") or "") == str(round_row.get("result_side", "") or ""):
                item["signal_hits"] += 1

        settlement = settlements_by_key.get(round_key)
        if settlement:
            item["settled_count"] += 1
            item["settle_wins"] += int(settlement.get("is_win", 0) or 0)
            item["pnl_total"] += int(settlement.get("profit", 0) or 0)

    lines = ["📊 按手位复盘（24h）", ""]
    for bucket_name in HAND_BUCKET_ORDER:
        item = stats[bucket_name]
        if int(item.get("samples", 0) or 0) <= 0:
            continue
        signal_rate = (
            float(item["signal_hits"]) / float(item["signal_total"])
            if int(item.get("signal_total", 0) or 0) > 0
            else 0.0
        )
        settle_rate = (
            float(item["settle_wins"]) / float(item["settled_count"])
            if int(item.get("settled_count", 0) or 0) > 0
            else 0.0
        )
        avg_pnl = (
            int(round(float(item["pnl_total"]) / float(item["settled_count"])))
            if int(item.get("settled_count", 0) or 0) > 0
            else 0
        )
        common_block = _most_common_label(item["blocks"])
        if common_block == "-":
            common_block = "下注为主"
        lines.append(
            f"{bucket_name}：样本 {item['samples']} | 信号命中 {_format_pct(signal_rate)} | "
            f"实盘胜率 {_format_pct(settle_rate)} | 均盈亏 {_format_signed_int(avg_pnl)} | "
            f"常见盘面 {_most_common_label(item['regimes'])} | 常见阻断 {common_block}"
        )
    return "\n".join(lines)


def build_fp_block_report(user_ctx) -> str:
    analytics = _recent_analytics_context(user_ctx, hours=24)
    latest_executions = list(analytics["executions_by_key"].values())
    relevant = [item for item in latest_executions if _execution_block_category(item)]
    if not relevant:
        return "📊 观望/阻断复盘（24h）\n\n暂无 24 小时观望或阻断数据"

    regime_by_key = analytics["regimes_by_key"]
    stats: Dict[str, Dict[str, Any]] = {
        key: {"count": 0, "regimes": Counter()}
        for key in BLOCK_REPORT_ORDER
    }
    for execution in relevant:
        category = _execution_block_category(execution)
        if not category:
            continue
        stats[category]["count"] += 1
        regime_label = str(
            regime_by_key.get(str(execution.get("round_key", "") or ""), {}).get("regime_label", "-") or "-"
        )
        stats[category]["regimes"][regime_label] += 1

    total = sum(int(item["count"]) for item in stats.values()) or 1
    lines = ["📊 观望/阻断复盘（24h）", ""]
    for category in BLOCK_REPORT_ORDER:
        item = stats[category]
        if int(item.get("count", 0) or 0) <= 0:
            continue
        lines.append(
            f"{BLOCK_REPORT_LABELS[category]}：{item['count']} 次 | 占比 {_format_pct(item['count'] / total)} | "
            f"常见盘面 {_most_common_label(item['regimes'])}"
        )
    return "\n".join(lines)


def _sample_round_keys(round_keys: List[str], limit: int = 3) -> str:
    samples = [str(item or "").strip() for item in round_keys if str(item or "").strip()][: max(1, limit)]
    return "、".join(samples) if samples else "-"


def build_fp_linkage_report(user_ctx) -> str:
    analytics = _recent_analytics_context(user_ctx, hours=24)
    rounds_by_key = analytics["rounds_by_key"]
    decisions_by_key = analytics["decisions_by_key"]
    executions_by_key = analytics["executions_by_key"]
    settlements_by_key = analytics["settlements_by_key"]
    regimes_by_key = analytics["regimes_by_key"]
    risks_by_round = analytics["risks_by_round"]

    if not rounds_by_key and not decisions_by_key and not executions_by_key and not settlements_by_key:
        return "📎 链路覆盖（24h）\n\n暂无 24 小时结构化链路数据"

    round_keys = set(rounds_by_key.keys())
    decision_keys = set(decisions_by_key.keys())
    execution_keys = set(executions_by_key.keys())
    settlement_keys = set(settlements_by_key.keys())
    regime_keys = set(regimes_by_key.keys())

    bet_execution_keys = {
        key
        for key, row in executions_by_key.items()
        if str(row.get("action_type", "") or "") == "bet"
    }
    observe_execution_keys = {
        key
        for key, row in executions_by_key.items()
        if str(row.get("action_type", "") or "") == "observe"
    }
    blocked_execution_keys = {
        key
        for key, row in executions_by_key.items()
        if str(row.get("action_type", "") or "") == "blocked"
    }
    risk_round_keys = {
        key
        for key, rows in risks_by_round.items()
        if rows
    }

    round_total = len(round_keys)
    decision_total = len(decision_keys)
    execution_total = len(execution_keys)
    bet_total = len(bet_execution_keys)
    settlement_total = len(settlement_keys)
    blocked_total = len(blocked_execution_keys)
    strategy_risk_keys = {
        key
        for key, rows in risks_by_round.items()
        if any(str(row.get("layer_code", "") or "") == "strategy" for row in rows)
    }

    missing_decision = sorted(round_keys - decision_keys)
    missing_execution = sorted(decision_keys - execution_keys)
    missing_settlement = sorted(bet_execution_keys - settlement_keys)
    settlement_without_bet = sorted(settlement_keys - bet_execution_keys)
    blocked_without_risk = sorted(blocked_execution_keys - risk_round_keys)
    observe_without_strategy = sorted(observe_execution_keys - strategy_risk_keys)

    lines = [
        "📎 链路覆盖（24h）",
        "",
        f"盘面入库覆盖：{len(regime_keys)}/{round_total} ({_format_pct((len(regime_keys) / round_total) if round_total else 0.0)})",
        f"决策链路覆盖：{decision_total}/{round_total} ({_format_pct((decision_total / round_total) if round_total else 0.0)})",
        f"执行动作覆盖：{execution_total}/{decision_total} ({_format_pct((execution_total / decision_total) if decision_total else 0.0)})",
        f"真实结算覆盖：{settlement_total}/{bet_total} ({_format_pct((settlement_total / bet_total) if bet_total else 0.0)})",
        f"策略观望记录覆盖：{len(observe_execution_keys) - len(observe_without_strategy)}/{len(observe_execution_keys)} ({_format_pct(((len(observe_execution_keys) - len(observe_without_strategy)) / len(observe_execution_keys)) if observe_execution_keys else 0.0)})",
        f"风控阻断记录覆盖：{blocked_total - len(blocked_without_risk)}/{blocked_total} ({_format_pct(((blocked_total - len(blocked_without_risk)) / blocked_total) if blocked_total else 0.0)})",
        "",
        f"缺口1 有盘面无决策：{len(missing_decision)} 局 | 样例 {_sample_round_keys(missing_decision)}",
        f"缺口2 有决策无执行：{len(missing_execution)} 局 | 样例 {_sample_round_keys(missing_execution)}",
        f"缺口3 有下注无结算：{len(missing_settlement)} 局 | 样例 {_sample_round_keys(missing_settlement)}",
        f"缺口4 有结算无下注：{len(settlement_without_bet)} 局 | 样例 {_sample_round_keys(settlement_without_bet)}",
        f"缺口5 有阻断无风控记录：{len(blocked_without_risk)} 局 | 样例 {_sample_round_keys(blocked_without_risk)}",
        f"补充 有观望无策略记录：{len(observe_without_strategy)} 局 | 样例 {_sample_round_keys(observe_without_strategy)}",
    ]
    return "\n".join(lines)


def _analytics_db_path(user_ctx) -> str:
    return os.path.join(user_ctx.user_dir, "analytics.db")


def _json_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _safe_int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_result_side(result_value: Any, fallback: Any = None) -> str:
    if result_value in (0, 1):
        return "big" if int(result_value) == 1 else "small"
    text = str(fallback or "").strip().lower()
    if text in {"big", "大"}:
        return "big"
    if text in {"small", "小"}:
        return "small"
    return ""


def _ensure_analytics_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rounds (
            round_key TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            history_index INTEGER NOT NULL,
            issue_no TEXT,
            result_num INTEGER,
            result_side TEXT,
            captured_at TEXT NOT NULL,
            board_5 TEXT NOT NULL,
            board_10 TEXT NOT NULL,
            board_20 TEXT NOT NULL,
            board_40 TEXT NOT NULL,
            current_round_no INTEGER NOT NULL,
            current_hand_no INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS decisions (
            decision_id TEXT PRIMARY KEY,
            round_key TEXT NOT NULL,
            decision_time TEXT NOT NULL,
            mode TEXT NOT NULL,
            model_id TEXT,
            prediction INTEGER NOT NULL,
            direction_code TEXT NOT NULL,
            direction_text TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            source TEXT NOT NULL,
            pattern_tag TEXT,
            reason_text TEXT,
            input_payload_json TEXT,
            output_json TEXT,
            is_observe INTEGER NOT NULL,
            is_fallback INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS risk_records (
            risk_record_id TEXT PRIMARY KEY,
            round_key TEXT NOT NULL,
            decision_id TEXT,
            phase TEXT NOT NULL,
            layer_code TEXT NOT NULL,
            layer_text TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            action TEXT NOT NULL,
            tier_cap TEXT,
            pause_rounds INTEGER,
            recheck_after INTEGER,
            reason_code TEXT NOT NULL,
            reason_text TEXT NOT NULL,
            metrics_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS execution_records (
            execution_id TEXT PRIMARY KEY,
            round_key TEXT NOT NULL,
            decision_id TEXT,
            bet_id TEXT,
            action_type TEXT NOT NULL,
            action_text TEXT NOT NULL,
            blocked_by TEXT,
            preset_name TEXT,
            bet_amount INTEGER,
            bet_hand_index INTEGER,
            current_round_no INTEGER,
            note TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settlements (
            settle_id TEXT PRIMARY KEY,
            round_key TEXT NOT NULL,
            decision_id TEXT,
            bet_id TEXT,
            settled_at TEXT NOT NULL,
            history_index INTEGER NOT NULL,
            result_num INTEGER,
            result_side TEXT,
            is_win INTEGER NOT NULL,
            profit INTEGER NOT NULL,
            fund_before INTEGER,
            fund_after INTEGER,
            balance_before INTEGER,
            balance_after INTEGER,
            lose_count_before INTEGER,
            lose_count_after INTEGER,
            streak_label TEXT
        );
        CREATE TABLE IF NOT EXISTS regime_features (
            round_key TEXT PRIMARY KEY,
            feature_version TEXT NOT NULL,
            w5_switch_rate REAL NOT NULL,
            w10_switch_rate REAL NOT NULL,
            w20_switch_rate REAL NOT NULL,
            w40_switch_rate REAL NOT NULL,
            w5_big_ratio REAL NOT NULL,
            w10_big_ratio REAL NOT NULL,
            w40_big_ratio REAL NOT NULL,
            tail_streak_len INTEGER NOT NULL,
            tail_streak_side TEXT NOT NULL,
            gap_big_small INTEGER NOT NULL,
            trend_score REAL NOT NULL,
            chaos_score REAL NOT NULL,
            reversal_score REAL NOT NULL,
            regime_label TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS task_runs (
            task_event_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            task_name TEXT NOT NULL,
            run_id TEXT,
            round_key TEXT,
            decision_id TEXT,
            bet_id TEXT,
            event_type TEXT NOT NULL,
            trigger_mode TEXT,
            base_preset TEXT,
            applied_preset TEXT,
            status_text TEXT,
            progress_bets INTEGER NOT NULL,
            target_bets INTEGER NOT NULL,
            profit_delta INTEGER NOT NULL,
            cum_profit INTEGER NOT NULL,
            cum_loss INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS package_runs (
            package_event_id TEXT PRIMARY KEY,
            package_id TEXT NOT NULL,
            package_name TEXT NOT NULL,
            run_id TEXT,
            task_id TEXT,
            task_name TEXT,
            round_key TEXT,
            event_type TEXT NOT NULL,
            status_text TEXT,
            progress_switches INTEGER NOT NULL,
            active_task_count INTEGER NOT NULL,
            profit_delta INTEGER NOT NULL,
            cum_profit INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_rounds_history_index ON rounds(history_index);
        CREATE INDEX IF NOT EXISTS idx_decisions_round_key ON decisions(round_key);
        CREATE INDEX IF NOT EXISTS idx_risk_round_key ON risk_records(round_key);
        CREATE INDEX IF NOT EXISTS idx_execution_round_key ON execution_records(round_key);
        CREATE INDEX IF NOT EXISTS idx_settlements_round_key ON settlements(round_key);
        CREATE INDEX IF NOT EXISTS idx_task_runs_task_id ON task_runs(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_runs_created_at ON task_runs(created_at);
        CREATE INDEX IF NOT EXISTS idx_package_runs_package_id ON package_runs(package_id);
        CREATE INDEX IF NOT EXISTS idx_package_runs_created_at ON package_runs(created_at);
        """
    )


def _write_analytics(user_ctx, statements: List[Tuple[str, Tuple[Any, ...]]]) -> None:
    db_path = _analytics_db_path(user_ctx)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_analytics_schema(conn)
        for sql, params in statements:
            conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def record_analysis_snapshot(user_ctx, snapshot: Dict[str, Any]) -> None:
    rt = user_ctx.state.runtime
    history = _normalize_history(getattr(user_ctx.state, "history", []))
    result_num = history[-1] if history else None
    features = snapshot.get("features", {}) if isinstance(snapshot.get("features", {}), dict) else {}
    round_key = str(snapshot.get("round_key", "") or build_round_key(rt, len(history) - 1))
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT OR REPLACE INTO rounds (
                    round_key, user_id, account_name, history_index, issue_no, result_num, result_side,
                    captured_at, board_5, board_10, board_20, board_40, current_round_no, current_hand_no
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_key,
                    _safe_int_value(user_ctx.user_id),
                    str(getattr(user_ctx.config, "name", "") or f"user-{user_ctx.user_id}"),
                    _safe_int_value(snapshot.get("history_index", len(history) - 1), len(history) - 1),
                    "",
                    result_num,
                    _normalize_result_side(result_num),
                    str(snapshot.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))),
                    str(snapshot.get("board_5", "-")),
                    str(snapshot.get("board_10", "-")),
                    str(snapshot.get("board_20", "-")),
                    str(snapshot.get("board_40", "-")),
                    _safe_int_value(rt.get("current_round", 1), 1),
                    _safe_int_value(rt.get("current_bet_seq", 1), 1),
                ),
            ),
            (
                """
                INSERT OR REPLACE INTO regime_features (
                    round_key, feature_version, w5_switch_rate, w10_switch_rate, w20_switch_rate, w40_switch_rate,
                    w5_big_ratio, w10_big_ratio, w40_big_ratio, tail_streak_len, tail_streak_side,
                    gap_big_small, trend_score, chaos_score, reversal_score, regime_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_key,
                    "v1",
                    float(features.get("w5_switch_rate", 0.0) or 0.0),
                    float(features.get("w10_switch_rate", 0.0) or 0.0),
                    float(features.get("w20_switch_rate", 0.0) or 0.0),
                    float(features.get("w40_switch_rate", 0.0) or 0.0),
                    float(features.get("w5_big_ratio", 0.5) or 0.5),
                    float(features.get("w10_big_ratio", 0.5) or 0.5),
                    float(features.get("w40_big_ratio", 0.5) or 0.5),
                    _safe_int_value(features.get("tail_streak_len", 0), 0),
                    str(features.get("tail_streak_side", "neutral") or "neutral"),
                    _safe_int_value(features.get("gap_big_small", 0), 0),
                    float(features.get("trend_score", 0.0) or 0.0),
                    float(features.get("chaos_score", 0.0) or 0.0),
                    float(features.get("reversal_score", 0.0) or 0.0),
                    str(snapshot.get("regime_label", REGIME_RANGE) or REGIME_RANGE),
                ),
            ),
        ],
    )


def record_decision_audit(user_ctx, audit_log: Dict[str, Any]) -> None:
    rt = user_ctx.state.runtime
    output = audit_log.get("output", {}) if isinstance(audit_log.get("output", {}), dict) else {}
    prediction = _safe_int_value(output.get("prediction", -1), -1)
    direction_code = "observe" if prediction == -1 else ("big" if prediction == 1 else "small")
    direction_text = "观望" if prediction == -1 else ("大" if prediction == 1 else "小")
    source = str(audit_log.get("prediction_source", "") or "")
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT OR REPLACE INTO decisions (
                    decision_id, round_key, decision_time, mode, model_id, prediction, direction_code,
                    direction_text, confidence, source, pattern_tag, reason_text, input_payload_json,
                    output_json, is_observe, is_fallback
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(audit_log.get("decision_id", "") or ""),
                    str(rt.get("current_round_key", "") or build_round_key(rt, len(_normalize_history(user_ctx.state.history)) - 1)),
                    str(audit_log.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))),
                    str(audit_log.get("mode", "M-SMP") or "M-SMP"),
                    str(audit_log.get("model_id", "") or ""),
                    prediction,
                    direction_code,
                    direction_text,
                    _safe_int_value(output.get("confidence", 0), 0),
                    source,
                    str(audit_log.get("pattern_tag", "") or ""),
                    str(output.get("reason", output.get("logic", "")) or ""),
                    _json_text(audit_log.get("input_payload", {})),
                    _json_text(output),
                    1 if prediction == -1 else 0,
                    0 if source in {"model", "model_skip"} else 1,
                ),
            ),
        ],
    )


def record_risk_action(
    user_ctx,
    *,
    phase: str,
    layer_code: str,
    enabled: bool,
    action: str,
    reason_code: str,
    reason_text: str,
    tier_cap: str = "",
    pause_rounds: int = 0,
    recheck_after: int = 0,
    metrics: Optional[Dict[str, Any]] = None,
) -> None:
    rt = user_ctx.state.runtime
    labels = {
        "fk1": "盘面风控",
        "fk2": "入场风控",
        "fk3": "连输风控",
        "fund": "资金风控",
        "strategy": "策略观望",
    }
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT INTO risk_records (
                    risk_record_id, round_key, decision_id, phase, layer_code, layer_text, enabled,
                    action, tier_cap, pause_rounds, recheck_after, reason_code, reason_text,
                    metrics_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"risk_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
                    str(rt.get("current_round_key", "") or ""),
                    str(rt.get("last_decision_id", "") or ""),
                    str(phase or "pre_bet"),
                    str(layer_code or ""),
                    labels.get(str(layer_code or ""), str(layer_code or "")),
                    1 if enabled else 0,
                    str(action or ""),
                    str(tier_cap or ""),
                    _safe_int_value(pause_rounds, 0),
                    _safe_int_value(recheck_after, 0),
                    str(reason_code or f"{layer_code}_{action}"),
                    str(reason_text or ""),
                    _json_text(metrics or {}),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ),
        ],
    )


def record_execution_action(
    user_ctx,
    *,
    action_type: str,
    action_text: str,
    blocked_by: str = "",
    bet_id: str = "",
    preset_name: str = "",
    bet_amount: int = 0,
    note: str = "",
) -> None:
    rt = user_ctx.state.runtime
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT INTO execution_records (
                    execution_id, round_key, decision_id, bet_id, action_type, action_text, blocked_by,
                    preset_name, bet_amount, bet_hand_index, current_round_no, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"exec_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
                    str(rt.get("current_round_key", "") or ""),
                    str(rt.get("last_decision_id", "") or ""),
                    str(bet_id or ""),
                    str(action_type or ""),
                    str(action_text or ""),
                    str(blocked_by or ""),
                    str(preset_name or rt.get("current_preset_name", "")),
                    _safe_int_value(bet_amount, 0),
                    _safe_int_value(rt.get("current_bet_seq", 1), 1),
                    _safe_int_value(rt.get("current_round", 1), 1),
                    str(note or ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ),
        ],
    )


def record_settlement(user_ctx, settled_entry: Dict[str, Any], result_num: Any, result_type: Any) -> None:
    rt = user_ctx.state.runtime
    profit = _safe_int_value(settled_entry.get("profit", 0), 0)
    lose_after = _safe_int_value(rt.get("lose_count", 0), 0)
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT INTO settlements (
                    settle_id, round_key, decision_id, bet_id, settled_at, history_index, result_num,
                    result_side, is_win, profit, fund_before, fund_after, balance_before, balance_after,
                    lose_count_before, lose_count_after, streak_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"stl_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
                    str(settled_entry.get("round_key", rt.get("current_round_key", "")) or ""),
                    str(settled_entry.get("decision_id", "") or ""),
                    str(settled_entry.get("bet_id", "") or ""),
                    str(settled_entry.get("settled_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))),
                    _safe_int_value(settled_entry.get("settle_history_index", len(_normalize_history(user_ctx.state.history)) - 1), 0),
                    _safe_int_value(result_num, 0),
                    _normalize_result_side(result_num, result_type),
                    1 if profit > 0 else 0,
                    profit,
                    _safe_int_value(settled_entry.get("fund_before", 0), 0),
                    _safe_int_value(rt.get("gambling_fund", settled_entry.get("fund_after", 0)), 0),
                    _safe_int_value(settled_entry.get("balance_before", 0), 0),
                    _safe_int_value(rt.get("account_balance", settled_entry.get("balance_after", 0)), 0),
                    _safe_int_value(settled_entry.get("lose_count_before", 0), 0),
                    lose_after,
                    "recover" if profit > 0 else (f"lose_{lose_after}" if lose_after > 0 else "flat"),
                ),
            ),
        ],
    )


def record_task_event(
    user_ctx,
    *,
    task_id: str,
    task_name: str,
    run_id: str = "",
    round_key: str = "",
    decision_id: str = "",
    bet_id: str = "",
    event_type: str,
    trigger_mode: str = "",
    base_preset: str = "",
    applied_preset: str = "",
    status_text: str = "",
    progress_bets: int = 0,
    target_bets: int = 0,
    profit_delta: int = 0,
    cum_profit: int = 0,
    cum_loss: int = 0,
    note: str = "",
) -> None:
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT INTO task_runs (
                    task_event_id, task_id, task_name, run_id, round_key, decision_id, bet_id,
                    event_type, trigger_mode, base_preset, applied_preset, status_text,
                    progress_bets, target_bets, profit_delta, cum_profit, cum_loss, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"task_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
                    str(task_id or ""),
                    str(task_name or ""),
                    str(run_id or ""),
                    str(round_key or ""),
                    str(decision_id or ""),
                    str(bet_id or ""),
                    str(event_type or ""),
                    str(trigger_mode or ""),
                    str(base_preset or ""),
                    str(applied_preset or ""),
                    str(status_text or ""),
                    _safe_int_value(progress_bets, 0),
                    _safe_int_value(target_bets, 0),
                    _safe_int_value(profit_delta, 0),
                    _safe_int_value(cum_profit, 0),
                    _safe_int_value(cum_loss, 0),
                    str(note or ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ),
        ],
    )


def record_package_event(
    user_ctx,
    *,
    package_id: str,
    package_name: str,
    run_id: str = "",
    task_id: str = "",
    task_name: str = "",
    round_key: str = "",
    event_type: str,
    status_text: str = "",
    progress_switches: int = 0,
    active_task_count: int = 0,
    profit_delta: int = 0,
    cum_profit: int = 0,
    note: str = "",
) -> None:
    _write_analytics(
        user_ctx,
        [
            (
                """
                INSERT INTO package_runs (
                    package_event_id, package_id, package_name, run_id, task_id, task_name, round_key,
                    event_type, status_text, progress_switches, active_task_count, profit_delta, cum_profit, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"pkg_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
                    str(package_id or ""),
                    str(package_name or ""),
                    str(run_id or ""),
                    str(task_id or ""),
                    str(task_name or ""),
                    str(round_key or ""),
                    str(event_type or ""),
                    str(status_text or ""),
                    _safe_int_value(progress_switches, 0),
                    _safe_int_value(active_task_count, 0),
                    _safe_int_value(profit_delta, 0),
                    _safe_int_value(cum_profit, 0),
                    str(note or ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ),
        ],
    )
