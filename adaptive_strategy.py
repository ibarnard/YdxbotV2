import math
from typing import Any, Dict, List, Tuple

import constants


REGIME_TREND_CONTINUATION = "TREND_CONTINUATION"
REGIME_TREND_EXHAUSTION = "TREND_EXHAUSTION"
REGIME_REVERSAL_SETUP = "REVERSAL_SETUP"
REGIME_RANGE = "RANGE"
REGIME_CHAOS = "CHAOS"

REGIME_VALUES = {
    REGIME_TREND_CONTINUATION,
    REGIME_TREND_EXHAUSTION,
    REGIME_REVERSAL_SETUP,
    REGIME_RANGE,
    REGIME_CHAOS,
}

PRESET_LADDER = ["yc1", "yc5", "yc10", "yc20", "yc50", "yc100", "yc200"]


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


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _tail_streak(values: List[int]) -> Tuple[int, int]:
    if not values:
        return 0, -1
    last = values[-1]
    count = 1
    for i in range(len(values) - 2, -1, -1):
        if values[i] != last:
            break
        count += 1
    return count, int(last)


def _switch_rate(values: List[int]) -> float:
    if len(values) < 2:
        return 0.0
    switches = 0
    for i in range(1, len(values)):
        if values[i] != values[i - 1]:
            switches += 1
    return switches / float(len(values) - 1)


def _ratio(values: List[int]) -> float:
    if not values:
        return 0.5
    return sum(1 for x in values if x == 1) / float(len(values))


def _window(data: List[int], size: int) -> List[int]:
    if size <= 0:
        return []
    return data[-size:] if len(data) >= size else list(data)


def compute_regime(history: List[int]) -> Dict[str, Any]:
    w5 = _window(history, 5)
    w10 = _window(history, 10)
    w40 = _window(history, 40)
    w120 = _window(history, 120)
    w500 = _window(history, 500)

    r5 = _ratio(w5)
    r10 = _ratio(w10)
    r40 = _ratio(w40)
    r120 = _ratio(w120)
    r500 = _ratio(w500)

    s5 = _switch_rate(w5)
    s10 = _switch_rate(w10)
    s40 = _switch_rate(w40)
    s500 = _switch_rate(w500)

    tail_len, tail_side = _tail_streak(w40 if w40 else history)

    drift_score = abs(r40 - r500) + 0.6 * abs(s40 - s500)
    if drift_score >= 0.25:
        drift_band = "high"
    elif drift_score >= 0.12:
        drift_band = "medium"
    else:
        drift_band = "low"

    regime = REGIME_CHAOS
    confidence = 0.58

    if tail_len >= 6 and abs(r40 - 0.5) >= 0.18:
        regime = REGIME_TREND_EXHAUSTION
        confidence = 0.78
    elif tail_len >= 4 and s40 <= 0.42:
        regime = REGIME_TREND_CONTINUATION
        confidence = 0.74
    elif s40 >= 0.62 and tail_len <= 2:
        regime = REGIME_RANGE
        confidence = 0.71
    elif (tail_len in (2, 3)) and (abs(r10 - r40) >= 0.15):
        regime = REGIME_REVERSAL_SETUP
        confidence = 0.69

    confidence = _clamp(confidence + (0.04 if len(history) >= 120 else 0.0), 0.5, 0.9)
    return {
        "regime": regime,
        "confidence": round(confidence, 3),
        "drift_score": round(drift_score, 4),
        "drift_band": drift_band,
        "tail_streak_len": tail_len,
        "tail_streak_side": tail_side,
        "features": {
            "r5": round(r5, 4),
            "r10": round(r10, 4),
            "r40": round(r40, 4),
            "r120": round(r120, 4),
            "r500": round(r500, 4),
            "s5": round(s5, 4),
            "s10": round(s10, 4),
            "s40": round(s40, 4),
            "s500": round(s500, 4),
            "tail_len": tail_len,
            "tail_side": tail_side,
        },
    }


def _infer_preset_from_entry(entry: Dict[str, Any]) -> str:
    preset = str(entry.get("preset", "") or "")
    if preset:
        return preset

    amount = _safe_int(entry.get("amount", 0), 0)
    if amount <= 0:
        return ""
    best_name = ""
    best_gap = 10**18
    for name, vals in constants.PRESETS.items():
        if not isinstance(vals, list) or len(vals) < 7:
            continue
        base_amount = _safe_int(vals[6], 0)
        if base_amount <= 0:
            continue
        gap = abs(base_amount - amount)
        if gap < best_gap:
            best_gap = gap
            best_name = str(name)
    return best_name


def _is_settled_result(value: Any) -> bool:
    text = str(value or "").strip()
    return text in {"赢", "输", "寮傚父鏈粨绠?", "异常未结算"}


def _is_win_result(value: Any) -> bool:
    text = str(value or "").strip()
    return text == "赢"


def build_historical_cases(user_ctx: Any, max_cases: int = 1200) -> List[Dict[str, Any]]:
    state = getattr(user_ctx, "state", None)
    history = list(getattr(state, "history", []) or [])
    logs = list(getattr(state, "bet_sequence_log", []) or [])
    if not history or not logs:
        return []

    cases: List[Dict[str, Any]] = []
    settled_counter = -1
    history_len = len(history)

    for entry in logs:
        if not isinstance(entry, dict):
            continue
        result = entry.get("result")
        if not _is_settled_result(result):
            continue
        settled_counter += 1

        settle_idx = _safe_int(entry.get("settle_history_index", -1), -1)
        if settle_idx < 0 or settle_idx >= history_len:
            settle_idx = min(settled_counter, history_len - 1)
        if settle_idx <= 5:
            continue

        pre_data = history[max(0, settle_idx - 40):settle_idx]
        if len(pre_data) < 5:
            continue

        sig = compute_regime(pre_data)
        cases.append(
            {
                "history_index": settle_idx,
                "regime": str(sig.get("regime", REGIME_CHAOS)),
                "features": sig.get("features", {}),
                "drift_band": sig.get("drift_band", "medium"),
                "preset": _infer_preset_from_entry(entry),
                "profit": _safe_int(entry.get("profit", 0), 0),
                "amount": _safe_int(entry.get("amount", 0), 0),
                "win": _is_win_result(result),
            }
        )

    if len(cases) > max_cases:
        return cases[-max_cases:]
    return cases


def _similarity_score(current_features: Dict[str, Any], case_features: Dict[str, Any]) -> float:
    keys = [
        ("r5", 2.0),
        ("r10", 2.0),
        ("r40", 2.0),
        ("s5", 1.5),
        ("s10", 1.5),
        ("s40", 1.5),
    ]
    distance = 0.0
    total_w = 0.0
    for key, weight in keys:
        a = _safe_float(current_features.get(key, 0.5), 0.5)
        b = _safe_float(case_features.get(key, 0.5), 0.5)
        distance += weight * abs(a - b)
        total_w += weight

    tail_a = _safe_int(current_features.get("tail_len", 0), 0)
    tail_b = _safe_int(case_features.get("tail_len", 0), 0)
    distance += 0.6 * abs(tail_a - tail_b) / 10.0
    total_w += 0.6

    if total_w <= 0:
        return 0.0
    normalized = distance / total_w
    return _clamp(1.0 - normalized, 0.0, 1.0)


def retrieve_top_k_cases(current_sig: Dict[str, Any], cases: List[Dict[str, Any]], k: int = 50) -> List[Dict[str, Any]]:
    current_features = current_sig.get("features", {})
    scored = []
    for case in cases:
        score = _similarity_score(current_features, case.get("features", {}))
        if score <= 0:
            continue
        item = dict(case)
        item["similarity"] = round(score, 4)
        scored.append(item)
    scored.sort(key=lambda x: x.get("similarity", 0.0), reverse=True)
    return scored[: max(1, _safe_int(k, 50))]


def summarize_by_preset(cases: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for case in cases:
        name = str(case.get("preset", "") or "")
        if not name:
            continue
        grouped.setdefault(name, []).append(case)

    summary: Dict[str, Dict[str, Any]] = {}
    for name, items in grouped.items():
        sample = len(items)
        wins = sum(1 for x in items if bool(x.get("win", False)))
        profits = [_safe_int(x.get("profit", 0), 0) for x in items]
        rois: List[float] = []
        for x in items:
            amount = _safe_int(x.get("amount", 0), 0)
            profit = _safe_int(x.get("profit", 0), 0)
            if amount > 0:
                rois.append(profit / float(amount))

        ordered = sorted(items, key=lambda x: _safe_int(x.get("history_index", 0), 0))
        equity = 0
        peak = 0
        max_dd = 0
        for x in ordered:
            equity += _safe_int(x.get("profit", 0), 0)
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)

        summary[name] = {
            "sample": sample,
            "win_rate": wins / float(sample) if sample else 0.0,
            "avg_profit": sum(profits) / float(sample) if sample else 0.0,
            "avg_roi": sum(rois) / float(len(rois)) if rois else 0.0,
            "max_dd": max_dd,
        }
    return summary


def _tier_index(name: str) -> int:
    try:
        return PRESET_LADDER.index(name)
    except ValueError:
        return -1


def _high_tier_allowed(
    preset_name: str,
    preset_stat: Dict[str, Any],
    confidence: float,
    task_cfg: Dict[str, Any],
    run_max_dd: int,
    run_loss_limit: int,
) -> bool:
    if preset_name not in {"yc100", "yc200"}:
        return True

    sample_need = _safe_int(task_cfg.get("high_tier_sample_min", 120), 120)
    conf_need = _safe_float(task_cfg.get("high_tier_conf_min", 0.78), 0.78)
    win_need = _safe_float(task_cfg.get("high_tier_win_rate_min", 0.57), 0.57)
    dd_ratio_max = _safe_float(task_cfg.get("high_tier_dd_ratio_max", 0.4), 0.4)

    sample = _safe_int(preset_stat.get("sample", 0), 0)
    win_rate = _safe_float(preset_stat.get("win_rate", 0.0), 0.0)
    if run_loss_limit > 0:
        dd_ratio = _safe_int(run_max_dd, 0) / float(run_loss_limit)
    else:
        dd_ratio = 1.0

    return (
        sample >= sample_need
        and confidence >= conf_need
        and win_rate >= win_need
        and dd_ratio <= dd_ratio_max
    )


def adaptive_recheck_interval(drift_band: str) -> int:
    if drift_band == "high":
        return 2
    if drift_band == "medium":
        return 3
    return 5


def get_preset_initial_amount(preset_name: str) -> int:
    vals = constants.PRESETS.get(preset_name)
    if not isinstance(vals, list) or len(vals) < 7:
        return 500
    return _safe_int(vals[6], 500)


def build_recommendation(
    user_ctx: Any,
    task_cfg: Dict[str, Any],
    run_max_dd: int = 0,
    run_loss_limit: int = 0,
) -> Dict[str, Any]:
    history = list(getattr(getattr(user_ctx, "state", None), "history", []) or [])
    current_sig = compute_regime(history)
    cases = build_historical_cases(user_ctx)
    top_k = _safe_int(task_cfg.get("top_k_cases", 50), 50)
    top_cases = retrieve_top_k_cases(current_sig, cases, k=top_k)
    preset_stats = summarize_by_preset(top_cases)

    candidates = task_cfg.get("candidate_presets", PRESET_LADDER)
    candidate_names = [str(x) for x in candidates if str(x) in PRESET_LADDER]
    if not candidate_names:
        candidate_names = list(PRESET_LADDER)

    regime_conf = _safe_float(current_sig.get("confidence", 0.58), 0.58)
    drift_band = str(current_sig.get("drift_band", "medium"))
    regime = str(current_sig.get("regime", REGIME_CHAOS))

    best_name = candidate_names[0]
    best_score = -10**9
    score_details: Dict[str, float] = {}

    for name in candidate_names:
        stat = preset_stats.get(name, {})
        sample = _safe_int(stat.get("sample", 0), 0)
        win_rate = _safe_float(stat.get("win_rate", 0.5), 0.5)
        avg_roi = _safe_float(stat.get("avg_roi", 0.0), 0.0)
        avg_profit = _safe_float(stat.get("avg_profit", 0.0), 0.0)
        max_dd = _safe_float(stat.get("max_dd", 0.0), 0.0)
        amount = get_preset_initial_amount(name)
        dd_ratio = (max_dd / amount) if amount > 0 else 0.0

        # Utility =收益 + 稳定性 + 样本置信 - 回撤惩罚
        utility = (
            avg_roi * 120.0
            + (win_rate - 0.5) * 18.0
            + min(sample / 40.0, 3.0)
            + (avg_profit / max(amount, 1)) * 15.0
            - dd_ratio * 20.0
        )

        # 在高漂移时偏向低档位，低漂移时允许更高档位。
        tier = _tier_index(name)
        if drift_band == "high":
            utility -= max(0, tier - 2) * 1.3
        elif drift_band == "medium":
            utility -= max(0, tier - 4) * 0.6
        else:
            utility -= max(0, tier - 5) * 0.25

        # 置信度低时，高档位惩罚更强
        if regime_conf < 0.7:
            utility -= max(0, tier - 3) * (0.7 - regime_conf) * 8.0

        score_details[name] = round(utility, 4)
        if utility > best_score:
            best_score = utility
            best_name = name

    best_stat = preset_stats.get(best_name, {})
    if not _high_tier_allowed(
        best_name,
        best_stat,
        regime_conf,
        task_cfg,
        run_max_dd=run_max_dd,
        run_loss_limit=run_loss_limit,
    ):
        # 强制降档到不高于 yc20 的最高候选档位
        fallback = "yc20"
        for name in candidate_names:
            if _tier_index(name) <= _tier_index("yc20"):
                fallback = name
        best_name = fallback

    min_rounds = _safe_int(task_cfg.get("min_rounds", 6), 6)
    max_rounds = _safe_int(task_cfg.get("max_rounds", 30), 30)
    min_rounds = max(1, min_rounds)
    max_rounds = max(min_rounds, max_rounds)
    conf_norm = _clamp((regime_conf - 0.5) / 0.4, 0.0, 1.0)
    planned_rounds = int(round(min_rounds + (max_rounds - min_rounds) * conf_norm))
    if drift_band == "high":
        planned_rounds = max(min_rounds, int(math.floor(planned_rounds * 0.75)))
    elif drift_band == "low":
        planned_rounds = min(max_rounds, int(math.ceil(planned_rounds * 1.1)))

    return {
        "regime": regime,
        "regime_confidence": round(regime_conf, 3),
        "drift_band": drift_band,
        "drift_score": _safe_float(current_sig.get("drift_score", 0.0), 0.0),
        "recommended_preset": best_name,
        "planned_rounds": planned_rounds,
        "recheck_interval": adaptive_recheck_interval(drift_band),
        "top_cases_count": len(top_cases),
        "preset_scores": score_details,
        "preset_stats": preset_stats,
        "evidence": {
            "features": current_sig.get("features", {}),
            "top_cases": top_cases[:10],
        },
    }


def compute_loss_limits(current_fund: int, task_cfg: Dict[str, Any], preset_name: str) -> Dict[str, int]:
    current_fund = max(0, _safe_int(current_fund, 0))
    task_loss_pct = _safe_float(task_cfg.get("task_loss_pct", 0.006), 0.006)
    day_loss_pct = _safe_float(task_cfg.get("daily_loss_pct", 0.02), 0.02)

    preset_vals = constants.PRESETS.get(preset_name, constants.PRESETS.get("yc1", ["1", "14", "2.8", "2.3", "2.2", "2.05", "1000"]))
    lose_stop = _safe_int(preset_vals[1], 10) if len(preset_vals) >= 2 else 10
    base_amount = _safe_int(preset_vals[6], 1000) if len(preset_vals) >= 7 else 1000

    loss_limit_by_fund = int(current_fund * task_loss_pct)
    loss_limit_by_bet = int(3 * base_amount * max(lose_stop, 1))
    run_loss_limit = min(max(loss_limit_by_fund, 1), max(loss_limit_by_bet, 1))
    day_loss_limit = max(int(current_fund * day_loss_pct), run_loss_limit)
    return {
        "run_loss_limit": run_loss_limit,
        "day_loss_limit": day_loss_limit,
    }
