from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from typing import Any, Dict, List, Optional

import history_analysis
import policy_engine


LEARNING_CENTER_VERSION = 1
LEARNING_STATUS_GENERATED = "generated"
LEARNING_STATUS_EVALUATED = "evaluated"
LEARNING_STATUS_SHADOW = "shadow"
LEARNING_STATUS_GRAY = "gray"
LEARNING_STATUS_PROMOTED = "promoted"
LEARNING_STATUS_ROLLED_BACK = "rolled_back"


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
        "当前实现：H1/H2 已启用（候选中心 / 规则生成）",
        f"当前策略：{active_policy.get('policy_id', '')}@{active_policy.get('policy_version', '')} ({active_policy.get('policy_mode', '')})",
        f"候选总数：{len(candidates)}",
        f"状态分布：{status_text}",
        f"最近生成：{center.get('last_generated_at', '') or '-'}",
        f"最近候选：{latest.get('candidate_id', '') or '-'}",
        f"候选摘要：{latest.get('summary', '') or '-'}",
        "",
        "命令：`learn` / `learn gen` / `learn list` / `learn show <id|cX>`",
        "后续：`learn eval` / `learn shadow` / `learn gray` / `learn promote` / `learn rollback`",
    ]
    return "\n".join(lines)


def build_learning_list_text(user_ctx) -> str:
    center = load_learning_center(user_ctx)
    candidates = _sorted_candidates(center)
    if not candidates:
        return "🧠 学习候选列表\n\n暂无学习候选\n\n生成：`learn gen`"
    lines = ["🧠 学习候选列表", ""]
    for item in candidates[-10:]:
        lines.append(
            f"- {item.get('candidate_version', '')} | {item.get('rule_name', '')} | {item.get('status', '')} | {item.get('summary', '')}"
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
    return "\n".join(lines)


def build_learning_pending_text(subcmd: str) -> str:
    mapping = {
        "eval": "H3 离线评估器",
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
