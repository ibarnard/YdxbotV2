from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from typing import Any, Dict, List, Optional

import history_analysis


POLICY_STORE_VERSION = 1

BASELINE_WRITEBACK_LINES = [
    "先判断方向，再决定是否下注和允许的档位；不要为了出手而忽略证据不足。",
    "证据冲突、样本不足、盘面混乱或近期实盘偏冷时，允许输出观望（SKIP=-1）。",
    "高档位只在延续盘、相似样本稳定且近期状态正常时考虑；其余场景优先低档或观望。",
    "不要为了追回亏损强行逆势；当高档位历史回撤偏大时，优先限档。",
]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _policy_store_path(user_ctx) -> str:
    return os.path.join(user_ctx.user_dir, "policy_versions.json")


def _default_policy_id(user_ctx) -> str:
    return f"pol_{getattr(user_ctx, 'user_id', 0)}_main"


def _safe_version_no(text: Any) -> int:
    raw = str(text or "").strip().lower()
    if raw.startswith("v"):
        raw = raw[1:]
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _sorted_policies(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    policies = [item for item in store.get("policies", []) if isinstance(item, dict)]
    return sorted(policies, key=lambda item: _safe_version_no(item.get("policy_version", "")))


def _find_policy_version(store: Dict[str, Any], ident: str = "") -> Optional[Dict[str, Any]]:
    policies = _sorted_policies(store)
    if not policies:
        return None
    target = str(ident or "").strip().lower()
    if not target:
        target = str(store.get("active_version", "") or "").strip().lower()
    for item in policies:
        if str(item.get("policy_version", "") or "").strip().lower() == target:
            return item
    return None


def _render_prompt_fragment(summary: str, writeback_lines: List[str]) -> str:
    clean_lines = [str(line or "").strip() for line in writeback_lines if str(line or "").strip()]
    bullet_lines = "\n".join(f"- {line}" for line in clean_lines[:4])
    if bullet_lines:
        return f"策略摘要：{summary}\n策略写回：\n{bullet_lines}"
    return f"策略摘要：{summary}"


def _baseline_policy(user_ctx) -> Dict[str, Any]:
    created_at = _now_text()
    summary = "基线策略：顺势优先，证据不足可观望，高档位谨慎使用"
    return {
        "policy_id": _default_policy_id(user_ctx),
        "policy_version": "v1",
        "source": "baseline",
        "activation_mode": "baseline",
        "status": "active",
        "created_at": created_at,
        "activated_at": created_at,
        "based_on_version": "",
        "summary": summary,
        "writeback_lines": list(BASELINE_WRITEBACK_LINES),
        "prompt_fragment": _render_prompt_fragment(summary, BASELINE_WRITEBACK_LINES),
        "evidence_hash": "baseline",
        "evidence_package": {
            "generated_at": created_at,
            "kind": "baseline",
            "current_regime": "未评估",
            "overview_24h": {},
        },
    }


def _default_policy_store(user_ctx) -> Dict[str, Any]:
    baseline = _baseline_policy(user_ctx)
    return {
        "version": POLICY_STORE_VERSION,
        "policy_id": baseline["policy_id"],
        "active_version": baseline["policy_version"],
        "previous_version": "",
        "last_synced_at": baseline["created_at"],
        "policies": [baseline],
    }


def _write_policy_store(user_ctx, store: Dict[str, Any]) -> None:
    path = _policy_store_path(user_ctx)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)


def load_policy_store(user_ctx) -> Dict[str, Any]:
    path = _policy_store_path(user_ctx)
    if not os.path.exists(path):
        store = _default_policy_store(user_ctx)
        _write_policy_store(user_ctx, store)
        return store

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("policy_versions.json 必须为对象")
    except Exception:
        store = _default_policy_store(user_ctx)
        _write_policy_store(user_ctx, store)
        return store

    store = {
        "version": int(payload.get("version", POLICY_STORE_VERSION) or POLICY_STORE_VERSION),
        "policy_id": str(payload.get("policy_id", _default_policy_id(user_ctx)) or _default_policy_id(user_ctx)),
        "active_version": str(payload.get("active_version", "") or ""),
        "previous_version": str(payload.get("previous_version", "") or ""),
        "last_synced_at": str(payload.get("last_synced_at", "") or ""),
        "policies": [item for item in payload.get("policies", []) if isinstance(item, dict)],
    }

    if not store["policies"]:
        store = _default_policy_store(user_ctx)
        _write_policy_store(user_ctx, store)
        return store

    if not _find_policy_version(store, store["active_version"]):
        first_policy = _sorted_policies(store)[-1]
        store["active_version"] = str(first_policy.get("policy_version", "v1") or "v1")

    return store


def _update_runtime_policy_snapshot(user_ctx, store: Dict[str, Any], active_policy: Optional[Dict[str, Any]]) -> None:
    rt = user_ctx.state.runtime
    active = active_policy or _find_policy_version(store, store.get("active_version", ""))
    if not active:
        rt["policy_active_id"] = ""
        rt["policy_active_version"] = ""
        rt["policy_active_mode"] = ""
        rt["policy_last_summary"] = ""
        rt["policy_prompt_fragment"] = ""
        return
    rt["policy_active_id"] = str(active.get("policy_id", store.get("policy_id", "")) or "")
    rt["policy_active_version"] = str(active.get("policy_version", "") or "")
    rt["policy_active_mode"] = str(active.get("activation_mode", "baseline") or "baseline")
    rt["policy_last_summary"] = str(active.get("summary", "") or "")
    rt["policy_prompt_fragment"] = str(active.get("prompt_fragment", "") or "")
    rt["policy_last_synced_at"] = str(store.get("last_synced_at", "") or "")


def _format_evidence_summary(evidence: Dict[str, Any]) -> str:
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    temp = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    recommended = str(similar.get("recommended_tier_cap", "") or "")
    if recommended == "observe":
        recommended_text = "观望优先"
    elif recommended:
        recommended_text = f"限档 {recommended}"
    else:
        recommended_text = "未额外限档"
    temp_text = {
        "normal": "正常",
        "cold": "偏冷",
        "very_cold": "很冷",
    }.get(str(temp.get("level", "normal") or "normal"), "正常")
    return (
        f"{evidence.get('current_regime', '未评估')} | "
        f"{similar.get('evidence_strength', 'insufficient')} | "
        f"{recommended_text} | "
        f"24h {int(overview.get('settled_count', 0) or 0)} 笔 | "
        f"温度 {temp_text}"
    )


def _build_writeback_lines(evidence: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    current_regime = str(evidence.get("current_regime", "") or "")
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    temp = evidence.get("recent_temperature", {}) if isinstance(evidence.get("recent_temperature", {}), dict) else {}
    recommended = str(similar.get("recommended_tier_cap", "") or "")
    evidence_strength = str(similar.get("evidence_strength", "insufficient") or "insufficient")
    weighted_hit = float(similar.get("weighted_signal_hit_rate", 0.0) or 0.0)
    high_stats = similar.get("tiers", {}).get("high", {}) if isinstance(similar.get("tiers", {}), dict) else {}
    low_stats = similar.get("tiers", {}).get("low", {}) if isinstance(similar.get("tiers", {}), dict) else {}
    pnl24 = int(overview.get("pnl_total", 0) or 0)
    drawdown24 = int(overview.get("max_drawdown", 0) or 0)
    temp_level = str(temp.get("level", "normal") or "normal")

    if current_regime == "混乱盘":
        lines.append("当前为混乱盘时，若证据一般或样本不足，优先输出观望（SKIP=-1），不要勉强给方向。")
    elif current_regime == "反转盘":
        lines.append("当前为反转盘时，只在反转证据足够强时才逆势；否则优先低档试探或观望。")
    elif current_regime == "延续盘":
        lines.append("当前为延续盘时，顺势优先；只有当反转证据明显强于延续证据时才考虑逆势。")
    else:
        lines.append("震荡或衰竭环境下，方向证据不足时允许观望，不要用模糊信号强行下注。")

    if recommended == "observe":
        lines.append("相似历史整体偏弱且高低档都不稳定时，直接观望优于勉强出手。")
    elif recommended:
        lines.append(f"相似历史提示高档位风险偏大时，执行档位上限收敛到 {recommended}，不要继续放大。")
    elif evidence_strength == "strong" and weighted_hit >= 0.54:
        lines.append("相似历史较稳定时，可在顺势前提下给出明确方向，但仍保留证据不足时观望的权利。")

    if high_stats and float(high_stats.get("avg_pnl", 0.0) or 0.0) < 0:
        lines.append("高档位历史均收益为负时，不要为追求收益而放大；优先低档验证。")
    elif low_stats and float(low_stats.get("avg_pnl", 0.0) or 0.0) > 0 and recommended:
        lines.append("若低档历史可做而高档风险偏大，优先保持低档试探，不要跳级。")

    if temp_level == "very_cold":
        lines.append("近期实盘很冷时，把保守权重提高到最高，证据接近时直接观望。")
    elif temp_level == "cold":
        lines.append("近期实盘偏冷时，证据接近也应偏保守，避免逆势和高档位。")

    if pnl24 < 0 and drawdown24 > 0:
        lines.append("最近24小时收益为负且回撤存在时，优先控制回撤，不要为了回补而提高激进度。")

    lines.append("最终输出必须是 -1、0、1 之一；当把握不足时，观望优先于勉强下注。")

    deduped: List[str] = []
    for line in lines:
        if line not in deduped:
            deduped.append(line)
    return deduped[:5]


def _next_policy_version(store: Dict[str, Any]) -> str:
    version_no = 0
    for item in _sorted_policies(store):
        version_no = max(version_no, _safe_version_no(item.get("policy_version", "")))
    return f"v{version_no + 1}"


def _policy_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_policy_prompt_context(user_ctx, analysis_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    store = load_policy_store(user_ctx)
    active = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
    _update_runtime_policy_snapshot(user_ctx, store, active)
    evidence = history_analysis.build_policy_evidence_package(user_ctx, analysis_snapshot)
    return {
        "policy_id": str(active.get("policy_id", store.get("policy_id", "")) or ""),
        "policy_version": str(active.get("policy_version", "") or ""),
        "policy_mode": str(active.get("activation_mode", "baseline") or "baseline"),
        "policy_source": str(active.get("source", "baseline") or "baseline"),
        "policy_summary": str(active.get("summary", "") or ""),
        "prompt_fragment": str(active.get("prompt_fragment", "") or ""),
        "writeback_lines": list(active.get("writeback_lines", []) or []),
        "evidence_package": evidence,
        "evidence_summary": _format_evidence_summary(evidence),
    }


def sync_policy_from_evidence(user_ctx, analysis_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    store = load_policy_store(user_ctx)
    current = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
    evidence = history_analysis.build_policy_evidence_package(user_ctx, analysis_snapshot)
    writeback_lines = _build_writeback_lines(evidence)
    summary = _format_evidence_summary(evidence)
    prompt_fragment = _render_prompt_fragment(summary, writeback_lines)
    evidence_hash = _policy_hash(
        {
            "evidence": evidence,
            "lines": writeback_lines,
            "summary": summary,
        }
    )

    if (
        current
        and str(current.get("source", "")) == "writeback"
        and str(current.get("evidence_hash", "")) == evidence_hash
        and str(current.get("prompt_fragment", "")) == prompt_fragment
    ):
        _update_runtime_policy_snapshot(user_ctx, store, current)
        return {
            "ok": True,
            "changed": False,
            "policy": current,
            "message": f"🧠 策略版本无变化，继续使用 {current.get('policy_version', 'v1')}（{current.get('summary', '基线策略')}）",
        }

    version = _next_policy_version(store)
    activated_at = _now_text()
    policy = {
        "policy_id": str(store.get("policy_id", _default_policy_id(user_ctx)) or _default_policy_id(user_ctx)),
        "policy_version": version,
        "source": "writeback",
        "activation_mode": "gray",
        "status": "active",
        "created_at": activated_at,
        "activated_at": activated_at,
        "based_on_version": str(current.get("policy_version", "") if current else ""),
        "summary": summary,
        "writeback_lines": writeback_lines,
        "prompt_fragment": prompt_fragment,
        "evidence_hash": evidence_hash,
        "evidence_package": evidence,
    }
    store["previous_version"] = str(current.get("policy_version", "") if current else "")
    store["active_version"] = version
    store["last_synced_at"] = activated_at
    store.setdefault("policies", []).append(policy)
    _write_policy_store(user_ctx, store)
    _update_runtime_policy_snapshot(user_ctx, store, policy)
    try:
        history_analysis.record_policy_version(user_ctx, policy)
        history_analysis.record_policy_event(
            user_ctx,
            policy_id=policy["policy_id"],
            policy_version=version,
            event_type="sync_activate",
            reason="根据复盘证据生成并激活策略版本",
            previous_version=str(current.get("policy_version", "") if current else ""),
            payload=evidence,
        )
    except Exception:
        pass
    return {
        "ok": True,
        "changed": True,
        "policy": policy,
        "message": f"🧠 已生成并激活策略版本 {version}（灰度）\n摘要：{summary}",
    }


def activate_policy_version(user_ctx, version: str, reason: str = "手动切换") -> Dict[str, Any]:
    store = load_policy_store(user_ctx)
    target = _find_policy_version(store, version)
    if not target:
        return {"ok": False, "message": f"❌ 未找到策略版本 `{version}`"}
    current = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
    if current and str(current.get("policy_version", "")) == str(target.get("policy_version", "")):
        _update_runtime_policy_snapshot(user_ctx, store, target)
        return {"ok": True, "message": f"ℹ️ 当前已在使用 {target.get('policy_version', '')}"}

    store["previous_version"] = str(current.get("policy_version", "") if current else "")
    store["active_version"] = str(target.get("policy_version", "") or "")
    store["last_synced_at"] = _now_text()
    target["activated_at"] = store["last_synced_at"]
    _write_policy_store(user_ctx, store)
    _update_runtime_policy_snapshot(user_ctx, store, target)
    try:
        history_analysis.record_policy_version(user_ctx, target)
        history_analysis.record_policy_event(
            user_ctx,
            policy_id=str(target.get("policy_id", store.get("policy_id", "")) or ""),
            policy_version=str(target.get("policy_version", "") or ""),
            event_type="manual_activate",
            reason=str(reason or "手动切换"),
            previous_version=str(current.get("policy_version", "") if current else ""),
            payload={"summary": str(target.get("summary", "") or "")},
        )
    except Exception:
        pass
    return {
        "ok": True,
        "policy": target,
        "message": f"✅ 已切换到策略版本 {target.get('policy_version', '')}\n摘要：{target.get('summary', '')}",
    }


def rollback_policy(user_ctx) -> Dict[str, Any]:
    store = load_policy_store(user_ctx)
    previous = str(store.get("previous_version", "") or "").strip()
    if previous:
        return activate_policy_version(user_ctx, previous, reason="回滚到上一版本")
    policies = _sorted_policies(store)
    current = _find_policy_version(store, store.get("active_version", "")) or (policies[-1] if policies else None)
    if not current or len(policies) < 2:
        return {"ok": False, "message": "❌ 没有可回滚的上一策略版本"}
    current_index = policies.index(current)
    if current_index <= 0:
        return {"ok": False, "message": "❌ 当前已是最早策略版本，无法回滚"}
    return activate_policy_version(user_ctx, str(policies[current_index - 1].get("policy_version", "")), reason="回滚到上一版本")


def build_policy_focus_text(user_ctx) -> str:
    store = load_policy_store(user_ctx)
    active = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
    _update_runtime_policy_snapshot(user_ctx, store, active)
    mode_text = "基线" if str(active.get("activation_mode", "baseline")) == "baseline" else "灰度"
    version = str(active.get("policy_version", "") or "v1")
    return f"🧠 策略提醒：当前 `{version}`（{mode_text}），可用 `policy` 查看 / `policy sync` 生成回写版本"


def build_policy_overview_text(user_ctx) -> str:
    store = load_policy_store(user_ctx)
    active = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
    _update_runtime_policy_snapshot(user_ctx, store, active)
    mode_text = "基线" if str(active.get("activation_mode", "baseline")) == "baseline" else "灰度"
    lines = [
        "🧠 策略版本中心",
        "",
        f"策略ID：{store.get('policy_id', '')}",
        f"当前版本：{active.get('policy_version', '')}（{mode_text}）",
        f"上一版本：{store.get('previous_version', '') or '-'}",
        f"最近同步：{store.get('last_synced_at', '') or '-'}",
        f"摘要：{active.get('summary', '') or '基线策略'}",
        f"写回条数：{len(active.get('writeback_lines', []) or [])}",
        "",
        "用法：`policy list` / `policy show [vX]` / `policy sync` / `policy use <vX>` / `policy rollback`",
    ]
    return "\n".join(lines)


def build_policy_list_text(user_ctx) -> str:
    store = load_policy_store(user_ctx)
    active_version = str(store.get("active_version", "") or "")
    policies = _sorted_policies(store)
    if not policies:
        return "🧠 策略版本列表\n\n暂无策略版本"
    lines = ["🧠 策略版本列表", ""]
    for item in policies:
        version = str(item.get("policy_version", "") or "")
        marker = "👈 当前" if version == active_version else ""
        source = "基线" if str(item.get("source", "baseline")) == "baseline" else "回写"
        mode = "基线" if str(item.get("activation_mode", "baseline")) == "baseline" else "灰度"
        summary = str(item.get("summary", "") or "")
        lines.append(f"- {version} | {source}/{mode} {marker} | {summary}")
    return "\n".join(lines)


def build_policy_detail_text(user_ctx, ident: str = "") -> str:
    store = load_policy_store(user_ctx)
    policy = _find_policy_version(store, ident)
    if not policy:
        active = _find_policy_version(store, store.get("active_version", "")) or _sorted_policies(store)[-1]
        policy = active
    if not policy:
        return "🧠 策略版本详情\n\n暂无策略版本"
    evidence = policy.get("evidence_package", {}) if isinstance(policy.get("evidence_package", {}), dict) else {}
    similar = evidence.get("similar_cases", {}) if isinstance(evidence.get("similar_cases", {}), dict) else {}
    overview = evidence.get("overview_24h", {}) if isinstance(evidence.get("overview_24h", {}), dict) else {}
    writeback_lines = [str(item or "").strip() for item in policy.get("writeback_lines", []) if str(item or "").strip()]
    lines = [
        "🧠 策略版本详情",
        "",
        f"策略ID：{policy.get('policy_id', '')}",
        f"版本：{policy.get('policy_version', '')}",
        f"来源：{'基线' if str(policy.get('source', 'baseline')) == 'baseline' else '回写'}",
        f"模式：{'基线' if str(policy.get('activation_mode', 'baseline')) == 'baseline' else '灰度'}",
        f"创建时间：{policy.get('created_at', '') or '-'}",
        f"激活时间：{policy.get('activated_at', '') or '-'}",
        f"基于版本：{policy.get('based_on_version', '') or '-'}",
        f"摘要：{policy.get('summary', '') or '-'}",
        "",
        f"当前证据：{evidence.get('current_regime', '未评估')} | 相似样本 {similar.get('similar_count', 0)} | 24h {int(overview.get('settled_count', 0) or 0)} 笔",
        f"Prompt 片段：{policy.get('prompt_fragment', '') or '-'}",
        "",
        "写回规则：",
    ]
    if writeback_lines:
        lines.extend(f"- {line}" for line in writeback_lines)
    else:
        lines.append("- 暂无写回规则")
    return "\n".join(lines)
