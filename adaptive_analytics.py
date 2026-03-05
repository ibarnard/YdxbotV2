import hashlib
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


DB_FILE_NAME = "analytics.db"


def get_db_path(user_dir: str) -> str:
    return os.path.join(user_dir, DB_FILE_NAME)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(user_dir: str) -> str:
    os.makedirs(user_dir, exist_ok=True)
    db_path = get_db_path(user_dir)
    conn = _connect(db_path)
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS decision_events (
                event_id TEXT PRIMARY KEY,
                decision_id TEXT NOT NULL DEFAULT '',
                ts TEXT NOT NULL DEFAULT '',
                round INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                tag TEXT NOT NULL DEFAULT '',
                confidence INTEGER NOT NULL DEFAULT 0,
                prediction INTEGER NOT NULL DEFAULT -1,
                model_id TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_events_decision_id
            ON decision_events(decision_id) WHERE decision_id <> '';

            CREATE TABLE IF NOT EXISTS bet_events (
                event_id TEXT PRIMARY KEY,
                bet_id TEXT NOT NULL DEFAULT '',
                ts TEXT NOT NULL DEFAULT '',
                round INTEGER NOT NULL DEFAULT 0,
                sequence INTEGER NOT NULL DEFAULT 0,
                direction TEXT NOT NULL DEFAULT '',
                amount INTEGER NOT NULL DEFAULT 0,
                decision_id TEXT NOT NULL DEFAULT '',
                preset TEXT NOT NULL DEFAULT '',
                regime TEXT NOT NULL DEFAULT '',
                task_name TEXT NOT NULL DEFAULT '',
                task_run_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_events_bet_ts
            ON bet_events(bet_id, ts);

            CREATE TABLE IF NOT EXISTS settle_events (
                event_id TEXT PRIMARY KEY,
                bet_id TEXT NOT NULL DEFAULT '',
                ts TEXT NOT NULL DEFAULT '',
                result TEXT NOT NULL DEFAULT '',
                profit INTEGER NOT NULL DEFAULT 0,
                result_num INTEGER NOT NULL DEFAULT -1,
                result_type TEXT NOT NULL DEFAULT '',
                history_index INTEGER NOT NULL DEFAULT -1,
                decision_id TEXT NOT NULL DEFAULT '',
                link_status TEXT NOT NULL DEFAULT 'exact',
                link_score REAL NOT NULL DEFAULT 1.0,
                preset TEXT NOT NULL DEFAULT '',
                regime TEXT NOT NULL DEFAULT '',
                task_name TEXT NOT NULL DEFAULT '',
                task_run_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_settle_events_bet_ts_result
            ON settle_events(bet_id, ts, result, profit);

            CREATE TABLE IF NOT EXISTS task_runs (
                task_run_id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                policy_id TEXT NOT NULL DEFAULT '',
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'running',
                pnl INTEGER NOT NULL DEFAULT 0,
                max_dd INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS task_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id TEXT NOT NULL,
                step_no INTEGER NOT NULL DEFAULT 0,
                ts TEXT NOT NULL DEFAULT '',
                regime TEXT NOT NULL DEFAULT '',
                preset TEXT NOT NULL DEFAULT '',
                planned_rounds INTEGER NOT NULL DEFAULT 0,
                executed_rounds INTEGER NOT NULL DEFAULT 0,
                action_type TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                pnl INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_task_actions_run_id
            ON task_actions(task_run_id);

            CREATE TABLE IF NOT EXISTS regime_preset_stats (
                regime TEXT NOT NULL,
                preset TEXT NOT NULL,
                window TEXT NOT NULL,
                sample_size INTEGER NOT NULL DEFAULT 0,
                hit_rate REAL NOT NULL DEFAULT 0.0,
                avg_profit REAL NOT NULL DEFAULT 0.0,
                max_dd INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (regime, preset, window)
            );

            CREATE TABLE IF NOT EXISTS policy_versions (
                policy_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'candidate',
                shadow_metrics TEXT NOT NULL DEFAULT '',
                prod_metrics TEXT NOT NULL DEFAULT '',
                rollback_from TEXT NOT NULL DEFAULT ''
            );
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


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


def _event_id(prefix: str, payload: Dict[str, Any]) -> str:
    dumped = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha1(dumped.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    result: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except Exception:
                continue
            if isinstance(obj, dict):
                result.append(obj)
    return result


def _parse_ts(ts_text: str) -> Optional[float]:
    text = str(ts_text or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except Exception:
            continue
    return None


def _infer_preset_from_amount(amount: int) -> str:
    try:
        from constants import PRESETS
    except Exception:
        return ""

    amount_to_name: Dict[int, str] = {}
    for name, vals in PRESETS.items():
        if not isinstance(vals, list) or len(vals) < 7:
            continue
        initial_amount = _safe_int(vals[6], 0)
        if initial_amount > 0:
            amount_to_name[initial_amount] = str(name)

    if amount in amount_to_name:
        return amount_to_name[amount]
    if not amount_to_name:
        return ""

    ordered = sorted(amount_to_name.keys())
    nearest = min(ordered, key=lambda x: abs(x - amount))
    return amount_to_name.get(nearest, "")


def _build_legacy_decision_cache(conn: sqlite3.Connection) -> List[Tuple[float, str]]:
    rows = conn.execute(
        "SELECT ts, decision_id FROM decision_events WHERE decision_id <> '' ORDER BY ts ASC"
    ).fetchall()
    cache: List[Tuple[float, str]] = []
    for row in rows:
        ts_epoch = _parse_ts(row["ts"])
        if ts_epoch is None:
            continue
        cache.append((ts_epoch, str(row["decision_id"])))
    return cache


def _estimate_decision_id(
    placed_at: str,
    cache: List[Tuple[float, str]],
    max_gap_seconds: int = 300,
) -> Tuple[str, str, float]:
    ts = _parse_ts(placed_at)
    if ts is None or not cache:
        return "", "missing", 0.0

    candidate_id = ""
    candidate_gap = 10**9
    for dec_ts, dec_id in cache:
        gap = ts - dec_ts
        if gap < 0:
            continue
        if gap < candidate_gap:
            candidate_gap = gap
            candidate_id = dec_id

    if candidate_id and candidate_gap <= max_gap_seconds:
        score = max(0.0, 1.0 - candidate_gap / float(max_gap_seconds))
        return candidate_id, "estimated", score
    return "", "missing", 0.0


def ingest_user_history(user_ctx: Any) -> Dict[str, Any]:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    if not user_dir:
        return {"ok": False, "error": "user_dir missing"}

    db_path = ensure_schema(user_dir)
    conn = _connect(db_path)
    report = {
        "ok": True,
        "db_path": db_path,
        "decision_rows": 0,
        "bet_rows": 0,
        "settle_rows": 0,
        "sources": [],
    }

    try:
        replay_path = os.path.join(user_dir, "replay_events.log")
        replay_rows = list(_read_jsonl(replay_path))
        if replay_rows:
            report["sources"].append("replay_events.log")
        for row in replay_rows:
            ts = str(row.get("timestamp", "") or "")
            event_type = str(row.get("event_type", "") or "").lower()
            payload = row.get("payload", {}) if isinstance(row.get("payload"), dict) else {}
            raw_json = json.dumps(row, ensure_ascii=False)

            if event_type == "decision":
                decision_id = str(payload.get("decision_id", "") or "")
                event_id = decision_id or _event_id("decision", row)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO decision_events
                    (event_id, decision_id, ts, round, mode, source, tag, confidence, prediction, model_id, reason, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        decision_id,
                        ts,
                        _safe_int(payload.get("round", 0), 0),
                        str(payload.get("mode", "") or ""),
                        str(payload.get("source", "") or ""),
                        str(payload.get("tag", "") or ""),
                        _safe_int(payload.get("confidence", 0), 0),
                        _safe_int(payload.get("prediction", -1), -1),
                        str(payload.get("model_id", "") or ""),
                        str(payload.get("reason", "") or ""),
                        raw_json,
                    ),
                )
            elif event_type == "bet_placed":
                bet_id = str(payload.get("bet_id", "") or "")
                if not bet_id:
                    continue
                event_id = _event_id("bet", row)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO bet_events
                    (event_id, bet_id, ts, round, sequence, direction, amount, decision_id, preset, regime, task_name, task_run_id, status, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        bet_id,
                        ts,
                        _safe_int(payload.get("round", 0), 0),
                        _safe_int(payload.get("sequence", 0), 0),
                        str(payload.get("direction", "") or ""),
                        _safe_int(payload.get("amount", 0), 0),
                        str(payload.get("decision_id", "") or ""),
                        str(payload.get("preset", "") or ""),
                        str(payload.get("regime", "") or ""),
                        str(payload.get("task_name", "") or ""),
                        str(payload.get("task_run_id", "") or ""),
                        "placed",
                        raw_json,
                    ),
                )
            elif event_type == "bet_settled":
                bet_id = str(payload.get("bet_id", "") or "")
                if not bet_id:
                    continue
                event_id = _event_id("settle", row)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO settle_events
                    (event_id, bet_id, ts, result, profit, result_num, result_type, history_index, decision_id, link_status, link_score, preset, regime, task_name, task_run_id, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        bet_id,
                        ts,
                        str(payload.get("result", "") or ""),
                        _safe_int(payload.get("profit", 0), 0),
                        _safe_int(payload.get("settle_result_num", -1), -1),
                        str(payload.get("settle_result_type", "") or ""),
                        _safe_int(payload.get("history_index", -1), -1),
                        str(payload.get("decision_id", "") or ""),
                        "exact",
                        1.0,
                        str(payload.get("preset", "") or ""),
                        str(payload.get("regime", "") or ""),
                        str(payload.get("task_name", "") or ""),
                        str(payload.get("task_run_id", "") or ""),
                        raw_json,
                    ),
                )

        decisions_path = os.path.join(user_dir, "decisions.log")
        decision_rows = list(_read_jsonl(decisions_path))
        if decision_rows:
            report["sources"].append("decisions.log")
        for row in decision_rows:
            output = row.get("output", {}) if isinstance(row.get("output"), dict) else {}
            decision_id = str(row.get("decision_id", "") or "")
            if not decision_id:
                decision_id = _event_id("legacy_decision", row)
            event_id = decision_id
            conn.execute(
                """
                INSERT OR IGNORE INTO decision_events
                (event_id, decision_id, ts, round, mode, source, tag, confidence, prediction, model_id, reason, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    decision_id,
                    str(row.get("timestamp", "") or ""),
                    _safe_int(row.get("round", 0), 0),
                    str(row.get("mode", "") or ""),
                    str(row.get("prediction_source", "") or ""),
                    str(row.get("pattern_tag", "") or ""),
                    _safe_int(output.get("confidence", 0), 0),
                    _safe_int(output.get("prediction", -1), -1),
                    str(row.get("model_id", "") or ""),
                    str(output.get("reason", output.get("logic", "")) or ""),
                    json.dumps(row, ensure_ascii=False),
                ),
            )

        state_logs = getattr(getattr(user_ctx, "state", None), "bet_sequence_log", [])
        if isinstance(state_logs, list) and state_logs:
            report["sources"].append("state.bet_sequence_log")
        decision_cache = _build_legacy_decision_cache(conn)
        for entry in state_logs:
            if not isinstance(entry, dict):
                continue
            bet_id = str(entry.get("bet_id", "") or "")
            if not bet_id:
                continue

            amount = _safe_int(entry.get("amount", 0), 0)
            preset = str(entry.get("preset", "") or "") or _infer_preset_from_amount(amount)
            regime = str(entry.get("regime", "") or entry.get("decision_tag", "") or "")
            task_name = str(entry.get("task_name", "") or "")
            task_run_id = str(entry.get("task_run_id", "") or "")
            placed_at = str(entry.get("placed_at", "") or entry.get("timestamp", "") or "")
            decision_id = str(entry.get("decision_id", "") or "")

            if not decision_id:
                estimated_id, _, _ = _estimate_decision_id(placed_at, decision_cache)
                decision_id = estimated_id

            bet_event_payload = {
                "bet_id": bet_id,
                "placed_at": placed_at,
                "sequence": _safe_int(entry.get("sequence", 0), 0),
                "amount": amount,
                "decision_id": decision_id,
            }
            bet_event_id = _event_id("state_bet", bet_event_payload)
            conn.execute(
                """
                INSERT OR IGNORE INTO bet_events
                (event_id, bet_id, ts, round, sequence, direction, amount, decision_id, preset, regime, task_name, task_run_id, status, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bet_event_id,
                    bet_id,
                    placed_at,
                    _safe_int(entry.get("round", 0), 0),
                    _safe_int(entry.get("sequence", 0), 0),
                    str(entry.get("direction", "") or ""),
                    amount,
                    decision_id,
                    preset,
                    regime,
                    task_name,
                    task_run_id,
                    str(entry.get("status", "legacy") or "legacy"),
                    json.dumps(entry, ensure_ascii=False),
                ),
            )

            result = entry.get("result")
            if result is None:
                continue
            settled_at = str(entry.get("settled_at", "") or "")
            link_status = "exact"
            link_score = 1.0
            settle_decision_id = decision_id
            if not settle_decision_id:
                estimated_id, link_status, link_score = _estimate_decision_id(
                    str(entry.get("placed_at", "") or ""),
                    decision_cache,
                )
                settle_decision_id = estimated_id

            settle_event_payload = {
                "bet_id": bet_id,
                "settled_at": settled_at,
                "result": str(result),
                "profit": _safe_int(entry.get("profit", 0), 0),
                "decision_id": settle_decision_id,
            }
            settle_event_id = _event_id("state_settle", settle_event_payload)
            conn.execute(
                """
                INSERT OR IGNORE INTO settle_events
                (event_id, bet_id, ts, result, profit, result_num, result_type, history_index, decision_id, link_status, link_score, preset, regime, task_name, task_run_id, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    settle_event_id,
                    bet_id,
                    settled_at,
                    str(result),
                    _safe_int(entry.get("profit", 0), 0),
                    _safe_int(entry.get("settle_result_num", -1), -1),
                    str(entry.get("settle_result_type", "") or ""),
                    _safe_int(entry.get("settle_history_index", -1), -1),
                    settle_decision_id,
                    link_status,
                    _safe_float(link_score, 0.0),
                    preset,
                    regime,
                    task_name,
                    task_run_id,
                    json.dumps(entry, ensure_ascii=False),
                ),
            )

        conn.commit()
        report["decision_rows"] = _safe_int(
            conn.execute("SELECT COUNT(*) FROM decision_events").fetchone()[0], 0
        )
        report["bet_rows"] = _safe_int(
            conn.execute("SELECT COUNT(*) FROM bet_events").fetchone()[0], 0
        )
        report["settle_rows"] = _safe_int(
            conn.execute("SELECT COUNT(*) FROM settle_events").fetchone()[0], 0
        )
    finally:
        conn.close()
    return report


def linkage_coverage_report(user_ctx: Any) -> Dict[str, Any]:
    db_path = get_db_path(str(getattr(user_ctx, "user_dir", "") or ""))
    if not os.path.exists(db_path):
        return {"total_settled": 0, "linked": 0, "estimated": 0, "coverage_pct": 0.0}

    conn = _connect(db_path)
    try:
        total_settled = _safe_int(conn.execute("SELECT COUNT(*) FROM settle_events").fetchone()[0], 0)
        linked = _safe_int(
            conn.execute("SELECT COUNT(*) FROM settle_events WHERE decision_id <> ''").fetchone()[0], 0
        )
        estimated = _safe_int(
            conn.execute("SELECT COUNT(*) FROM settle_events WHERE link_status = 'estimated'").fetchone()[0], 0
        )
        coverage = round((linked / total_settled * 100.0), 2) if total_settled > 0 else 0.0
        return {
            "total_settled": total_settled,
            "linked": linked,
            "estimated": estimated,
            "coverage_pct": coverage,
        }
    finally:
        conn.close()


def upsert_policy_version(
    user_ctx: Any,
    policy_id: str,
    status: str,
    shadow_metrics: Optional[Dict[str, Any]] = None,
    prod_metrics: Optional[Dict[str, Any]] = None,
    rollback_from: str = "",
) -> None:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    ensure_schema(user_dir)
    conn = _connect(get_db_path(user_dir))
    try:
        conn.execute(
            """
            INSERT INTO policy_versions(policy_id, created_at, status, shadow_metrics, prod_metrics, rollback_from)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(policy_id) DO UPDATE SET
                status=excluded.status,
                shadow_metrics=excluded.shadow_metrics,
                prod_metrics=excluded.prod_metrics,
                rollback_from=excluded.rollback_from
            """,
            (
                str(policy_id),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                str(status),
                json.dumps(shadow_metrics or {}, ensure_ascii=False),
                json.dumps(prod_metrics or {}, ensure_ascii=False),
                str(rollback_from or ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def start_task_run(
    user_ctx: Any,
    task_run_id: str,
    task_name: str,
    trigger_type: str,
    policy_id: str,
) -> None:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    ensure_schema(user_dir)
    conn = _connect(get_db_path(user_dir))
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO task_runs(task_run_id, task_name, trigger_type, policy_id, start_at, end_at, status, pnl, max_dd)
            VALUES(?, ?, ?, ?, ?, '', 'running', 0, 0)
            """,
            (
                str(task_run_id),
                str(task_name),
                str(trigger_type),
                str(policy_id or ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def finish_task_run(
    user_ctx: Any,
    task_run_id: str,
    status: str,
    pnl: int,
    max_dd: int,
) -> None:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    db_path = get_db_path(user_dir)
    if not os.path.exists(db_path):
        return
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE task_runs
            SET end_at = ?, status = ?, pnl = ?, max_dd = ?
            WHERE task_run_id = ?
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                str(status),
                _safe_int(pnl, 0),
                _safe_int(max_dd, 0),
                str(task_run_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def append_task_action(
    user_ctx: Any,
    task_run_id: str,
    step_no: int,
    regime: str,
    preset: str,
    planned_rounds: int,
    executed_rounds: int,
    action_type: str,
    reason: str,
    pnl: int,
) -> None:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    ensure_schema(user_dir)
    conn = _connect(get_db_path(user_dir))
    try:
        conn.execute(
            """
            INSERT INTO task_actions(task_run_id, step_no, ts, regime, preset, planned_rounds, executed_rounds, action_type, reason, pnl)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(task_run_id),
                _safe_int(step_no, 0),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                str(regime or ""),
                str(preset or ""),
                _safe_int(planned_rounds, 0),
                _safe_int(executed_rounds, 0),
                str(action_type or ""),
                str(reason or ""),
                _safe_int(pnl, 0),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def refresh_regime_preset_stats(user_ctx: Any) -> Dict[str, Any]:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    db_path = get_db_path(user_dir)
    if not os.path.exists(db_path):
        ensure_schema(user_dir)
    conn = _connect(db_path)
    report = {"updated": 0}
    try:
        rows = conn.execute(
            """
            SELECT regime, preset, profit
            FROM settle_events
            WHERE regime <> '' AND preset <> ''
            ORDER BY ts DESC
            LIMIT 3000
            """
        ).fetchall()

        grouped: Dict[Tuple[str, str], List[int]] = {}
        for row in rows:
            key = (str(row["regime"]), str(row["preset"]))
            grouped.setdefault(key, []).append(_safe_int(row["profit"], 0))

        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for (regime, preset), profits in grouped.items():
            if not profits:
                continue
            sample = len(profits)
            wins = sum(1 for p in profits if p > 0)
            hit_rate = wins / sample
            avg_profit = sum(profits) / sample
            cum = 0
            peak = 0
            max_dd = 0
            for p in reversed(profits):
                cum += p
                peak = max(peak, cum)
                max_dd = max(max_dd, peak - cum)

            conn.execute(
                """
                INSERT INTO regime_preset_stats(regime, preset, window, sample_size, hit_rate, avg_profit, max_dd, updated_at)
                VALUES(?, ?, 'rolling3000', ?, ?, ?, ?, ?)
                ON CONFLICT(regime, preset, window) DO UPDATE SET
                    sample_size=excluded.sample_size,
                    hit_rate=excluded.hit_rate,
                    avg_profit=excluded.avg_profit,
                    max_dd=excluded.max_dd,
                    updated_at=excluded.updated_at
                """,
                (
                    regime,
                    preset,
                    sample,
                    hit_rate,
                    avg_profit,
                    _safe_int(max_dd, 0),
                    now_text,
                ),
            )
            report["updated"] += 1

        conn.commit()
        return report
    finally:
        conn.close()


def fetch_regime_preset_rows(user_ctx: Any, regime: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    db_path = get_db_path(str(getattr(user_ctx, "user_dir", "") or ""))
    if not os.path.exists(db_path):
        return []
    conn = _connect(db_path)
    try:
        if regime:
            rows = conn.execute(
                """
                SELECT regime, preset, window, sample_size, hit_rate, avg_profit, max_dd, updated_at
                FROM regime_preset_stats
                WHERE regime = ?
                ORDER BY sample_size DESC
                LIMIT ?
                """,
                (str(regime), _safe_int(limit, 50)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT regime, preset, window, sample_size, hit_rate, avg_profit, max_dd, updated_at
                FROM regime_preset_stats
                ORDER BY sample_size DESC
                LIMIT ?
                """,
                (_safe_int(limit, 50),),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
