import json
import os
from datetime import datetime
from typing import Any, Dict, List


_STREAM_FILES = {
    "all": "interaction_journal.jsonl",
    "commands": "telegram_commands.jsonl",
    "inbound": "telegram_inbound.jsonl",
    "outbound": "telegram_outbound.jsonl",
}


def _analytics_dir(user_ctx) -> str:
    user_dir = str(getattr(user_ctx, "user_dir", "") or "")
    analytics_dir = os.path.join(user_dir, "analytics")
    os.makedirs(analytics_dir, exist_ok=True)
    return analytics_dir


def _journal_path(user_ctx, stream: str = "all") -> str:
    analytics_dir = _analytics_dir(user_ctx)
    filename = _STREAM_FILES.get(stream, _STREAM_FILES["all"])
    return os.path.join(analytics_dir, filename)


def _base_record(user_ctx, kind: str) -> Dict[str, Any]:
    return {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "kind": kind,
        "user_id": getattr(user_ctx, "user_id", None),
        "account_name": str(getattr(getattr(user_ctx, "config", None), "name", "") or "").strip(),
    }


def _write_record(user_ctx, record: Dict[str, Any], streams: List[str]) -> None:
    line = json.dumps(record, ensure_ascii=False) + "\n"
    for stream in streams:
        path = _journal_path(user_ctx, stream)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line)


def record_command(
    user_ctx,
    *,
    source: str,
    command: str,
    accepted: bool,
    reason: str = "",
    sender_id: Any = None,
    chat_id: Any = None,
) -> None:
    record = _base_record(user_ctx, "command")
    record.update(
        {
            "source": source,
            "command": str(command or "").strip(),
            "accepted": bool(accepted),
            "reason": str(reason or "").strip(),
            "sender_id": sender_id,
            "chat_id": chat_id,
        }
    )
    _write_record(user_ctx, record, ["all", "commands"])


def record_inbound(
    user_ctx,
    *,
    source: str,
    message: str,
    sender_id: Any = None,
    chat_id: Any = None,
    msg_id: Any = None,
) -> None:
    text = str(message or "")
    record = _base_record(user_ctx, "inbound")
    record.update(
        {
            "source": str(source or "").strip(),
            "sender_id": sender_id,
            "chat_id": chat_id,
            "msg_id": msg_id,
            "message_preview": text[:240],
            "message": text,
        }
    )
    _write_record(user_ctx, record, ["all", "inbound"])


def record_message(
    user_ctx,
    *,
    channel: str,
    target: Any,
    message: str,
    ok: bool,
    error: str = "",
    parse_mode: str = "",
    msg_type: str = "",
    message_kind: str = "",
) -> None:
    record = _base_record(user_ctx, "message")
    text = str(message or "")
    record.update(
        {
            "direction": "outbound",
            "channel": str(channel or "").strip(),
            "target": target,
            "ok": bool(ok),
            "error": str(error or "").strip(),
            "parse_mode": str(parse_mode or "").strip(),
            "msg_type": str(msg_type or "").strip(),
            "message_kind": str(message_kind or "").strip(),
            "message_preview": text[:240],
            "message": text,
        }
    )
    _write_record(user_ctx, record, ["all", "outbound"])


def read_recent_events(user_ctx, limit: int = 20, stream: str = "all") -> List[Dict[str, Any]]:
    path = _journal_path(user_ctx, stream)
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as fh:
        lines = [line.strip() for line in fh if line.strip()]

    records: List[Dict[str, Any]] = []
    for line in lines[-max(0, int(limit or 0)):]:
        try:
            records.append(json.loads(line))
        except Exception:
            records.append({"kind": "invalid", "raw": line})
    return records
