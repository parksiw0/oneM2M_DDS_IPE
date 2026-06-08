from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ipe.ir import TopicIR


def epoch_to_onem2m_ts(epoch: float) -> str:
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    millis = dt.microsecond // 1000
    return f"{dt.strftime('%Y%m%dT%H%M%S')},{millis:03d}"


def coerce_value(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        return list(v)
    if isinstance(v, (list, tuple)):
        return [coerce_value(x) for x in v]
    if isinstance(v, dict):
        return {k: coerce_value(val) for k, val in v.items()}
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return None
    return v


def select_fields(payload: dict[str, Any], fields: list[str] | None) -> dict[str, Any]:
    if not fields:
        return {k: coerce_value(v) for k, v in payload.items()}
    out: dict[str, Any] = {}
    for f in fields:
        if f in payload:
            out[f] = coerce_value(payload[f])
    return out


def normalize_ir(ir: TopicIR, selected: list[str] | None) -> dict[str, Any]:
    return {
        "topic": ir["interface_name"],
        "ts": ir["timestamp"],
        "ts_iso": epoch_to_onem2m_ts(ir["timestamp"]),
        "fields": select_fields(ir["payload"], selected),
    }
