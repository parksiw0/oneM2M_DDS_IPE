from __future__ import annotations

import time
from typing import Any

from ipe.ir import TopicIR


def parse_message(msg: Any) -> dict[str, Any]:
    from rosidl_runtime_py import message_to_ordereddict
    return dict(message_to_ordereddict(msg))


def make_topic_ir(
    topic_name: str,
    message_type: str,
    payload: dict[str, Any],
    timestamp: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> TopicIR:
    return TopicIR(
        interface_type="topic",
        interface_name=topic_name,
        message_type=message_type,
        timestamp=timestamp if timestamp is not None else time.time(),
        payload=payload,
        metadata=metadata or {},
    )


def extract_timestamp(
    payload: dict[str, Any],
    field: str | None,
    fmt: str | None,
) -> float | None:
    if field is None:
        return None

    value = _get_nested(payload, field)
    if value is None:
        return None

    if fmt == "px4_microseconds":
        return float(value) / 1_000_000
    if fmt == "epoch_seconds":
        return float(value)
    if fmt == "ros_time":
        if isinstance(value, dict):
            return value.get("sec", 0) + value.get("nanosec", 0) / 1e9
        return float(value)
    return None


def _get_nested(d: dict[str, Any], path: str) -> Any:
    parts = path.split(".")
    current: Any = d
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
        if current is None:
            return None
    return current
