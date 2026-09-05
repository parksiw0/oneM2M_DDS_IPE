"""Operation encoding and single-attempt delivery shared by live and spool workers."""

from __future__ import annotations

import json
import math
from dataclasses import asdict
from typing import Any

from ipe.core.policy import Op
from ipe.onem2m.client import OneM2MResponse, OversizeError, TransportError

_UPDATE_KINDS = frozenset({"update_fcnt", "update_cnt"})


def send_operation(op: Op, ops: Any) -> OneM2MResponse | BaseException | None:
    """Return None on success, otherwise the response/error to classify."""
    try:
        if op.kind in _UPDATE_KINDS:
            response = getattr(ops, op.kind)(op.path, op.content)
        elif op.kind == "update_lbl":
            response = ops.update_lbl(op.path, op.content["labels"])
        elif op.kind == "create_cin":
            result = ops.create_cin(op.path, op.content, rn=op.rn, et=op.et)
            return None if result.created or result.duplicate else result.response
        else:
            return ValueError(f"unknown operation kind: {op.kind!r}")
        return None if response.ok else response
    except (TransportError, OversizeError, ValueError, TypeError, KeyError) as exc:
        return exc


def encode_operation(op: Op) -> str:
    """Persist the operation and its interface identity for serialized replay."""
    return json.dumps(asdict(op), ensure_ascii=False)


def decode_operation(payload: str, queue_class: str, legacy_key: str | None) -> Op:
    """Read current rows and older rows whose identity was stored only in key."""
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("spool payload must be an object")
    if not isinstance(data.get("kind", "create_cin"), str):
        raise ValueError("spool operation kind must be a string")
    if not isinstance(data.get("path"), str) or not isinstance(data.get("content"), dict):
        raise ValueError("spool operation needs a path and content object")
    if data.get("kind") == "update_lbl" and not isinstance(data["content"].get("labels"), list):
        raise ValueError("update_lbl needs a labels list")
    identity = (legacy_key or "").split(":", 2)
    robot, interface, view = identity if len(identity) == 3 else ("-", data["path"], "-")
    expires_at = data.get("expires_at")
    if expires_at is not None:
        expires_at = float(expires_at)
        if not math.isfinite(expires_at):
            raise ValueError("expires_at must be finite")
    return Op(
        kind=data.get("kind", "create_cin"), path=data["path"], content=data["content"],
        robot_id=str(data.get("robot_id", robot)),
        interface=str(data.get("interface", interface)), view=str(data.get("view", view)),
        queue_class=queue_class, rn=data.get("rn"), et=data.get("et"),
        expires_at=expires_at, oversized=data.get("oversized", False),
        anomalous=data.get("anomalous", False),
    )
