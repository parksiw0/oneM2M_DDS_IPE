from __future__ import annotations

import json
from typing import Any

from ipe.core.normalize import coerce_value


def build_cin_content(normalized: dict[str, Any]) -> dict[str, Any]:
    body = {
        "ts": normalized["ts_iso"],
        "topic": normalized["topic"],
        "data": normalized["fields"],
    }
    return {"m2m:cin": {"con": json.dumps(body, ensure_ascii=False)}}


def build_fcnt_attrs(
    normalized: dict[str, Any],
    field_map: dict[str, str],
    frame: str | None,
) -> dict[str, Any]:
    fields = normalized["fields"]
    attrs: dict[str, Any] = {}
    for short, src in field_map.items():
        if src in fields:
            v = coerce_value(fields[src])
            if v is not None:
                attrs[short] = v
    if frame and "frm" not in attrs:
        attrs["frm"] = frame
    attrs["dgt"] = normalized["ts_iso"]
    attrs["src"] = normalized["topic"]
    return attrs


def build_fcnt_create(
    cnd: str,
    fcnt_type: str,
    resource_name: str,
    initial_attrs: dict[str, Any],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "cnd": cnd,
        "rn": resource_name,
    }
    body.update(initial_attrs)
    return {fcnt_type: body}


def build_fcnt_update(fcnt_type: str, attrs: dict[str, Any]) -> dict[str, Any]:
    return {fcnt_type: attrs}
