"""Decode QoS change requests independently of runtime routing and I/O."""

from __future__ import annotations

from typing import Any

_QOS_CF_ENUMS = {
    "cfRlb": ("reliability", ("RELIABLE", "BEST_EFFORT")),
    "cfDrb": ("durability", ("VOLATILE", "TRANSIENT_LOCAL")),
    "cfHst": ("history", ("KEEP_LAST", "KEEP_ALL")),
    "cfLiv": ("liveliness", ("AUTOMATIC", "MANUAL_BY_TOPIC")),
}


_QOS_CF_DURS = {
    "cfDdl": "deadline_ms",
    "cfLsp": "lifespan_ms",
    "cfLse": "liveliness_lease_duration_ms",
}


def parse_cf_update(payload: dict[str, Any], base: Any) -> tuple[Any | None, str]:
    """NOTIFY rep의 cf* → 후보 QoSSpec. (None, 사유) = 도메인 위반."""
    from dataclasses import replace

    updates: dict[str, Any] = {}
    for sn, (fld, allowed) in _QOS_CF_ENUMS.items():
        if sn not in payload:
            continue
        v = str(payload[sn]).upper()
        if v not in allowed:
            return None, f"{sn}: '{payload[sn]}' not in {allowed}"
        updates[fld] = v
    if "cfDpt" in payload:
        d = payload["cfDpt"]
        if not isinstance(d, int) or isinstance(d, bool) or d < 1:
            return None, f"cfDpt: expected integer >= 1, got {d!r}"
        updates["depth"] = d
    for sn, fld in _QOS_CF_DURS.items():
        if sn not in payload:
            continue
        v = payload[sn]
        if isinstance(v, str) and v.upper() == "INF":
            updates[fld] = None
        elif isinstance(v, int) and not isinstance(v, bool) and v >= 0:
            updates[fld] = v
        elif isinstance(v, str) and v.isdigit():
            updates[fld] = int(v)
        else:
            return None, f"{sn}: expected 'INF' or decimal ms, got {v!r}"
    cand = replace(base, **updates)
    if cand.liveliness == "MANUAL_BY_TOPIC" and cand.liveliness_lease_duration_ms is None:
        return None, "liveliness MANUAL_BY_TOPIC requires cfLse (B8)"
    return cand, ""


def policy_changes_to_cf(changes: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    from ipe.qos.registry import DDS_QOS_POLICY_NAMES

    aliases = {
        "RELIABILITY": "cfRlb",
        "DURABILITY": "cfDrb",
        "DEADLINE": "cfDdl",
        "LIFESPAN": "cfLsp",
    }
    translated: dict[str, Any] = {}
    for raw_name, value in changes.items():
        name = str(raw_name).upper()
        if name not in DDS_QOS_POLICY_NAMES:
            return None, f"unknown DDS QoS policy: {raw_name}"
        if name in aliases:
            if isinstance(value, dict):
                value = value.get("durationMs", value.get("value"))
            translated[aliases[name]] = value
            continue
        if name == "HISTORY":
            if isinstance(value, str):
                translated["cfHst"] = value
            elif isinstance(value, dict):
                translated["cfHst"] = value.get("kind")
                if "depth" in value:
                    translated["cfDpt"] = value["depth"]
            else:
                return None, "HISTORY must be a kind or an object"
            continue
        if name == "LIVELINESS":
            if isinstance(value, str):
                translated["cfLiv"] = value
            elif isinstance(value, dict):
                translated["cfLiv"] = value.get("kind")
                if "leaseDurationMs" in value:
                    translated["cfLse"] = value["leaseDurationMs"]
            else:
                return None, "LIVELINESS must be a kind or an object"
            continue
        return None, f"{name} is not changeable through the active ROS 2 RMW adapter"
    if not translated:
        return None, "changes must contain at least one supported policy"
    if "cfHst" in translated and translated["cfHst"] is None:
        return None, "HISTORY.kind is required"
    if "cfLiv" in translated and translated["cfLiv"] is None:
        return None, "LIVELINESS.kind is required"
    return translated, ""


def fields_in_update(payload):
    fields = {field for short, (field, _) in _QOS_CF_ENUMS.items() if short in payload}
    fields.update(field for short, field in _QOS_CF_DURS.items() if short in payload)
    if "cfDpt" in payload:
        fields.add("depth")
    return frozenset(fields)
