"""LIFESPAN: ingest-time sample expiration and representation limitations."""

from __future__ import annotations

NAME = "LIFESPAN"
SCHEMA = {"lifespan_ms": {"type": "integer", "min": 0}}
DEFAULT_MAPPING = {
    "kind": "RESOURCE",
    "resourceType": "contentInstance",
    "attribute": "expirationTime",
}


def value(spec):
    return {"durationMs": "INF" if spec.lifespan_ms is None else spec.lifespan_ms}


def expires_at(spec, ingest_ts):
    return None if spec.lifespan_ms is None else ingest_ts + spec.lifespan_ms / 1000.0


def mapping(spec, direction, paths):
    if direction != "observe":
        return {
            "result": "PRESERVED",
            "reason": "command freshness is enforced by the command safety gate",
        }
    if spec.lifespan_ms is None:
        return {"result": "NOT_CONFIGURED"}
    resources = [paths[view] for view in ("history", "latest") if paths.get(view)]
    if resources:
        result = {
            "resources": resources,
            "attribute": "contentInstance.et",
            "result": "APPROXIMATED",
        }
        if paths.get("fcnt"):
            result.update(
                result="PARTIALLY_APPLIED",
                unsupportedResources=[paths["fcnt"]],
                reason="the mutable data flexContainer has no per-sample expirationTime",
            )
        return result
    return {
        "resource": paths.get("fcnt"),
        "result": "UNSUPPORTED",
        "reason": "a mutable data flexContainer has no per-sample expirationTime",
    }
