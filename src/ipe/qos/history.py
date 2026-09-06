"""HISTORY: DDS cache shape and bounded oneM2M container retention."""

from __future__ import annotations

NAME = "HISTORY"
SCHEMA = {
    "history": {"type": "string", "allowed": ["KEEP_LAST", "KEEP_ALL"]},
    "depth": {"type": "integer", "min": 1},
}
DEFAULT_MAPPING = {"kind": "RESOURCE", "resourceType": "container", "attribute": "maxNrOfInstances"}


def value(spec):
    return {"kind": spec.history, "depth": spec.depth}


def container_attrs(spec, keep_all_limit):
    return {"mni": spec.depth if spec.history == "KEEP_LAST" else keep_all_limit}


def mapping(spec, direction, paths, keep_all_limit):
    if direction != "observe":
        return {
            "result": "PRESERVED",
            "reason": "DDS writer cache depth does not control command retention",
        }
    if paths.get("history"):
        attrs = {
            "resource": paths["history"],
            "attribute": "mni",
            "value": container_attrs(spec, keep_all_limit)["mni"],
            "result": "APPROXIMATED",
        }
        if spec.history == "KEEP_ALL":
            attrs["reason"] = "KEEP_ALL remains bounded by the configured CSE retention limit"
        return attrs
    return {
        "resource": paths.get("latest") or paths.get("fcnt"),
        "result": "CONSTRAINED_BY_REPRESENTATION",
        "reason": "the interface exposes only the latest value",
    }
