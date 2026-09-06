"""LIVELINESS: DDS endpoint selection and oneM2M mapping status."""

from __future__ import annotations

from typing import Any

from ipe.qos._shared import choose_kind, guard_duration, offered_duration, requested_duration

NAME = "LIVELINESS"
FIELD = "liveliness"
STRENGTH = {"AUTOMATIC": 0, "MANUAL_BY_TOPIC": 1}
SCHEMA: dict[str, dict[str, Any]] = {
    FIELD: {"type": "string", "allowed": list(STRENGTH)},
    "liveliness_lease_duration_ms": {"type": "integer", "min": 0},
}
DEFAULT_MAPPING = {"kind": "BEHAVIOR", "handler": "livelinessMonitor"}


def observe(peers, configured, explicit):
    return choose_kind(peers, FIELD, configured, STRENGTH, weakest=True, explicit=explicit)


def command(peers, configured):
    return choose_kind(peers, FIELD, configured, STRENGTH, weakest=False)


def guard(peers, configured):
    weakest = observe(peers, configured, False)
    strict = configured in STRENGTH and STRENGTH[configured] > STRENGTH[weakest]
    return (weakest if strict else configured), strict


def value(spec):
    return {
        "kind": spec.liveliness,
        "leaseDurationMs": "INF"
        if spec.liveliness_lease_duration_ms is None
        else spec.liveliness_lease_duration_ms,
    }


def mapping():
    return {"handler": "ros2EndpointEvent", "result": "APPLIED_AT_ROS2_ENDPOINT"}


def observe_lease(peers, configured, explicit):
    return requested_duration(peers, "liveliness_lease_duration", configured, explicit)


def command_lease(peers, configured):
    return offered_duration(peers, "liveliness_lease_duration", configured)


def guard_lease(peers, configured):
    return guard_duration(peers, "liveliness_lease_duration", configured)
