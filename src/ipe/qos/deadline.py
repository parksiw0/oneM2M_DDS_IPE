"""DEADLINE: requested/offered intervals and endpoint event reporting."""

from __future__ import annotations

from ipe.qos._shared import guard_duration, offered_duration, requested_duration

NAME = "DEADLINE"
SCHEMA = {"deadline_ms": {"type": "integer", "min": 0}}
DEFAULT_MAPPING = {"kind": "BEHAVIOR", "handler": "deadlineMonitor"}


def observe(peers, configured, explicit):
    return requested_duration(peers, "deadline", configured, explicit)


def command(peers, configured):
    return offered_duration(peers, "deadline", configured)


def guard(peers, configured):
    return guard_duration(peers, "deadline", configured)


def value(spec):
    return {"durationMs": "INF" if spec.deadline_ms is None else spec.deadline_ms}


def mapping():
    return {"handler": "ros2EndpointEvent", "result": "APPLIED_AT_ROS2_ENDPOINT"}
