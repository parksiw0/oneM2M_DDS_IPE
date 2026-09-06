"""DURABILITY: DDS endpoint selection and oneM2M mapping status."""

from __future__ import annotations

from ipe.qos._shared import choose_kind

NAME = "DURABILITY"
FIELD = "durability"
STRENGTH = {"VOLATILE": 0, "TRANSIENT_LOCAL": 1}
SCHEMA = {FIELD: {"type": "string", "allowed": list(STRENGTH)}}
DEFAULT_MAPPING = {"kind": "BEHAVIOR", "handler": "retentionReplay"}


def observe(peers, configured, explicit):
    return choose_kind(peers, FIELD, configured, STRENGTH, weakest=True, explicit=explicit)


def command(peers, configured):
    return choose_kind(peers, FIELD, configured, STRENGTH, weakest=False)


def guard(peers, configured):
    weakest = observe(peers, configured, False)
    strict = configured in STRENGTH and STRENGTH[configured] > STRENGTH[weakest]
    return (weakest if strict else configured), strict


def value(spec):
    return spec.durability


def mapping():
    return {"handler": "ros2EndpointAndCseRetention", "result": "APPROXIMATED"}
