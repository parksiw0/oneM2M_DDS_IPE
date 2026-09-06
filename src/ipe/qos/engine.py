"""Compose per-policy DDS decisions and oneM2M mapping plans; no I/O."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from ipe.qos import deadline as ddl
from ipe.qos import durability as drb
from ipe.qos import history, lifespan
from ipe.qos import liveliness as liv
from ipe.qos import reliability as rlb
from ipe.qos._shared import _dedupe
from ipe.qos.models import QoSSpec


def reconcile_observe(
    offered: list[Any],
    configured: QoSSpec,
    has_explicit: bool,
    explicit_fields: frozenset[str] | set[str] | None = None,
) -> tuple[QoSSpec, list[str]]:
    """Derive a weak-compatible request while preserving explicit duration fields."""
    if not offered:
        return configured, ["noPublisherFallback"]

    events: list[str] = []

    fields = explicit_fields

    def explicit(field: str) -> bool:
        return has_explicit if fields is None else field in fields

    reliability = rlb.observe(offered, configured.reliability, explicit("reliability"))
    durability = drb.observe(offered, configured.durability, explicit("durability"))
    liveliness = liv.observe(offered, configured.liveliness, explicit("liveliness"))

    if configured.durability == "TRANSIENT_LOCAL" and durability == "VOLATILE":
        # 래치 손실은 RxO 호환이라 DDS 수준에서는 조용히 지나간다 —
        # 이 이벤트가 유일한 가시성이다.
        events.append("latchedDowngraded")

    fields = explicit_fields or frozenset()
    deadline_ms = ddl.observe(offered, configured.deadline_ms, "deadline_ms" in fields)
    lease_ms = liv.observe_lease(
        offered, configured.liveliness_lease_duration_ms, "liveliness_lease_duration_ms" in fields
    )

    spec = replace(
        configured,
        reliability=reliability,
        durability=durability,
        liveliness=liveliness,
        deadline_ms=deadline_ms,
        liveliness_lease_duration_ms=lease_ms,
    )
    return spec, _dedupe(events)


def reconcile_command(requested, configured):
    if not requested:
        return configured, ["noSubscriberFallback"]
    spec, events = configured, []
    for policy in (rlb, drb, liv):
        current = getattr(spec, policy.FIELD)
        selected = policy.command(requested, current)
        if current != selected:
            spec = replace(spec, **{policy.FIELD: selected})
            events.append("qosUpgraded")
    if spec.durability == "TRANSIENT_LOCAL" and spec.lifespan_ms is None:
        events.append("transientLocalLifespanRequired")
    deadline_ms = ddl.command(requested, spec.deadline_ms)
    if deadline_ms != spec.deadline_ms:
        spec = replace(spec, deadline_ms=deadline_ms)
        events.append("qosUpgraded")
    lease_ms = liv.command_lease(requested, spec.liveliness_lease_duration_ms)
    if lease_ms != spec.liveliness_lease_duration_ms:
        spec = replace(spec, liveliness_lease_duration_ms=lease_ms)
        events.append("qosUpgraded")
    return spec, _dedupe(events)


def strictness_guard(spec, offered, mode):
    if mode not in ("reject", "demote"):
        raise ValueError(f"strictness mode must be 'reject' or 'demote', got {mode!r}")
    if not offered:
        return spec, []
    demoted, events = spec, []
    for policy in (rlb, drb, liv):
        selected, strict = policy.guard(offered, getattr(spec, policy.FIELD))
        if strict:
            events.append("strict" + policy.FIELD.title())
            demoted = replace(demoted, **{policy.FIELD: selected})
    value, strict = ddl.guard(offered, spec.deadline_ms)
    if strict:
        events.append("strictDeadline")
        demoted = replace(demoted, deadline_ms=value)
    value, strict = liv.guard_lease(offered, spec.liveliness_lease_duration_ms)
    if strict:
        events.append("strictLease")
        demoted = replace(demoted, liveliness_lease_duration_ms=value)
    return (demoted if mode == "demote" else spec), events


def topic_mapping(
    spec: QoSSpec,
    direction: str,
    paths: dict[str, str],
    keep_all_limit: int,
) -> dict[str, dict[str, Any]]:
    return {
        "RELIABILITY": rlb.mapping(),
        "DURABILITY": drb.mapping(),
        "DEADLINE": ddl.mapping(),
        "LIVELINESS": liv.mapping(),
        "HISTORY": history.mapping(spec, direction, paths, keep_all_limit),
        "LIFESPAN": lifespan.mapping(spec, direction, paths),
    }
