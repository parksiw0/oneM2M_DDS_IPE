"""DesiredBindingPlan 세대와 graph diff."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ipe.config.spec import ResolvedConfig

PlanKey = tuple[str, str, str]  # kind, robot_id, interface


def binding_map(rc: ResolvedConfig) -> dict[PlanKey, Any]:
    out: dict[PlanKey, Any] = {}
    out.update({
        ("topic", x.robot_id, x.interface): x
        for x in rc.topics
        if x.msg_type and (x.direction in ("observe", "both") or x.access_enabled)
    })
    out.update({
        ("service", x.robot_id, x.interface): x
        for x in rc.services
        if x.srv_type and x.access_enabled
    })
    out.update({
        ("action", x.robot_id, x.interface): x
        for x in rc.actions
        if x.action_type and x.access_enabled
    })
    return out


def append_spec(rc: ResolvedConfig, key: PlanKey, spec: Any) -> None:
    kind = key[0]
    target = cast(
        list[Any],
        rc.topics if kind == "topic" else rc.services if kind == "service" else rc.actions,
    )
    if not any(x.robot_id == spec.robot_id and x.interface == spec.interface for x in target):
        target.append(spec)


def set_spec(rc: ResolvedConfig, key: PlanKey, spec: Any) -> None:
    kind, robot_id, interface = key
    target = cast(
        list[Any],
        rc.topics if kind == "topic" else rc.services if kind == "service" else rc.actions,
    )
    target[:] = [x for x in target
                 if not (x.robot_id == robot_id and x.interface == interface)]
    target.append(spec)


@dataclass
class PendingBindingPlan:
    rc: ResolvedConfig
    provision: Any
    additions: list[tuple[PlanKey, Any]]
    removals: list[tuple[PlanKey, Any]]
    base_generation: int | None = None


__all__ = ["PendingBindingPlan", "PlanKey", "append_spec", "binding_map", "set_spec"]
