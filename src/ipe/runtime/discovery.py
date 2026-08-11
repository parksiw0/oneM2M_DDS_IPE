"""ROS graph 수렴과 DesiredBindingPlan 입력 snapshot 관리."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


class GraphNotReady(RuntimeError):
    pass


@dataclass(frozen=True)
class DiscoveredState:
    snapshot: dict[str, Any]
    samples: int
    stable_polls: int
    elapsed_sec: float


def graph_fingerprint(snapshot: dict[str, Any]) -> tuple[Any, ...]:
    """순서와 endpoint 객체 표현에 영향받지 않는 graph 비교 키."""
    kinds = []
    for kind in ("topics", "services", "actions"):
        entries = tuple(sorted((name, tuple(sorted(types)))
                               for name, types in snapshot.get(kind, [])))
        kinds.append((kind, entries))
    directions = tuple(sorted(snapshot.get("topic_directions", {}).items()))
    owners = []
    owner_root = snapshot.get("owners", {})
    for kind in ("topics", "services", "actions"):
        entries = tuple(sorted(
            (name, tuple(sorted(namespaces)))
            for name, namespaces in owner_root.get(kind, {}).items()
        ))
        owners.append((kind, entries))
    return (*kinds, ("topic_directions", directions), ("owners", tuple(owners)))


def application_interface_count(snapshot: dict[str, Any]) -> int:
    return sum(len(snapshot.get(kind, [])) for kind in ("topics", "services", "actions"))


def await_graph_convergence(
    snapshot_fn: Callable[[], dict[str, Any]],
    *,
    timeout_sec: float = 10.0,
    stable_polls: int = 2,
    poll_sec: float = 0.5,
    spin_once: Callable[[float], None] | None = None,
) -> DiscoveredState:
    """동일한 비어 있지 않은 graph가 연속 관측될 때까지 기다린다."""
    if stable_polls < 1:
        raise ValueError("stable_polls must be at least 1")
    started = time.monotonic()
    deadline = started + timeout_sec
    previous: tuple[Any, ...] | None = None
    stable = 0
    samples = 0
    last: dict[str, Any] = {}

    while True:
        if spin_once is not None:
            spin_once(min(poll_sec, max(0.0, deadline - time.monotonic())))
        last = snapshot_fn()
        samples += 1
        fingerprint = graph_fingerprint(last)
        if application_interface_count(last) > 0 and fingerprint == previous:
            stable += 1
        elif application_interface_count(last) > 0:
            stable = 1
        else:
            stable = 0
        previous = fingerprint
        if stable >= stable_polls:
            return DiscoveredState(last, samples, stable,
                                   time.monotonic() - started)
        if time.monotonic() >= deadline:
            break
        if spin_once is None:
            time.sleep(min(poll_sec, max(0.0, deadline - time.monotonic())))

    raise GraphNotReady(
        f"ROS graph did not converge within {timeout_sec:g}s "
        f"({application_interface_count(last)} interfaces in last snapshot)"
    )


__all__ = [
    "DiscoveredState",
    "GraphNotReady",
    "application_interface_count",
    "await_graph_convergence",
    "graph_fingerprint",
]
