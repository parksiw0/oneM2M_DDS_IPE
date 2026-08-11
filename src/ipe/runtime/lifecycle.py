"""IPE 기동/운영 상태의 닫힌 어휘와 영속 스냅숏."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class IPEState(str, Enum):
    NOT_READY = "NOT_READY"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"


class IPEPhase(str, Enum):
    IDLE = "IDLE"
    VALIDATING_CONFIG = "VALIDATING_CONFIG"
    TRANSPORT_READY = "TRANSPORT_READY"
    DDS_JOINED = "DDS_JOINED"
    GRAPH_DISCOVERED = "GRAPH_DISCOVERED"
    BINDING_PLAN_RESOLVED = "BINDING_PLAN_RESOLVED"
    AE_REGISTERED = "AE_REGISTERED"
    CSE_RESOURCES_PREPARED = "CSE_RESOURCES_PREPARED"
    CSE_PROVISIONED = "CSE_PROVISIONED"
    BINDING = "BINDING"
    BINDING_READY = "BINDING_READY"


class IPEHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class LifecycleSnapshot:
    ipe_state: str
    ipe_phase: str
    ipe_health: str
    generation: int
    updated_at: float
    detail: str = ""


class Lifecycle:
    def __init__(self, persistence: Any) -> None:
        self.persistence = persistence
        self._generation = 0
        self._snapshot = LifecycleSnapshot(
            IPEState.NOT_READY.value, IPEPhase.IDLE.value,
            IPEHealth.HEALTHY.value, 0, time.time())
        self._save()

    @property
    def snapshot(self) -> LifecycleSnapshot:
        return self._snapshot

    def set(
        self,
        state: IPEState,
        phase: IPEPhase,
        *,
        health: IPEHealth | None = None,
        detail: str = "",
        next_generation: bool = False,
    ) -> LifecycleSnapshot:
        if next_generation:
            self._generation += 1
        self._snapshot = LifecycleSnapshot(
            state.value,
            phase.value,
            (health or IPEHealth(self._snapshot.ipe_health)).value,
            self._generation,
            time.time(),
            detail,
        )
        self._save()
        return self._snapshot

    def _save(self) -> None:
        if self.persistence is not None:
            self.persistence.set_kv("ipe_lifecycle", asdict(self._snapshot))


__all__ = ["IPEHealth", "IPEPhase", "IPEState", "Lifecycle", "LifecycleSnapshot"]
