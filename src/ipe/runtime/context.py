"""Shared typing contract for runtime mixins."""

from __future__ import annotations

from typing import Any, Protocol


class RuntimeContext(Protocol):
    rc: Any
    args: Any
    state: Any
    lifecycle: Any
    inbound: Any
    outbound: Any
    routes: Any
    protocol: str
    worker_client: Any
    worker_ops: Any
    prov_client: Any
    prov_ops: Any
    provisioner: Any
    catchup: Any
    path_map: dict[tuple[str, str, str], str]
    status_paths: dict[str, str]
    specs_by_key: dict[tuple[str, str, str], Any]
    pipeline: Any
    adapter: Any
    guard: Any
    aei: str | None
    cmd_mgr: Any
    svc_tx: Any
    act_tx: Any
    _admission_lock: Any
    _prov_jobs: Any
    _stop_worker: Any
    _spool_pending: Any
    _muted_pipeline: set[tuple[str, str]]
    _confirm_pending: dict[str, tuple[str, str, str]]
    _approval_prompter: Any
    _approval_requests_emitted: set[str]
    _budget: Any
    _budget_dropped: int
    _inflight: dict[tuple[str, str], set[str]]
    _avail: dict[tuple[str, str, str], dict[str, Any]]
    _plan_misses: dict[tuple[str, str, str], int]
    _resource_removal_pending: dict[tuple[str, str, str], Any]
    _qos_fcnt_cache: dict[tuple[str, str, str], dict[str, Any]]
    _qos_fcnt_last_pub: dict[tuple[str, str, str], float]
    _qos_fcnt_revision: dict[tuple[str, str, str], int]
    _qos_resource_cache: dict[tuple[str, str, str], dict[str, Any]]
    _qos_republish: Any
    _anomaly_last: dict[tuple[str, str], float]

    def emit_event(self, category: str, severity: str, payload: dict[str, Any]) -> None: ...

    def _put_terminal(self, op: Any) -> None: ...

    def _spool_op(self, op: Any) -> None: ...

    def _publish_contract_for(self, key: tuple[str, str, str], spec: Any) -> None: ...

    def _publish_qos_state(self, only_key: tuple[str, str] | None = None) -> None: ...

    def _churn_track(self, snap: dict[str, Any]) -> None: ...

    def _terminate_inflight(self, robot: str, iface: str) -> None: ...

    def _finish(self, robot: str, iface: str, corr: str, terminal: str, ts: float) -> bool: ...

    def _finish_route_staging(self) -> None: ...

    def _defer_unloadable_types(self, rc: Any) -> None: ...

    def _absorb_provision(self, result: Any) -> None: ...
