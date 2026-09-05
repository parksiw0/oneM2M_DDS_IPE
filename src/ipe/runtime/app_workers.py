"""IPEApp 분해 mixin — 상태는 전부 IPEApp.__init__이 소유한다.

각 mixin은 self의 구성요소(state/queues/adapter/path_map/...)를 공유하는
같은 객체의 단면이다. 단독 인스턴스화 금지.
"""

from __future__ import annotations

import logging
import queue
import time
from typing import Any

from ipe.core.policy import Op
from ipe.core.vocab import CLASS_TERMINAL
from ipe.onem2m.client import backoff_delays, classify
from ipe.runtime.context import RuntimeContext
from ipe.runtime.delivery import decode_operation, encode_operation, send_operation
from ipe.runtime.dispatcher import InboundEvent
from ipe.runtime.plan import PendingBindingPlan, append_spec, binding_map, set_spec

log = logging.getLogger(__name__)

GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}


class WorkersMixin(RuntimeContext):
    def _provision_staged(self) -> Any:
        """Provision CSE routes while deferring notifications until activation."""
        staging = getattr(self, "_routes_staging", None)
        if staging is None:
            return self.provisioner.provision_all()
        staging.set()
        try:
            return self.provisioner.provision_all()
        except Exception:
            staging.clear()
            raise

    def _finish_route_staging(self) -> None:
        staging = getattr(self, "_routes_staging", None)
        if staging is not None:
            staging.clear()

    def _spool_op(self, op: Op) -> None:
        self.state.spool_put(op.queue_class, f"{op.robot_id}:{op.interface}:{op.view}",
                             encode_operation(op), time.time())
        self._spool_pending.set()

    def _mark_cse_unavailable(self) -> None:
        with self._transport_state_lock:
            self._cse_transport_down = True

    def _mark_cse_available(self) -> bool:
        with self._transport_state_lock:
            if not self._cse_transport_down:
                return False
            self._cse_transport_down = False
        self._prov_jobs.put(("recover", None))
        return True

    def _outbound_worker(self, worker_index: int) -> None:
        rec = self.rc.recovery
        retries = int(rec.get("retry_count", 3))
        base_ms = int(rec.get("retry_delay_ms", 500))
        ops = self.worker_ops_pool[worker_index]
        next_spool_attempt = 0.0
        while not self._stop_worker.is_set():
            try:
                now = time.monotonic()
                if (worker_index == 0 and self._spool_pending.is_set()
                        and now >= next_spool_attempt):
                    self._spool_pending.clear()
                    self._drain_spool(ops)
                    next_spool_attempt = time.monotonic() + max(0.5, base_ms / 1000.0)
                try:
                    op = self.outbound.claim(timeout=0.5)
                except queue.Empty:
                    continue
                try:
                    self._send_with_retry(op, retries, base_ms, ops)
                finally:
                    self.outbound.release(op)
            except Exception:
                log.exception("outbound worker iteration failed (isolated)")

    def _send_with_retry(
        self, op: Op, retries: int, base_ms: int, ops: Any
    ) -> None:
        if not self._deliver(op, retries, base_ms, ops) and op.queue_class == CLASS_TERMINAL:
            self._spool_op(op)

    def _deliver(self, op: Op, retries: int, base_ms: int, ops: Any) -> bool:
        """True means consumed (sent, expired or rejected); False means retain."""
        factor = 1.0 if self.rc.recovery.get("backoff") == "fixed" else 2.0
        delays = iter(backoff_delays(retries, base_ms, factor=factor))
        for attempt in range(retries + 1):
            if self._stop_worker.is_set():
                return False
            if op.expires_at is not None and time.time() >= op.expires_at:
                return True
            failure = send_operation(op, ops)
            if failure is None:
                self._mark_cse_available()
                return True
            cls = classify(failure)
            if cls == "non_recoverable":
                if not isinstance(failure, BaseException):
                    self._mark_cse_available()
                self._report_dropped_op(op, failure)
                return True
            if cls == "policy_dependent":
                if not self._mark_cse_available():
                    self._prov_jobs.put(("reconcile", None))
                return False
            if attempt == retries:
                self._mark_cse_unavailable()
                return False
            if self._stop_worker.wait(next(delays) / 1000.0):
                return False
        return False

    def _report_dropped_op(self, op: Op, failure: Any) -> None:
        log.error("non-recoverable op dropped: %s (%s)", op.path, failure)
        # A rejected health event must not recursively generate another one.
        if op.view != "ipeHealth":
            self.emit_event("ipeHealth", "error",
                            {"event": "opFailed", "path": op.path,
                             "interface": op.interface, "robot": op.robot_id,
                             "kind": op.kind})

    def _drain_spool(self, ops: Any) -> None:
        try:
            for row in self.state.spool_list(limit=20):
                if self._stop_worker.is_set():
                    return
                try:
                    op = decode_operation(row["payload"], row["class"], row["key"])
                except (ValueError, TypeError) as exc:
                    log.error("invalid spool row %s dropped: %s", row["id"], exc)
                    self.state.spool_delete([row["id"]])
                    continue
                if not self.outbound.try_claim(op):
                    return
                try:
                    if not self._deliver(op, 0, 0, ops):
                        return
                    self.state.spool_delete([row["id"]])
                finally:
                    self.outbound.release(op)
        finally:
            # Preserve the wakeup even if decoding, storage or a client fails.
            if self.state.spool_list(limit=1):
                self._spool_pending.set()

    # ------------------------------------------------------------------
    # 프로비저닝 워커
    # ------------------------------------------------------------------

    def _prov_worker(self) -> None:
        while not self._stop_worker.is_set():
            try:
                job, arg = self._prov_jobs.get(timeout=1.0)
            except queue.Empty:
                continue
            if self._stop_worker.is_set():
                break
            try:
                if job == "reconcile":
                    restarted = self.provisioner.check_cse_identity()
                    self._absorb_provision(self._provision_staged())
                    # 재프로비저닝 후 캐시 1회 무효화(§4.8.4) — 재생성된 qos
                    # FCNT가 CREATE 초기 속성만 든 채 캐시에 가려지는 것을 막는다.
                    # 게시는 executor 틱이 수행한다(스레드 소유권).
                    self._qos_republish.set()
                    if restarted:
                        self.catchup.sweep("cse-restart")
                elif job == "catchup":
                    self.catchup.sweep(str(arg or "manual"))
                elif job == "recover":
                    restarted = self.provisioner.check_cse_identity()
                    self._absorb_provision(self._provision_staged())
                    self._qos_republish.set()
                    self.catchup.sweep("cse-restart" if restarted else "cse-recovered")
                elif job == "reconcile_discovery":
                    self._reconcile_discovery(arg)
                elif job == "remove_interfaces":
                    self._remove_interfaces(arg)
            except Exception:
                self._finish_route_staging()
                log.exception("provisioning job %s failed", job)

    @staticmethod
    def _removal_path_count(key: tuple[str, str, str], spec: Any) -> int:
        if key[0] != "topic":
            return 1
        return int(spec.direction in ("observe", "both")) + int(
            spec.direction in ("command", "both")
        )

    def _remove_interfaces(self, items: list[tuple[Any, Any]]) -> None:
        """CSE subtree 제거 실패를 다음 Graph 주기까지 보존한다."""
        for key, spec in items:
            try:
                removed = self.provisioner.remove_interface(key[0], spec)
            except Exception as e:
                log.warning("resource removal deferred for %s: %s", key, e)
                self._resource_removal_pending[key] = spec
                continue
            if len(removed) < WorkersMixin._removal_path_count(key, spec):
                self._resource_removal_pending[key] = spec
            else:
                self._resource_removal_pending.pop(key, None)

    def _reconcile_discovery(self, snap: dict[str, Any]) -> None:
        from ipe.config.resolver import resolve
        if self._resource_removal_pending:
            self._remove_interfaces(list(self._resource_removal_pending.items()))
        try:
            new_rc = resolve(self.rc.raw, discovered=snap)
        except Exception as e:
            log.warning("discovery re-resolve failed: %s", e)
            return
        apply_approvals = getattr(self, "_apply_saved_control_approvals", None)
        if apply_approvals is not None:
            apply_approvals(new_rc)
        apply_qos = getattr(self, "_apply_saved_qos_overrides", None)
        if apply_qos is not None:
            apply_qos(new_rc)
        self._defer_unloadable_types(new_rc)
        self._churn_track(snap)

        active = binding_map(self.rc)
        desired = binding_map(new_rc)
        grace = int(self.rc.discovery.get("vanish_grace_polls", 2) or 2)
        removals: list[tuple[Any, Any]] = []

        for key, spec in active.items():
            if key in desired:
                self._plan_misses.pop(key, None)
                continue
            misses = self._plan_misses.get(key, 0) + 1
            self._plan_misses[key] = misses
            if misses < grace:
                append_spec(new_rc, key, spec)
                desired[key] = spec
            else:
                removals.append((key, spec))

        # 같은 이름의 type/direction 변동은 기존 endpoint를 유지한 채 defer한다.
        # endpoint가 완전히 사라진 뒤 재등장하면 일반 remove/add 세대로 처리된다.
        for key in active.keys() & desired.keys():
            if active[key] != desired[key]:
                log.warning("binding change deferred until endpoint rejoin: %s", key)
                set_spec(new_rc, key, active[key])
                desired[key] = active[key]

        additions = [(key, spec) for key, spec in desired.items() if key not in active]
        if not additions and not removals:
            return

        previous = self.provisioner.rc
        self.provisioner.rc = new_rc
        try:
            result = self._provision_staged()
        finally:
            self.provisioner.rc = previous
        if not result.ok:
            self._finish_route_staging()
            log.warning("staged discovery provisioning failed: %s", result.errors)
            return

        pending = PendingBindingPlan(
            new_rc,
            result,
            additions,
            removals,
            base_generation=self.lifecycle.snapshot.generation,
        )
        ev = InboundEvent(kind="_activate_plan", robot_id="-", interface="-",
                          correlation_id=None, event_id=f"plan:{time.time_ns()}",
                          payload=None, ct=None, spec=pending)
        if not self.inbound.put_control(ev):
            self._finish_route_staging()
            log.error("control lane full: staged binding plan was not activated")
            return
        if self.guard is not None:
            self.guard.trigger()
