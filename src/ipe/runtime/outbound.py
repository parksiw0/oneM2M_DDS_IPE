"""Observation processing, bounded delivery workers, and durable terminal replay."""

from __future__ import annotations

import json
import logging
import math
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from ipe.core.common import TokenBucket
from ipe.core.models import TopicSpec
from ipe.core.pipeline import Op, Pipeline
from ipe.core.vocab import CLASS_OBSERVE_BULK, CLASS_TERMINAL
from ipe.onem2m.client import (
    OneM2MResponse,
    OversizeError,
    TransportError,
    backoff_delays,
    classify,
)
from ipe.onem2m.resource_ops import ResourceOps
from ipe.runtime.queues import OutboundQueue
from ipe.runtime.state import StatePersistence

if TYPE_CHECKING:
    from ipe.runtime.bindings import BindingRegistry

log = logging.getLogger(__name__)


SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2}

_UPDATE_KINDS = frozenset({"update_fcnt", "update_cnt"})


def send_operation(op: Op, ops: Any) -> OneM2MResponse | BaseException | None:
    """Return None on success, otherwise the response/error to classify."""
    try:
        if op.kind in _UPDATE_KINDS:
            response = getattr(ops, op.kind)(op.path, op.content)
        elif op.kind == "update_lbl":
            response = ops.update_lbl(op.path, op.content["labels"])
        elif op.kind == "create_cin":
            result = ops.create_cin(op.path, op.content, rn=op.rn, et=op.et)
            return None if result.created or result.duplicate else result.response
        else:
            return ValueError(f"unknown operation kind: {op.kind!r}")
        return None if response.ok else response
    except (TransportError, OversizeError, ValueError, TypeError, KeyError) as exc:
        return exc


def encode_operation(op: Op) -> str:
    """Persist the operation and its interface identity for serialized replay."""
    return json.dumps(asdict(op), ensure_ascii=False)


def decode_operation(payload: str, queue_class: str, legacy_key: str | None) -> Op:
    """Read current rows and older rows whose identity was stored only in key."""
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("spool payload must be an object")
    if not isinstance(data.get("kind", "create_cin"), str):
        raise ValueError("spool operation kind must be a string")
    if not isinstance(data.get("path"), str) or not isinstance(data.get("content"), dict):
        raise ValueError("spool operation needs a path and content object")
    if data.get("kind") == "update_lbl" and not isinstance(data["content"].get("labels"), list):
        raise ValueError("update_lbl needs a labels list")
    identity = (legacy_key or "").split(":", 2)
    robot, interface, view = identity if len(identity) == 3 else ("-", data["path"], "-")
    expires_at = data.get("expires_at")
    if expires_at is not None:
        expires_at = float(expires_at)
        if not math.isfinite(expires_at):
            raise ValueError("expires_at must be finite")
    return Op(
        kind=data.get("kind", "create_cin"),
        path=data["path"],
        content=data["content"],
        robot_id=str(data.get("robot_id", robot)),
        interface=str(data.get("interface", interface)),
        view=str(data.get("view", view)),
        queue_class=queue_class,
        rn=data.get("rn"),
        et=data.get("et"),
        expires_at=expires_at,
        oversized=data.get("oversized", False),
        anomalous=data.get("anomalous", False),
    )


class OutboundProcessor:
    """Own delivery state; ROS callbacks only process and enqueue observations."""

    def __init__(
        self,
        registry: BindingRegistry,
        state: StatePersistence,
        ops_pool: list[ResourceOps],
        request_recovery: Callable[[str], None],
    ) -> None:
        self.registry = registry
        self.state = state
        self.queue = OutboundQueue(maxsize=registry.rc.recovery.get("outbound_max", 5000))
        self._ops_pool = ops_pool
        self._request_recovery = request_recovery
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._spool_pending = threading.Event()
        if state.spool_counts():
            self._spool_pending.set()
        self._transport_state_lock = threading.Lock()
        self._cse_transport_down = False
        self.pipeline: Pipeline | None = None
        self._muted_pipeline: set[tuple[str, str]] = set()
        self._anomaly_last: dict[tuple[str, str], float] = {}
        rate = float(registry.rc.policy.get("max_total_write_hz", 0) or 0)
        self._budget = TokenBucket(rate) if rate > 0 else None
        self._budget_dropped = 0

    def configure_pipeline(self) -> None:
        rc = self.registry.rc
        self.pipeline = Pipeline(
            rc.topics,
            self.registry.path_map,
            large_payload_bytes=rc.policy.get("suitability", {}).get("large_payload_bytes", 49152),
            cse_timezone=rc.cse.timezone,
        )
        buffers = self.state.get_kv("anomaly_bufs")
        if buffers:
            self.pipeline.anomaly.restore(buffers)

    def update_paths(self) -> None:
        if self.pipeline is not None:
            self.pipeline.path_map = self.registry.path_map

    def add_topic(self, spec: TopicSpec) -> None:
        if self.pipeline is not None:
            self.pipeline.add_spec(spec)

    def remove_topic(self, robot: str, interface: str) -> None:
        if self.pipeline is not None:
            self.pipeline.remove_spec(robot, interface)

    def start(self) -> None:
        self._threads = [
            threading.Thread(
                target=self._outbound_worker,
                args=(index,),
                name=f"onem2m-worker-{index + 1}",
                daemon=True,
            )
            for index in range(len(self._ops_pool))
        ]
        for thread in self._threads:
            thread.start()

    def flush(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not self.queue.idle() and time.monotonic() < deadline:
            time.sleep(0.1)

    def request_stop(self) -> None:
        self._stop.set()

    def join(self) -> None:
        for thread in self._threads:
            thread.join(timeout=6.0)
            if thread.is_alive():
                log.warning("waiting for %s before closing resources", thread.name)
                thread.join()

    def persist_pending(self) -> None:
        """Called after all producers and delivery workers have stopped."""
        while True:
            try:
                op = self.queue.get_nowait()
            except queue.Empty:
                break
            if op.queue_class == CLASS_TERMINAL:
                self._spool_op(op)
        self.state.set_kv("anomaly_bufs", self.pipeline.anomaly.snapshot() if self.pipeline else {})

    def diagnostics(self) -> dict[str, Any]:
        anomaly = self.pipeline.anomaly if self.pipeline is not None else None
        return {
            "outbound": self.queue.depths(),
            "dropped": self.queue.dropped_counters(),
            "spool": self.state.spool_counts(),
            "muted": [f"{r}:{i}" for r, i in self._muted_pipeline],
            "anomaly_suppressed": dict(anomaly.suppressed if anomaly is not None else {}),
            "budget_dropped": self._budget_dropped,
        }

    def on_topic_ir(self, ir: Any) -> None:
        if self.pipeline is None:
            return
        try:
            ops = self.pipeline.process(ir)
        except Exception as e:
            # 같은 인터페이스의 반복 실패는 1회만 보고 (이벤트 폭주 방지)
            key = (ir["robot_id"], ir["interface_name"])
            if key not in self._muted_pipeline:
                self._muted_pipeline.add(key)
                self.emit_event(
                    "topicHealth",
                    "error",
                    {
                        "event": "pipelineError",
                        "interface": key[1],
                        "robot": key[0],
                        "error": str(e),
                        "muted": True,
                    },
                )
            return
        for op in ops:
            if op.oversized:
                self.emit_event(
                    "topicHealth",
                    "warning",
                    {"event": "payloadOversize", "interface": op.interface, "robot": op.robot_id},
                )
            if getattr(op, "anomalous", False):
                self._anomaly_event(op)
            if (
                op.queue_class == CLASS_OBSERVE_BULK
                and self._budget is not None
                and not self._budget.allow()
            ):
                self._budget_dropped += 1
                continue
            if not self.queue.put(op, op.queue_class) and op.queue_class == CLASS_TERMINAL:
                self._spool_op(op)

    def _anomaly_event(self, op: Any) -> None:
        # CIN 자체는 매번 가고(fast-path), 알림 이벤트만 인터페이스당 5s coalesce
        key = (op.robot_id, op.interface)
        now = time.monotonic()
        last = self._anomaly_last
        if key in last and now - last[key] < 5.0:
            return
        last[key] = now
        self.emit_event(
            "topicHealth",
            "warning",
            {
                "event": "anomalyDetected",
                "interface": op.interface,
                "robot": op.robot_id,
                "anomaly": (op.content or {}).get("anomaly"),
            },
        )

    def _spool_op(self, op: Op) -> None:
        self.state.spool_put(
            op.queue_class,
            f"{op.robot_id}:{op.interface}:{op.view}",
            encode_operation(op),
            time.time(),
        )
        self._spool_pending.set()

    def _mark_cse_unavailable(self) -> None:
        with self._transport_state_lock:
            self._cse_transport_down = True

    def _mark_cse_available(self) -> bool:
        with self._transport_state_lock:
            if not self._cse_transport_down:
                return False
            self._cse_transport_down = False
        self._request_recovery("recover")
        return True

    def _outbound_worker(self, worker_index: int) -> None:
        rec = self.registry.rc.recovery
        retries = int(rec.get("retry_count", 3))
        base_ms = int(rec.get("retry_delay_ms", 500))
        ops = self._ops_pool[worker_index]
        next_spool_attempt = 0.0
        while not self._stop.is_set():
            try:
                now = time.monotonic()
                if worker_index == 0 and self._spool_pending.is_set() and now >= next_spool_attempt:
                    self._spool_pending.clear()
                    self._drain_spool(ops)
                    next_spool_attempt = time.monotonic() + max(0.5, base_ms / 1000.0)
                try:
                    op = self.queue.claim(timeout=0.5)
                except queue.Empty:
                    continue
                try:
                    self._send_with_retry(op, retries, base_ms, ops)
                finally:
                    self.queue.release(op)
            except Exception:
                log.exception("outbound worker iteration failed (isolated)")

    def _send_with_retry(self, op: Op, retries: int, base_ms: int, ops: Any) -> None:
        if not self._deliver(op, retries, base_ms, ops) and op.queue_class == CLASS_TERMINAL:
            self._spool_op(op)

    def _deliver(self, op: Op, retries: int, base_ms: int, ops: Any) -> bool:
        """True means consumed (sent, expired or rejected); False means retain."""
        factor = 1.0 if self.registry.rc.recovery.get("backoff") == "fixed" else 2.0
        delays = iter(backoff_delays(retries, base_ms, factor=factor))
        for attempt in range(retries + 1):
            if self._stop.is_set():
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
                    self._request_recovery("reconcile")
                return False
            if attempt == retries:
                self._mark_cse_unavailable()
                return False
            if self._stop.wait(next(delays) / 1000.0):
                return False
        return False

    def _report_dropped_op(self, op: Op, failure: Any) -> None:
        log.error("non-recoverable op dropped: %s (%s)", op.path, failure)
        # A rejected health event must not recursively generate another one.
        if op.view != "ipeHealth":
            self.emit_event(
                "ipeHealth",
                "error",
                {
                    "event": "opFailed",
                    "path": op.path,
                    "interface": op.interface,
                    "robot": op.robot_id,
                    "kind": op.kind,
                },
            )

    def _drain_spool(self, ops: Any) -> None:
        try:
            for row in self.state.spool_list(limit=20):
                if self._stop.is_set():
                    return
                try:
                    op = decode_operation(row["payload"], row["class"], row["key"])
                except (ValueError, TypeError) as exc:
                    log.error("invalid spool row %s dropped: %s", row["id"], exc)
                    self.state.spool_delete([row["id"]])
                    continue
                if not self.queue.try_claim(op):
                    return
                try:
                    if not self._deliver(op, 0, 0, ops):
                        return
                    self.state.spool_delete([row["id"]])
                finally:
                    self.queue.release(op)
        finally:
            # Preserve the wakeup even if decoding, storage or a client fails.
            if self.state.spool_list(limit=1):
                self._spool_pending.set()

    def emit_event(self, category: str, severity: str, payload: dict[str, Any]) -> None:
        min_sev = self.registry.rc.logging.get("status_severity_min", "info")
        if SEVERITY_ORDER.get(severity, 0) < SEVERITY_ORDER.get(min_sev, 0):
            return
        body = {"severity": severity, "ts": time.time(), **payload}
        level = {"info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}.get(
            severity, logging.WARNING
        )
        log.log(level, "[%s] %s", category, body)
        path = self.registry.status_paths.get(category)
        if path is None:
            return
        self.put_terminal(
            Op(
                "create_cin",
                path,
                body,
                payload.get("robot", "-"),
                payload.get("interface", "-"),
                category,
                CLASS_TERMINAL,
            )
        )

    def put_terminal(self, op: Op) -> None:
        if not self.queue.put(op, CLASS_TERMINAL):
            self._spool_op(op)
