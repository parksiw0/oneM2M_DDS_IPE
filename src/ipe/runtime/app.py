"""IPE 런타임 오케스트레이션 (DESIGN §2, §13).

스레드 소유권: executor 스레드가 모든 ROS2 호출을, oneM2M 워커가 유일한
CSE 쓰기를, 프로비저닝 워커가 GET-or-create 체인을 소유한다. 리스너는
전역 락 아래에서 admission + enqueue만 한다. 종료는 §13.9 순서를 따른다.
"""

from __future__ import annotations

import logging
import queue
import signal
import threading
import time
from contextlib import suppress
from typing import Any

from ipe.config.resolver import resolve
from ipe.config.spec import ResolvedConfig
from ipe.core.command import CommandDispatchManager
from ipe.core.common import TokenBucket
from ipe.core.policy import Pipeline
from ipe.core.transaction import ActionTransactionManager, ServiceTransactionManager
from ipe.onem2m.catchup import CatchUpSweeper
from ipe.onem2m.client import idify, make_onem2m_client
from ipe.onem2m.notification_server import NotificationServer
from ipe.onem2m.resource_ops import ResourceOps
from ipe.runtime.app_dispatch import DispatchMixin
from ipe.runtime.app_ops import OpsMixin
from ipe.runtime.app_workers import WorkersMixin
from ipe.runtime.discovery import GraphNotReady, await_graph_convergence
from ipe.runtime.dispatcher import Route, RouteTable
from ipe.runtime.lifecycle import IPEHealth, IPEPhase, IPEState, Lifecycle
from ipe.runtime.provisioning import Provisioner
from ipe.runtime.queues import (
    CLASS_TERMINAL,
    InboundQueue,
    OutboundQueue,
)
from ipe.runtime.state import StatePersistence
from ipe.runtime.type_support import type_support_requirement

log = logging.getLogger(__name__)

GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}


class IPEApp(DispatchMixin, WorkersMixin, OpsMixin):
    def __init__(self, rc: ResolvedConfig, args: Any) -> None:
        self.rc = rc
        self.args = args
        rec = rc.recovery
        storage = rc.storage
        self.state = StatePersistence(
            storage.get("state_db", "ipe_state.db"),
            backend=storage.get("backend", "sqlite"),
            dsn=storage.get("dsn"),
            schema=storage.get("schema", "ipe_state"),
            pool_min_size=storage.get("pool_min_size", 1),
            pool_max_size=storage.get("pool_max_size", 8),
            max_spool_entries=storage.get("max_spool_entries", 10000),
            max_spool_mb=storage.get("max_spool_mb", 64),
        )
        self.lifecycle = Lifecycle(self.state)
        self.inbound = InboundQueue(maxsize=rec.get("inbound_max", 1000),
                                    control_maxsize=rec.get("control_lane_max", 64))
        self.outbound = OutboundQueue(maxsize=rec.get("outbound_max", 5000))
        self.routes = RouteTable()
        self._routes_staging = threading.Event()
        self.protocol = rc.cse.protocol
        if self.protocol == "mqtt":
            mqtt = rc.cse.mqtt
            if mqtt is None:
                raise ValueError("cse.mqtt is required when cse.protocol is mqtt")
            # tinyIoT는 POA URI 경로부를 NOTIFY 토픽으로 그대로 쓴다(aei 무관)
            self.poa_path = idify(rc.cse.ae_name)
            self.poa = f"mqtt://{mqtt.host}:{mqtt.port}/{self.poa_path}"
        else:
            self.poa_path = ""
            self.poa = rc.cse.poa or f"http://127.0.0.1:{rc.notification_port}"

        # 전송 클라이언트: 스레드(worker / provisioner)마다 1개 (HTTP=Session, MQTT=연결)
        self.worker_client = make_onem2m_client(rc, rc.cse.origin)
        self.worker_ops = ResourceOps(self.worker_client)
        self.prov_client = make_onem2m_client(rc, rc.cse.origin)
        self.prov_ops = ResourceOps(self.prov_client)
        self.provisioner = Provisioner(rc, self.prov_ops, self.state, self.poa,
                                       protocol=self.protocol)
        self.catchup = CatchUpSweeper(self.state, self.prov_ops, self._catchup_admit)

        self.svc_tx = ServiceTransactionManager(self.state)
        self.act_tx = ActionTransactionManager(self.state)
        self.cmd_mgr = CommandDispatchManager(
            self._publish_command,
            lambda spec, payload: self.adapter.validate_command(spec, payload),
        )

        self.path_map: dict[tuple[str, str, str], str] = {}
        self.status_paths: dict[str, str] = {}
        self.specs_by_key: dict[tuple[str, str, str], Any] = {}
        self.pipeline: Pipeline | None = None
        self.adapter: Any = None
        self.node: Any = None
        self.executor: Any = None
        self.guard: Any = None
        self.aei: str | None = None

        self._admission_lock = threading.Lock()
        self._prov_jobs: queue.Queue = queue.Queue()
        self._shutdown = threading.Event()
        self._stop_worker = threading.Event()
        self._spool_pending = threading.Event()
        if self.state.spool_counts():
            self._spool_pending.set()
        self._cse_transport_down = False
        self._muted_pipeline: set[tuple[str, str]] = set()
        self._confirm_pending: dict[str, tuple[str, str, str]] = {}   # proposalId -> spec 키
        self._approval_prompter: Any = None
        self._approval_requests_emitted: set[str] = set()
        rate = float(rc.policy.get("max_total_write_hz", 0) or 0)
        # 0 = 무제한. BULK 전용(§9) — TERMINAL/LATEST 면제
        self._budget = TokenBucket(rate) if rate > 0 else None
        self._budget_dropped = 0
        self._inflight: dict[tuple[str, str], set[str]] = {}          # (robot,iface) -> corr들
        self._avail: dict[tuple[str, str, str], dict[str, Any]] = {}  # churn 상태기계(§4.6)
        self._plan_misses: dict[tuple[str, str, str], int] = {}
        self._resource_removal_pending: dict[tuple[str, str, str], Any] = {}
        self._deferred_type_support: dict[tuple[str, str, str], dict[str, Any]] = {}
        # qos FCNT 게시 상태 (QoS_FCNT_설계서 §4.5.2)
        self._qos_fcnt_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._qos_fcnt_last_pub: dict[tuple[str, str, str], float] = {}
        self._qos_fcnt_revision: dict[tuple[str, str, str], int] = {}
        self._qos_resource_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._qos_lbl_only: set[tuple[str, str, str]] = set()   # FCNT 생성 실패 키
        self._qos_republish = threading.Event()   # CSE 재기동 → 전량 재게시

    # ------------------------------------------------------------------
    # 기동
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Start transport, discover the ROS graph, and run the bridge lifecycle."""
        rc = self.rc
        target = self.poa if self.protocol == "mqtt" else rc.cse.endpoint
        log.info("IPE starting (%s): %s -> %s (AE %s)", self.protocol, rc.instance_id,
                 target, rc.cse.ae_name)
        self.lifecycle.set(IPEState.PREPARING, IPEPhase.VALIDATING_CONFIG)

        # S1: SUB 검증을 받을 listener와 oneM2M 전송만 준비한다. route는 비어 있다.
        try:
            self.server = self._make_listener()
            self.server.start()
            self.worker_client.start()
            self.prov_client.start()
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.TRANSPORT_READY)

            # S2-S3: application endpoint 없이 DDS Domain에 참여하고 graph 수렴을 기다린다.
            self._init_ros()
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.DDS_JOINED)
            disc = rc.discovery
            discovered = await_graph_convergence(
                self.adapter.snapshot,
                timeout_sec=float(disc.get("graph_settle_timeout_sec", 10)),
                stable_polls=int(disc.get("graph_stable_polls", 2)),
                poll_sec=float(disc.get("graph_poll_sec", 0.5)),
                spin_once=lambda timeout: self.executor.spin_once(timeout_sec=timeout),
            )
            self.lifecycle.set(
                IPEState.PREPARING, IPEPhase.GRAPH_DISCOVERED,
                detail=f"samples={discovered.samples}, elapsed={discovered.elapsed_sec:.2f}s")

            # S4: graph snapshot이 인터페이스 목록의 단일 권위다.
            resolved = resolve(rc.raw, discovered=discovered.snapshot)
            self._apply_saved_control_approvals(resolved)
            self._apply_saved_qos_overrides(resolved)
            self._defer_unloadable_types(resolved)
            self.rc = resolved
            self.provisioner.rc = resolved
            rc = resolved
            counts = self._binding_counts()
            if not any(counts.values()):
                raise GraphNotReady("ROS graph converged but has no bindable interfaces")
            self.lifecycle.set(
                IPEState.PREPARING, IPEPhase.BINDING_PLAN_RESOLVED,
                detail=str(counts))

            # S5-S7: AE/CSE identity 뒤에만 robot 리소스와 SUB를 staged 생성한다.
            self.aei = self.provisioner.ensure_ae_identity()
            self.worker_client.origin = self.aei
            self.prov_client.origin = self.aei
            self.provisioner.check_cse_identity()
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.AE_REGISTERED)
            result = self._provision_staged()
            if not result.ok:
                self._finish_route_staging()
                raise RuntimeError(f"provisioning failed: {result.errors}")
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.CSE_RESOURCES_PREPARED)
            for err in result.errors:
                self.emit_event("provisioningStatus", "error",
                                {"event": "provisionError", "detail": err})
            for fb in result.fallbacks:
                self.emit_event("provisioningStatus", "warning",
                                {"event": "fcntFallback", "detail": fb})
            for f in result.qos_fcnt_failed:
                self.emit_event("provisioningStatus", "warning",
                                {"event": "qosFcntUnavailable", "robot": f["robot"],
                                 "interface": f["interface"],
                                 "direction": f["direction"], "detail": f["error"]})
            self.lifecycle.set(IPEState.PREPARED, IPEPhase.CSE_PROVISIONED)
        except Exception as e:
            self.lifecycle.set(IPEState.NOT_READY, self.lifecycle_phase,
                               health=IPEHealth.FAILED, detail=str(e))
            log.exception("bootstrap failed")
            return self._abort_bootstrap()

        if getattr(self.args, "bootstrap_only", False):
            self._finish_route_staging()
            log.info("bootstrap complete (--bootstrap-only)")
            self.lifecycle.set(IPEState.STOPPED, IPEPhase.IDLE)
            return self._abort_bootstrap(code=0)

        # S8: staged CSE plan을 Pipeline에 주입한 뒤 ROS endpoint를 생성한다.
        self.path_map.update(result.path_map)
        large = rc.policy.get("suitability", {}).get("large_payload_bytes", 49152)
        self.pipeline = Pipeline(
            rc.topics,
            self.path_map,
            large_payload_bytes=large,
            cse_timezone=rc.cse.timezone,
        )
        bufs = self.state.get_kv("anomaly_bufs")
        if bufs:
            self.pipeline.anomaly.restore(bufs)
        self.lifecycle.set(IPEState.PREPARED, IPEPhase.BINDING, next_generation=True)
        if not self._bind_all():
            self.lifecycle.set(IPEState.NOT_READY, IPEPhase.BINDING,
                               health=IPEHealth.FAILED, detail="ROS endpoint rollback")
            return self._abort_bootstrap()

        # S8.5: endpoint가 모두 준비된 뒤 route와 binding generation을 활성화한다.
        self._absorb_provision(result)
        self._publish_deferred_type_support_status()
        self.lifecycle.set(IPEState.PREPARED, IPEPhase.BINDING_READY)

        self.node.create_timer(1.0, self._tick_1s)
        self.node.create_timer(float(rc.logging.get("heartbeat_sec", 30) or 30),
                               self._heartbeat)
        refresh = float(rc.discovery.get("refresh_sec", 0) or 0)
        if refresh > 0:
            self.node.create_timer(refresh, self._discovery_refresh)
        cu = float(rc.recovery.get("catch_up_sec", 0) or 0)
        if cu > 0:
            self.node.create_timer(cu, lambda: self._prov_jobs.put(("catchup", "periodic")))
        rcs = float(rc.recovery.get("reconcile_sec", 0) or 0)
        if rcs > 0:
            self.node.create_timer(rcs, lambda: self._prov_jobs.put(("reconcile", None)))

        self._publish_contracts()

        self.worker_thread = threading.Thread(target=self._outbound_worker,
                                              name="onem2m-worker", daemon=True)
        self.worker_thread.start()
        self.prov_thread = threading.Thread(target=self._prov_worker,
                                            name="provisioning-worker", daemon=True)
        self.prov_thread.start()

        self._boot_sweep()
        self._prov_jobs.put(("catchup", "boot"))

        signal.signal(signal.SIGINT, lambda *_: self._shutdown.set())
        signal.signal(signal.SIGTERM, lambda *_: self._shutdown.set())

        counts = {k: sum(1 for key in self.specs_by_key if key[0] == k)
                  for k in ("observe", "command", "service", "action")}
        self.lifecycle.set(IPEState.RUNNING, IPEPhase.IDLE, health=IPEHealth.HEALTHY)
        self.emit_event("ipeHealth", "info",
                        {"event": "running", **self.lifecycle.snapshot.__dict__})
        log.info("IPE running %s", counts)

        # 콜백 예외는 격리한다 — spin 루프는 죽으면 안 된다
        while not self._shutdown.is_set():
            try:
                self.executor.spin_once(timeout_sec=0.2)
            except Exception:
                log.exception("executor callback raised (isolated)")
                self.emit_event("ipeHealth", "error", {"event": "adapterError"})
        return self._graceful_shutdown()

    @property
    def lifecycle_phase(self) -> IPEPhase:
        return IPEPhase(self.lifecycle.snapshot.ipe_phase)

    def _binding_counts(self) -> dict[str, int]:
        return {
            "topics": sum(
                1 for x in self.rc.topics
                if x.msg_type and (
                    x.direction in ("observe", "both") or x.access_enabled
                )
            ),
            "services": sum(
                1 for x in self.rc.services if x.srv_type and x.access_enabled
            ),
            "actions": sum(
                1 for x in self.rc.actions if x.action_type and x.access_enabled
            ),
        }

    def _defer_unloadable_types(self, rc: ResolvedConfig) -> None:
        """로컬 type support가 없는 항목은 실패시키지 않고 다음 세대로 defer."""
        groups: tuple[tuple[Any, str, str, str], ...] = (
            (rc.topics, "msg_type", "msg", "topic"),
            (rc.services, "srv_type", "srv", "service"),
            (rc.actions, "action_type", "action", "action"),
        )
        previous = getattr(self, "_deferred_type_support", {})
        deferred: dict[tuple[str, str, str], dict[str, Any]] = {}
        available: set[tuple[str, str, str]] = set()
        seen: set[tuple[str, str, str]] = set()
        for items, attr, type_kind, binding_kind in groups:
            kept = []
            for spec in items:
                key = (binding_kind, spec.robot_id, spec.interface)
                seen.add(key)
                type_name = getattr(spec, attr)
                if type_name and self.adapter.type_available(type_kind, type_name):
                    kept.append(spec)
                    available.add(key)
                else:
                    requirement = type_support_requirement(
                        binding_kind, spec.robot_id, spec.interface, type_name)
                    deferred[key] = requirement
            items[:] = kept

        newly_deferred = {
            key for key, requirement in deferred.items()
            if previous.get(key) != requirement
        }
        recovered = previous.keys() & available
        disappeared = previous.keys() - deferred.keys() - available
        for key in newly_deferred:
            requirement = deferred[key]
            log.warning(
                "binding deferred (ROS 2 Type Support unavailable): "
                "%s [%s] install=%s",
                requirement["interface"],
                requirement.get("rosType") or "ambiguous",
                requirement.get("installPackage", "resolve interface type"),
            )
        for key in recovered:
            requirement = previous[key]
            log.info(
                "binding resumed (ROS 2 Type Support available): %s [%s]",
                requirement["interface"], requirement.get("rosType") or "resolved",
            )
        self._deferred_type_support = deferred

        # 초기 부팅에서는 status 경로가 아직 staged 상태다. 활성화 이후 호출되는
        # refresh부터는 상태 전이가 있을 때만 provisioningStatus에 남긴다.
        if getattr(self, "status_paths", None):
            self._publish_deferred_type_support_status(
                [deferred[key] for key in newly_deferred]
            )
            for key in recovered:
                self._publish_type_support_transition(
                    "typeSupportAvailable", previous[key]
                )
            for key in disappeared:
                if key not in seen:
                    self._publish_type_support_transition(
                        "typeSupportNoLongerRequired", previous[key]
                    )

    def _publish_deferred_type_support_status(
        self,
        requirements: Any = None,
    ) -> None:
        selected = (
            self._deferred_type_support.values()
            if requirements is None else requirements
        )
        for requirement in selected:
            self.emit_event(
                "provisioningStatus",
                "warning",
                {"event": "typeSupportUnavailable", **requirement},
            )

    def _publish_type_support_transition(
        self,
        event: str,
        requirement: dict[str, Any],
    ) -> None:
        payload = {key: value for key, value in requirement.items() if key != "reason"}
        self.emit_event(
            "provisioningStatus", "info", {"event": event, **payload}
        )

    def _init_ros(self) -> None:
        import rclpy
        from rclpy.executors import SingleThreadedExecutor
        from rclpy.node import Node

        rclpy.init()
        try:
            self.node = Node("ros2_onem2m_ipe", enable_rosout=False,
                             start_parameter_services=False)
        except TypeError:  # 구형 rclpy 호환
            self.node = Node("ros2_onem2m_ipe")
        from ipe.adapter.ros2 import GenericROS2Adapter
        self.adapter = GenericROS2Adapter(
            self.node, self._on_topic_ir, self.emit_event,
            qos_strictness=self.rc.policy.get("qos_strictness", "reject"))
        self.executor = SingleThreadedExecutor()
        self.executor.add_node(self.node)
        self.guard = self.node.create_guard_condition(self._drain_inbound)

    def _abort_bootstrap(self, code: int = 2) -> int:
        self._finish_route_staging()
        self._close_approval_prompt()
        server = getattr(self, "server", None)
        if server is not None:
            server.stop()
        if self.adapter is not None:
            with suppress(Exception):
                self.adapter.shutdown()
        if self.executor is not None:
            with suppress(Exception):
                self.executor.shutdown()
        if self.node is not None:
            with suppress(Exception):
                self.node.destroy_node()
        with suppress(Exception):
            import rclpy
            if rclpy.ok():
                rclpy.shutdown()
        self._stop_clients()
        self.state.close()
        return code

    def _make_listener(self) -> Any:
        rc = self.rc
        if self.protocol == "mqtt":
            from ipe.onem2m.mqtt_listener import MQTTNotificationListener
            mqtt = rc.cse.mqtt
            if mqtt is None:
                raise ValueError("cse.mqtt is required when cse.protocol is mqtt")
            return MQTTNotificationListener(
                mqtt, rc.cse.cse_id, self.poa_path,
                self._on_notify, self.routes.resolve_sur)
        return NotificationServer(rc.notification_host, rc.notification_port,
                                  on_notify=self._on_notify, diag_fn=self._diag)

    def _stop_clients(self) -> None:
        # stop()은 각 구현이 자체적으로 예외를 삼킨다(mqtt suppress, http session close)
        for c in (getattr(self, "worker_client", None), getattr(self, "prov_client", None)):
            if c is not None:
                c.stop()

    def _absorb_provision(self, result: Any) -> None:
        # 새 generation의 경로 사전을 먼저 완성한 뒤 참조를 교체한다. Pipeline도
        # 같은 사전을 보게 해 clear/update 중간 상태가 노출되지 않게 한다.
        self.path_map = dict(result.path_map)
        pipeline = getattr(self, "pipeline", None)
        if pipeline is not None:
            pipeline.path_map = self.path_map
        self.status_paths = dict(result.status_paths)
        self._qos_lbl_only = {
            (f["robot"], f["interface"], f["direction"])
            for f in getattr(result, "qos_fcnt_failed", [])
        }
        routes: dict[str, Route] = {}
        aliases: dict[str, str] = {}
        catchup_inputs: dict[str, str] = {}
        for path_key, r in result.routes.items():
            routes[path_key] = Route(r["kind"], r["robot_id"], r["interface"], dict(r))
            if r["kind"] != "qos_update":
                catchup_inputs[path_key] = r["input_cnt_path"]
            if self.protocol == "mqtt":
                # MQTT NOTIFY는 sur(SUB 구조 경로)로 라우팅
                cnt = r["input_cnt_path"].rstrip("/")
                aliases[(cnt + "/ipeSub").lstrip("/")] = path_key
                if r.get("sub_ri"):
                    aliases[r["sub_ri"]] = path_key
        self.routes.replace(routes, aliases)
        self.catchup.replace(catchup_inputs)
        finish_staging = getattr(self, "_finish_route_staging", None)
        if finish_staging is not None:
            finish_staging()

    def _bind_all(self) -> bool:
        ok = True
        for t in self.rc.topics:
            if t.direction in ("observe", "both") and t.msg_type:
                if self.adapter.bind_observe(t):
                    self.specs_by_key[("observe", t.robot_id, t.interface)] = t
                else:
                    ok = False
            if t.direction in ("command", "both") and t.access_enabled and t.msg_type:
                if getattr(t, "confirm", "auto") == "on_first_use" or self.adapter.bind_command(t):
                    self.specs_by_key[("command", t.robot_id, t.interface)] = t
                else:
                    ok = False
        for s in self.rc.services:
            if s.srv_type and s.access_enabled:
                if self.adapter.bind_service(s):
                    self.specs_by_key[("service", s.robot_id, s.interface)] = s
                else:
                    ok = False
        for a in self.rc.actions:
            if a.action_type and a.access_enabled:
                if self.adapter.bind_action(a):
                    self.specs_by_key[("action", a.robot_id, a.interface)] = a
                else:
                    ok = False
        if not ok:
            # generation 활성화 전이므로 새 endpoint만 제거하면 기존 route에는
            # 영향이 없다. 초기 기동에서는 이 generation 전체를 폐기한다.
            self.adapter.shutdown()
            self.specs_by_key.clear()
        return ok

    # ------------------------------------------------------------------
    # observe 경로 (executor 스레드)
    # ------------------------------------------------------------------

    def _graceful_shutdown(self) -> int:
        log.info("shutting down (§13.9)")
        self.server.stop()
        with suppress(Exception):
            self.adapter.publish_safety_stops()
        now = time.time()
        for ev in self.inbound.get_batch(10_000):
            corr = ev.correlation_id or ev.event_id or ""
            if ev.kind == "cancel":
                with suppress(Exception):
                    self._dispatch_one(ev, corr)
            elif ev.kind.startswith("_"):
                continue
            else:
                self.emit_event(self._status_category(ev.kind), "warning",
                                {"event": "rejected", "reason": "shuttingDown",
                                 "interface": ev.interface, "robot": ev.robot_id,
                                 "correlationId": corr})
                self._finish(ev.robot_id, ev.interface, corr, "shuttingDown", now)

        in_flight_states = {"invoked", "goalSent", "goalAccepted", "executing", "canceling"}
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not any(t["state"] in in_flight_states
                       for t in self.state.active_transactions()):
                break
            with suppress(Exception):
                self.executor.spin_once(timeout_sec=0.2)
        now = time.time()
        for t in self.state.active_transactions():
            if t["state"] in in_flight_states:
                terminal = "shutdownAbandoned" if t["kind"] == "action" else "failed"
                self.state.update_transaction(t["corr_id"], terminal, now)
                self.emit_event("actionStatus" if t["kind"] == "action"
                                else "serviceStatus", "warning",
                                {"event": "shutdownAbandoned",
                                 "correlationId": t["corr_id"]})

        self.emit_event("ipeHealth", "info", {"event": "shutdown"})
        flush_deadline = time.monotonic() + 5.0
        while not self.outbound.empty() and time.monotonic() < flush_deadline:
            time.sleep(0.1)
        while True:
            try:
                op = self.outbound.get_nowait()
            except queue.Empty:
                break
            if op.queue_class == CLASS_TERMINAL:
                self._spool_op(op)
        self._stop_worker.set()
        # 한 단계 실패가 나머지 정리를 막지 않게 단계별로 격리한다
        for step in (self._close_approval_prompt,
                     lambda: self.state.set_kv("anomaly_bufs",
                                               self.pipeline.anomaly.snapshot()
                                               if self.pipeline else {}),
                     self.adapter.shutdown,
                     self.executor.shutdown,
                     self.node.destroy_node,
                     self.worker_client.stop,
                     self.prov_client.stop):
            try:
                step()
            except Exception:
                log.exception("shutdown step failed (continuing)")
        try:
            import rclpy
            if rclpy.ok():
                rclpy.shutdown()
        except Exception:
            log.exception("rclpy shutdown failed")
        self.lifecycle.set(IPEState.STOPPED, IPEPhase.IDLE)
        self.state.close()
        log.info("shutdown complete")
        return 0

    def _close_approval_prompt(self) -> None:
        if self._approval_prompter is not None:
            self._approval_prompter.close()
            self._approval_prompter = None






def run(rc: ResolvedConfig, args: Any) -> int:
    return IPEApp(rc, args).run()
