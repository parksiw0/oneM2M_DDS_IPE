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
from typing import Any


from ipe.config.spec import ResolvedConfig
from ipe.core.command import CommandDispatchManager
from ipe.core.common import TokenBucket
from ipe.core.policy import Pipeline
from ipe.core.transaction import ActionTransactionManager, ServiceTransactionManager
from ipe.onem2m.catchup import CatchUpSweeper
from ipe.onem2m.client import idify, make_onem2m_client
from ipe.onem2m.notification_server import NotificationServer
from ipe.onem2m.resource_ops import ResourceOps
from ipe.runtime.dispatcher import RouteTable
from ipe.runtime.provisioning import Provisioner
from ipe.runtime.queues import (
    CLASS_TERMINAL,
    InboundQueue,
    OutboundQueue,
)
from ipe.runtime.state import StatePersistence

log = logging.getLogger(__name__)

GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}


from ipe.runtime.app_dispatch import DispatchMixin
from ipe.runtime.app_ops import OpsMixin
from ipe.runtime.app_workers import WorkersMixin


class IPEApp(DispatchMixin, WorkersMixin, OpsMixin):
    def __init__(self, rc: ResolvedConfig, args: Any) -> None:
        self.rc = rc
        self.args = args
        rec = rc.recovery
        self.state = StatePersistence(
            rc.storage.get("state_db", "ipe_state.db"),
            max_spool_entries=rc.storage.get("max_spool_entries", 10000),
            max_spool_mb=rc.storage.get("max_spool_mb", 64))
        self.inbound = InboundQueue(maxsize=rec.get("inbound_max", 1000),
                                    control_maxsize=rec.get("control_lane_max", 64))
        self.outbound = OutboundQueue(maxsize=rec.get("outbound_max", 5000))
        self.routes = RouteTable()
        self.protocol = rc.cse.protocol
        if self.protocol == "mqtt":
            # tinyIoT는 POA URI 경로부를 NOTIFY 토픽으로 그대로 쓴다(aei 무관)
            self.poa_path = idify(rc.cse.ae_name)
            self.poa = f"mqtt://{rc.cse.mqtt.host}:{rc.cse.mqtt.port}/{self.poa_path}"
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
        self.cmd_mgr = CommandDispatchManager(self._publish_command)

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
        self._muted_pipeline: set[tuple[str, str]] = set()
        self._confirm_pending: dict[str, tuple[str, str, str]] = {}   # proposalId -> spec 키
        rate = float(rc.policy.get("max_total_write_hz", 0) or 0)
        # 0 = 무제한. BULK 전용(§9) — TERMINAL/LATEST 면제
        self._budget = TokenBucket(rate) if rate > 0 else None
        self._budget_dropped = 0
        self._inflight: dict[tuple[str, str], set[str]] = {}          # (robot,iface) -> corr들
        self._avail: dict[tuple[str, str, str], dict[str, Any]] = {}  # churn 상태기계(§4.6)
        # qos FCNT 게시 상태 (QoS_FCNT_설계서 §4.5.2)
        self._qos_fcnt_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._qos_fcnt_last_pub: dict[tuple[str, str, str], float] = {}
        self._qos_lbl_only: set[tuple[str, str, str]] = set()   # FCNT 생성 실패 키
        self._qos_republish = threading.Event()   # CSE 재기동 → 전량 재게시

    # ------------------------------------------------------------------
    # 기동
    # ------------------------------------------------------------------

    def run(self) -> int:
        rc = self.rc
        log.info("IPE starting (%s): %s -> %s (AE %s)", self.protocol, rc.instance_id,
                 rc.cse.endpoint or self.poa, rc.cse.ae_name)

        # SUB 생성(vrq 검증) 전에 알림 리스너가 떠 있어야 한다 (MQTT는 SUBACK까지 블록)
        self.server = self._make_listener()
        self.server.start()
        try:
            # MQTT 전송은 브로커 연결이 필요(HTTP는 no-op)
            self.worker_client.start()
            self.prov_client.start()
            self.aei = self.provisioner.ensure_ae_identity()
            self.worker_client.origin = self.aei
            self.prov_client.origin = self.aei
            self.provisioner.check_cse_identity()
            result = self.provisioner.provision_all()
            self._absorb_provision(result)
            if not result.ok:
                log.error("provisioning failed: %s", result.errors)
                return 2
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
        except Exception:
            log.exception("bootstrap failed")
            self._stop_clients()
            self.server.stop()
            return 2

        if getattr(self.args, "bootstrap_only", False):
            log.info("bootstrap complete (--bootstrap-only)")
            self._stop_clients()
            self.server.stop()
            return 0

        import rclpy
        from rclpy.executors import SingleThreadedExecutor
        from rclpy.node import Node

        rclpy.init()
        self.node = Node("ros2_onem2m_ipe")
        from ipe.adapter.ros2 import GenericROS2Adapter
        self.adapter = GenericROS2Adapter(
            self.node, self._on_topic_ir, self.emit_event,
            qos_strictness=rc.policy.get("qos_strictness", "reject"))
        self.executor = SingleThreadedExecutor()
        self.executor.add_node(self.node)
        self.guard = self.node.create_guard_condition(self._drain_inbound)
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

        large = rc.policy.get("suitability", {}).get("large_payload_bytes", 49152)
        self.pipeline = Pipeline(rc.topics, self.path_map, large_payload_bytes=large)
        bufs = self.state.get_kv("anomaly_bufs")
        if bufs:
            self.pipeline.anomaly.restore(bufs)   # 워밍업 공백 제거(§7.4)
        self._bind_all()
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
        log.info("IPE running %s", counts)

        # 콜백 예외는 격리한다 — spin 루프는 죽으면 안 된다
        while not self._shutdown.is_set():
            try:
                self.executor.spin_once(timeout_sec=0.2)
            except Exception:
                log.exception("executor callback raised (isolated)")
                self.emit_event("ipeHealth", "error", {"event": "adapterError"})
        return self._graceful_shutdown()

    def _make_listener(self) -> Any:
        rc = self.rc
        if self.protocol == "mqtt":
            from ipe.onem2m.mqtt_listener import MQTTNotificationListener
            return MQTTNotificationListener(
                rc.cse.mqtt, rc.cse.cse_id, self.poa_path,
                self._on_notify, self.routes.resolve_sur)
        return NotificationServer(rc.notification_host, rc.notification_port,
                                  on_notify=self._on_notify, diag_fn=self._diag)

    def _stop_clients(self) -> None:
        # stop()은 각 구현이 자체적으로 예외를 삼킨다(mqtt suppress, http session close)
        for c in (getattr(self, "worker_client", None), getattr(self, "prov_client", None)):
            if c is not None:
                c.stop()

    def _absorb_provision(self, result: Any) -> None:
        self.path_map.update(result.path_map)
        self.status_paths.update(result.status_paths)
        for f in getattr(result, "qos_fcnt_failed", []):
            self._qos_lbl_only.add((f["robot"], f["interface"], f["direction"]))
        for path_key, r in result.routes.items():
            self.routes.add(path_key, r["kind"], r["robot_id"], r["interface"], meta=r)
            if r["kind"] != "qos_update":
                self.catchup.register(path_key, r["input_cnt_path"])
            if self.protocol == "mqtt":
                # MQTT NOTIFY는 sur(SUB 구조 경로)로 라우팅
                cnt = r["input_cnt_path"].rstrip("/")
                self.routes.add_alias((cnt + "/ipeSub").lstrip("/"), path_key)
                if r.get("sub_ri"):
                    self.routes.add_alias(r["sub_ri"], path_key)

    def _bind_all(self) -> None:
        for t in self.rc.topics:
            if t.direction in ("observe", "both") and t.msg_type:
                if self.adapter.bind_observe(t):
                    self.specs_by_key[("observe", t.robot_id, t.interface)] = t
            if t.direction in ("command", "both") and t.access_enabled and t.msg_type:
                if self.adapter.bind_command(t):
                    self.specs_by_key[("command", t.robot_id, t.interface)] = t
        for s in self.rc.services:
            if s.srv_type and self.adapter.bind_service(s):
                self.specs_by_key[("service", s.robot_id, s.interface)] = s
        for a in self.rc.actions:
            if a.action_type and self.adapter.bind_action(a):
                self.specs_by_key[("action", a.robot_id, a.interface)] = a

    # ------------------------------------------------------------------
    # observe 경로 (executor 스레드)
    # ------------------------------------------------------------------

    def _graceful_shutdown(self) -> int:
        log.info("shutting down (§13.9)")
        self.server.stop()
        try:
            self.adapter.publish_safety_stops()
        except Exception:
            pass
        now = time.time()
        for ev in self.inbound.get_batch(10_000):
            corr = ev.correlation_id or ev.event_id or ""
            if ev.kind == "cancel":
                try:
                    self._dispatch_one(ev, corr)
                except Exception:
                    pass
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
            try:
                self.executor.spin_once(timeout_sec=0.2)
            except Exception:
                pass
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
        for step in (lambda: self.state.set_kv("anomaly_bufs",
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
        self.state.close()
        log.info("shutdown complete")
        return 0






def run(rc: ResolvedConfig, args: Any) -> int:
    return IPEApp(rc, args).run()
