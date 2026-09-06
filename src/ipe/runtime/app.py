"""Application composition and ROS/oneM2M startup and shutdown ordering."""

from __future__ import annotations

import logging
import signal
import threading
from contextlib import suppress
from typing import Any

from ipe.models import ResolvedConfig
from ipe.onem2m.client import idify, make_onem2m_client
from ipe.onem2m.notification_server import NotificationServer
from ipe.onem2m.resource_ops import ResourceOps
from ipe.runtime.bindings import BindingManager, BindingRegistry
from ipe.runtime.discovery import GraphNotReady, await_graph_convergence
from ipe.runtime.inbound import InboundProcessor
from ipe.runtime.lifecycle import IPEHealth, IPEPhase, IPEState, Lifecycle
from ipe.runtime.outbound import OutboundProcessor
from ipe.runtime.planning import resolve
from ipe.runtime.provisioning import Provisioner
from ipe.runtime.state import StatePersistence
from ipe.runtime.status import StatusPublisher

log = logging.getLogger(__name__)


class IPEApp:
    def __init__(self, rc: ResolvedConfig, args: Any) -> None:
        self.registry = BindingRegistry(rc)
        self.args = args
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
        self.protocol = rc.cse.protocol
        if self.protocol == "mqtt":
            mqtt = rc.cse.mqtt
            if mqtt is None:
                raise ValueError("cse.mqtt is required when cse.protocol is mqtt")
            self.poa_path = idify(rc.cse.ae_name)
            self.poa = f"mqtt://{mqtt.host}:{mqtt.port}/{self.poa_path}"
        else:
            self.poa_path = ""
            self.poa = rc.cse.poa or f"http://127.0.0.1:{rc.notification_port}"
        self.outbound_worker_count = int(rc.recovery.get("outbound_workers", 8))
        if not 1 <= self.outbound_worker_count <= 8:
            raise ValueError("outbound_workers must be within 1..8")
        if self.protocol == "mqtt":
            shared_client = make_onem2m_client(rc, rc.cse.origin)
            self.worker_clients = [shared_client] * self.outbound_worker_count
        else:
            self.worker_clients = [
                make_onem2m_client(rc, rc.cse.origin) for _ in range(self.outbound_worker_count)
            ]
        worker_ops = [ResourceOps(client) for client in self.worker_clients]
        self.prov_client = make_onem2m_client(rc, rc.cse.origin)
        prov_ops = ResourceOps(self.prov_client)
        self.provisioner = Provisioner(rc, prov_ops, self.state, self.poa, protocol=self.protocol)
        self.outbound = OutboundProcessor(
            self.registry,
            self.state,
            worker_ops,
            lambda job: self.bindings.request(job),
        )
        self.status = StatusPublisher(self.registry, self.outbound)
        self.inbound = InboundProcessor(
            self.registry,
            self.state,
            prov_ops,
            self.outbound,
            self.status,
            lambda event: self.bindings.handle_event(event),
        )
        self.bindings = BindingManager(
            self.registry,
            self.provisioner,
            prov_ops,
            self.lifecycle,
            self.inbound,
            self.outbound,
            self.status,
        )
        self.adapter: Any = None
        self.node: Any = None
        self.executor: Any = None
        self.server: Any = None
        self._shutdown = threading.Event()

    def run(self) -> int:
        """Start transport, discover the ROS graph, and run the bridge lifecycle."""
        rc = self.registry.rc
        target = self.poa if self.protocol == "mqtt" else rc.cse.endpoint
        log.info(
            "IPE starting (%s): %s -> %s (AE %s)",
            self.protocol,
            rc.instance_id,
            target,
            rc.cse.ae_name,
        )
        self.lifecycle.set(IPEState.PREPARING, IPEPhase.VALIDATING_CONFIG)

        # S1: SUB 검증을 받을 listener와 oneM2M 전송만 준비한다. route는 비어 있다.
        try:
            self.server = self._make_listener()
            self.server.start()
            for client in {id(c): c for c in self.worker_clients}.values():
                client.start()
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
                IPEState.PREPARING,
                IPEPhase.GRAPH_DISCOVERED,
                detail=f"samples={discovered.samples}, elapsed={discovered.elapsed_sec:.2f}s",
            )

            # S4: graph snapshot이 인터페이스 목록의 단일 권위다.
            resolved = resolve(rc.raw, discovered=discovered.snapshot)
            self.inbound.apply_saved_control_approvals(resolved)
            self.inbound.apply_saved_qos_overrides(resolved)
            self.bindings.defer_unloadable_types(resolved)
            self.registry.rc = resolved
            self.provisioner.rc = resolved
            rc = resolved
            counts = self.bindings.binding_counts()
            if not any(counts.values()):
                raise GraphNotReady("ROS graph converged but has no bindable interfaces")
            self.lifecycle.set(
                IPEState.PREPARING, IPEPhase.BINDING_PLAN_RESOLVED, detail=str(counts)
            )

            # S5-S7: AE/CSE identity 뒤에만 robot 리소스와 SUB를 staged 생성한다.
            self.registry.aei = self.provisioner.ensure_ae_identity()
            for client in {id(c): c for c in self.worker_clients}.values():
                client.origin = self.registry.aei
            self.prov_client.origin = self.registry.aei
            self.provisioner.check_cse_identity()
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.AE_REGISTERED)
            result = self.bindings.provision_staged()
            if not result.ok:
                self.bindings.finish_staging()
                raise RuntimeError(f"provisioning failed: {result.errors}")
            self.lifecycle.set(IPEState.PREPARING, IPEPhase.CSE_RESOURCES_PREPARED)
            for err in result.errors:
                self.outbound.emit_event(
                    "provisioningStatus", "error", {"event": "provisionError", "detail": err}
                )
            for fb in result.fallbacks:
                self.outbound.emit_event(
                    "provisioningStatus", "warning", {"event": "fcntFallback", "detail": fb}
                )
            for f in result.qos_fcnt_failed:
                self.outbound.emit_event(
                    "provisioningStatus",
                    "warning",
                    {
                        "event": "qosFcntUnavailable",
                        "robot": f["robot"],
                        "interface": f["interface"],
                        "direction": f["direction"],
                        "detail": f["error"],
                    },
                )
            self.lifecycle.set(IPEState.PREPARED, IPEPhase.CSE_PROVISIONED)
        except Exception as e:
            self.lifecycle.set(
                IPEState.NOT_READY, self.lifecycle_phase, health=IPEHealth.FAILED, detail=str(e)
            )
            log.exception("bootstrap failed")
            return self._abort_bootstrap()

        if getattr(self.args, "bootstrap_only", False):
            self.bindings.finish_staging()
            log.info("bootstrap complete (RUN_MODE=bootstrap)")
            self.lifecycle.set(IPEState.STOPPED, IPEPhase.IDLE)
            return self._abort_bootstrap(code=0)

        # S8: staged CSE plan을 Pipeline에 주입한 뒤 ROS endpoint를 생성한다.
        self.registry.path_map.update(result.path_map)
        self.outbound.configure_pipeline()
        self.lifecycle.set(IPEState.PREPARED, IPEPhase.BINDING, next_generation=True)
        if not self.bindings.bind_all():
            self.lifecycle.set(
                IPEState.NOT_READY,
                IPEPhase.BINDING,
                health=IPEHealth.FAILED,
                detail="ROS endpoint rollback",
            )
            return self._abort_bootstrap()

        # S8.5: endpoint가 모두 준비된 뒤 route와 binding generation을 활성화한다.
        self.bindings.absorb_provision(result)
        self.bindings.publish_deferred_type_support_status()
        self.lifecycle.set(IPEState.PREPARED, IPEPhase.BINDING_READY)

        self.node.create_timer(1.0, self._tick_1s)
        self.node.create_timer(float(rc.logging.get("heartbeat_sec", 30) or 30), self._heartbeat)
        refresh = float(rc.discovery.get("refresh_sec", 0) or 0)
        if refresh > 0:
            self.node.create_timer(refresh, self.bindings.refresh)
        cu = float(rc.recovery.get("catch_up_sec", 0) or 0)
        if cu > 0:
            self.node.create_timer(cu, lambda: self.bindings.request("catchup", "periodic"))
        rcs = float(rc.recovery.get("reconcile_sec", 0) or 0)
        if rcs > 0:
            self.node.create_timer(rcs, lambda: self.bindings.request("reconcile"))

        self.inbound.publish_contracts()

        self.outbound.start()
        self.bindings.start()

        self.inbound.boot_sweep()
        self.bindings.request("catchup", "boot")

        signal.signal(signal.SIGINT, lambda *_: self._shutdown.set())
        signal.signal(signal.SIGTERM, lambda *_: self._shutdown.set())

        counts = {
            k: sum(1 for key in self.registry.specs_by_key if key[0] == k)
            for k in ("observe", "command", "service", "action")
        }
        self.lifecycle.set(IPEState.RUNNING, IPEPhase.IDLE, health=IPEHealth.HEALTHY)
        self.outbound.emit_event(
            "ipeHealth", "info", {"event": "running", **self.lifecycle.snapshot.__dict__}
        )
        log.info("IPE running %s", counts)

        # 콜백 예외는 격리한다 — spin 루프는 죽으면 안 된다
        while not self._shutdown.is_set():
            try:
                self.executor.spin_once(timeout_sec=0.2)
            except Exception:
                log.exception("executor callback raised (isolated)")
                self.outbound.emit_event("ipeHealth", "error", {"event": "adapterError"})
        return self._graceful_shutdown()

    @property
    def lifecycle_phase(self) -> IPEPhase:
        return IPEPhase(self.lifecycle.snapshot.ipe_phase)

    def _init_ros(self) -> None:
        import rclpy
        from rclpy.executors import SingleThreadedExecutor
        from rclpy.node import Node

        rclpy.init()
        try:
            self.node = Node("ros2_onem2m_ipe", enable_rosout=False, start_parameter_services=False)
        except TypeError:  # 구형 rclpy 호환
            self.node = Node("ros2_onem2m_ipe")
        from ipe.adapter.ros2 import GenericROS2Adapter

        self.adapter = GenericROS2Adapter(
            self.node,
            self.outbound.on_topic_ir,
            self.outbound.emit_event,
            qos_strictness=self.registry.rc.policy.get("qos_strictness", "reject"),
            self_echo_window_sec=self.registry.rc.policy.get("self_echo_window_sec", 0.5),
            qos_event_coalesce_sec=self.registry.rc.policy.get("qos_event_coalesce_sec", 5.0),
        )
        self.executor = SingleThreadedExecutor()
        self.executor.add_node(self.node)
        guard = self.node.create_guard_condition(self.inbound.drain)
        self.inbound.attach_adapter(self.adapter, guard.trigger)
        self.status.attach_adapter(self.adapter)
        self.bindings.attach_adapter(self.adapter)

    def _abort_bootstrap(self, code: int = 2) -> int:
        self.bindings.finish_staging()
        self.inbound.close()
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
        rc = self.registry.rc
        if self.protocol == "mqtt":
            from ipe.onem2m.mqtt_listener import MQTTNotificationListener

            mqtt = rc.cse.mqtt
            if mqtt is None:
                raise ValueError("cse.mqtt is required when cse.protocol is mqtt")
            return MQTTNotificationListener(
                mqtt,
                rc.cse.cse_id,
                self.poa_path,
                self.inbound.on_notify,
                self.registry.routes.resolve_sur,
            )
        return NotificationServer(
            rc.notification_host,
            rc.notification_port,
            on_notify=self.inbound.on_notify,
            diag_fn=self.diagnostics,
        )

    def _stop_clients(self) -> None:
        clients = [*getattr(self, "worker_clients", []), getattr(self, "prov_client", None)]
        for c in {id(c): c for c in clients if c is not None}.values():
            if c is not None:
                c.stop()

    def _tick_1s(self) -> None:
        self.status.tick()
        self.inbound.sweep_timeouts()

    def diagnostics(self) -> dict[str, Any]:
        return {
            **self.bindings.diagnostics(),
            **self.inbound.diagnostics(),
            **self.outbound.diagnostics(),
            **self._transport_status(),
        }

    def _transport_status(self) -> dict[str, Any]:
        st: dict[str, Any] = {
            "transport": self.protocol,
            "outboundWorkers": self.outbound_worker_count,
        }
        if self.protocol == "mqtt":
            st["connected"] = {
                "worker": all(getattr(c, "connected", False) for c in self.worker_clients),
                "prov": getattr(self.prov_client, "connected", None),
                "listener": getattr(getattr(self, "server", None), "connected", None),
            }
        return st

    def _heartbeat(self) -> None:
        self.outbound.emit_event(
            "ipeHealth",
            "info",
            {
                "event": "heartbeat",
                "inbound": self.inbound.queue.depths(),
                "outbound": self.outbound.queue.depths(),
                "dropped": self.outbound.queue.dropped_counters(),
                "spool": self.state.spool_counts(),
                **self._transport_status(),
            },
        )

    def _graceful_shutdown(self) -> int:
        log.info("shutting down")
        self.server.stop()
        self.bindings.request_stop()
        self.bindings.join()
        self.inbound.close()
        with suppress(Exception):
            self.adapter.publish_safety_stops()
        self.inbound.shutdown(lambda timeout: self.executor.spin_once(timeout_sec=timeout))
        self.outbound.emit_event("ipeHealth", "info", {"event": "shutdown"})
        self.outbound.flush()
        self.outbound.request_stop()
        self.outbound.join()
        # No producer can add terminal records after this point.
        for step in (
            self.outbound.persist_pending,
            self.adapter.shutdown,
            self.executor.shutdown,
            self.node.destroy_node,
            self._stop_clients,
        ):
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


def run(rc: ResolvedConfig, args: Any) -> int:
    return IPEApp(rc, args).run()
