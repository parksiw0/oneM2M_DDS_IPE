"""Active binding registry, staged graph changes, and CSE provisioning lifecycle."""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from ipe.models import ActionSpec, ResolvedConfig, ServiceSpec, TopicSpec
from ipe.onem2m.resource_ops import ResourceOps
from ipe.runtime.dispatcher import InboundEvent, Route, RouteTable
from ipe.runtime.lifecycle import IPEHealth, IPEPhase, IPEState, Lifecycle
from ipe.runtime.provisioning import Provisioner

if TYPE_CHECKING:
    from ipe.runtime.inbound import InboundProcessor
    from ipe.runtime.outbound import OutboundProcessor
    from ipe.runtime.status import StatusPublisher

log = logging.getLogger(__name__)


PlanKey = tuple[str, str, str]  # kind, robot_id, interface


def binding_map(rc: ResolvedConfig) -> dict[PlanKey, Any]:
    out: dict[PlanKey, Any] = {}
    out.update(
        {
            ("topic", x.robot_id, x.interface): x
            for x in rc.topics
            if x.msg_type and (x.direction in ("observe", "both") or x.access_enabled)
        }
    )
    out.update(
        {
            ("service", x.robot_id, x.interface): x
            for x in rc.services
            if x.srv_type and x.access_enabled
        }
    )
    out.update(
        {
            ("action", x.robot_id, x.interface): x
            for x in rc.actions
            if x.action_type and x.access_enabled
        }
    )
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
    target[:] = [x for x in target if not (x.robot_id == robot_id and x.interface == interface)]
    target.append(spec)


@dataclass
class PendingBindingPlan:
    rc: ResolvedConfig
    provision: Any
    additions: list[tuple[PlanKey, Any]]
    removals: list[tuple[PlanKey, Any]]
    base_generation: int | None = None


def type_support_requirement(
    binding_kind: str,
    robot_id: str,
    interface: str,
    ros_type: str | None,
) -> dict[str, Any]:
    """누락된 Type Support를 운영자가 조치할 수 있는 형태로 설명한다."""
    item: dict[str, Any] = {
        "kind": binding_kind,
        "robot": robot_id,
        "interface": interface,
        "rosType": ros_type,
        "reason": "typeSupportUnavailable" if ros_type else "ambiguousType",
    }
    if not ros_type:
        return item

    ros_package = ros_type.split("/", 1)[0]
    item["rosPackage"] = ros_package
    distro = os.environ.get("ROS_DISTRO", "").strip().lower()
    if distro:
        item["installPackage"] = f"ros-{distro}-{ros_package.lower().replace('_', '-')}"
    return item


@dataclass
class BindingRegistry:
    """Active interface data shared by processors; no worker or request state."""

    rc: ResolvedConfig
    path_map: dict[tuple[str, str, str], str] = field(default_factory=dict)
    status_paths: dict[str, str] = field(default_factory=dict)
    specs_by_key: dict[tuple[str, str, str], Any] = field(default_factory=dict)
    aei: str | None = None
    routes: RouteTable = field(default_factory=RouteTable)
    routes_staging: threading.Event = field(default_factory=threading.Event)


class BindingManager:
    def __init__(
        self,
        registry: BindingRegistry,
        provisioner: Provisioner,
        prov_ops: ResourceOps,
        lifecycle: Lifecycle,
        inbound: InboundProcessor,
        outbound: OutboundProcessor,
        status: StatusPublisher,
    ) -> None:
        self.registry = registry
        self.provisioner = provisioner
        self.prov_ops = prov_ops
        self.lifecycle = lifecycle
        self.inbound = inbound
        self.outbound = outbound
        self.status = status
        self.adapter: Any = None
        self._jobs: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._avail: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._plan_misses: dict[tuple[str, str, str], int] = {}
        self._resource_removal_pending: dict[tuple[str, str, str], Any] = {}
        self._deferred_type_support: dict[tuple[str, str, str], dict[str, Any]] = {}

    def attach_adapter(self, adapter: Any) -> None:
        self.adapter = adapter

    def request(self, job: str, arg: Any = None) -> None:
        self._jobs.put((job, arg))

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._prov_worker, name="provisioning-worker", daemon=True
        )
        self._thread.start()

    def request_stop(self) -> None:
        self._stop.set()

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join(timeout=6.0)
            if self._thread.is_alive():
                log.warning("waiting for provisioning worker before closing resources")
                self._thread.join()

    def diagnostics(self) -> dict[str, Any]:
        return {
            "aei": self.registry.aei,
            "bound": {f"{k[0]}:{k[1]}:{k[2]}": True for k in self.registry.specs_by_key},
            "routes": len(self.registry.routes),
            "availability": {f"{k[1]}:{k[2]}": v["state"] for k, v in self._avail.items()},
            "deferred_type_support": sorted(
                self._deferred_type_support.values(),
                key=lambda item: (item["kind"], item["robot"], item["interface"]),
            ),
        }

    def provision_staged(self) -> Any:
        self.registry.routes_staging.set()
        try:
            return self.provisioner.provision_all()
        except Exception:
            self.finish_staging()
            raise

    def finish_staging(self) -> None:
        self.registry.routes_staging.clear()

    def binding_counts(self) -> dict[str, int]:
        return {
            "topics": sum(
                1
                for x in self.registry.rc.topics
                if x.msg_type and (x.direction in ("observe", "both") or x.access_enabled)
            ),
            "services": sum(
                1 for x in self.registry.rc.services if x.srv_type and x.access_enabled
            ),
            "actions": sum(
                1 for x in self.registry.rc.actions if x.action_type and x.access_enabled
            ),
        }

    def defer_unloadable_types(self, rc: ResolvedConfig) -> None:
        """로컬 type support가 없는 항목은 실패시키지 않고 다음 세대로 defer."""
        groups: tuple[tuple[Any, str, str, str], ...] = (
            (rc.topics, "msg_type", "msg", "topic"),
            (rc.services, "srv_type", "srv", "service"),
            (rc.actions, "action_type", "action", "action"),
        )
        previous = self._deferred_type_support
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
                        binding_kind, spec.robot_id, spec.interface, type_name
                    )
                    deferred[key] = requirement
            items[:] = kept

        newly_deferred = {
            key for key, requirement in deferred.items() if previous.get(key) != requirement
        }
        recovered = previous.keys() & available
        disappeared = previous.keys() - deferred.keys() - available
        for key in newly_deferred:
            requirement = deferred[key]
            log.warning(
                "binding deferred (ROS 2 Type Support unavailable): %s [%s] install=%s",
                requirement["interface"],
                requirement.get("rosType") or "ambiguous",
                requirement.get("installPackage", "resolve interface type"),
            )
        for key in recovered:
            requirement = previous[key]
            log.info(
                "binding resumed (ROS 2 Type Support available): %s [%s]",
                requirement["interface"],
                requirement.get("rosType") or "resolved",
            )
        self._deferred_type_support = deferred

        # 초기 부팅에서는 status 경로가 아직 staged 상태다. 활성화 이후 호출되는
        # refresh부터는 상태 전이가 있을 때만 provisioningStatus에 남긴다.
        if self.registry.status_paths:
            self.publish_deferred_type_support_status([deferred[key] for key in newly_deferred])
            for key in recovered:
                self._publish_type_support_transition("typeSupportAvailable", previous[key])
            for key in disappeared:
                if key not in seen:
                    self._publish_type_support_transition(
                        "typeSupportNoLongerRequired", previous[key]
                    )

    def publish_deferred_type_support_status(
        self,
        requirements: Any = None,
    ) -> None:
        selected = self._deferred_type_support.values() if requirements is None else requirements
        for requirement in selected:
            self.outbound.emit_event(
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
        self.outbound.emit_event("provisioningStatus", "info", {"event": event, **payload})

    def absorb_provision(self, result: Any) -> None:
        if not result.ok:
            self.finish_staging()
            raise RuntimeError(f"provisioning failed; active routes retained: {result.errors}")
        # 새 generation의 경로 사전을 먼저 완성한 뒤 참조를 교체한다. Pipeline도
        # 같은 사전을 보게 해 clear/update 중간 상태가 노출되지 않게 한다.
        self.registry.path_map = dict(result.path_map)
        self.outbound.update_paths()
        self.registry.status_paths = dict(result.status_paths)
        routes: dict[str, Route] = {}
        aliases: dict[str, str] = {}
        catchup_inputs: dict[str, str] = {}
        for path_key, r in result.routes.items():
            routes[path_key] = Route(r["kind"], r["robot_id"], r["interface"], dict(r))
            if r["kind"] != "qos_update":
                catchup_inputs[path_key] = r["input_cnt_path"]
            if self.registry.rc.cse.protocol == "mqtt":
                # MQTT NOTIFY는 sur(SUB 구조 경로)로 라우팅
                cnt = r["input_cnt_path"].rstrip("/")
                aliases[(cnt + "/ipeSub").lstrip("/")] = path_key
                if r.get("sub_ri"):
                    aliases[r["sub_ri"]] = path_key
        self.registry.routes.replace(routes, aliases)
        self.inbound.catchup.replace(catchup_inputs)
        self.finish_staging()

    def bind_all(self) -> bool:
        ok = True
        for t in self.registry.rc.topics:
            if t.direction in ("observe", "both") and t.msg_type:
                if self.adapter.bind_observe(t):
                    self.registry.specs_by_key[("observe", t.robot_id, t.interface)] = t
                else:
                    ok = False
            if t.direction in ("command", "both") and t.access_enabled and t.msg_type:
                if getattr(t, "confirm", "auto") == "on_first_use" or self.adapter.bind_command(t):
                    self.registry.specs_by_key[("command", t.robot_id, t.interface)] = t
                else:
                    ok = False
        for s in self.registry.rc.services:
            if s.srv_type and s.access_enabled:
                if self.adapter.bind_service(s):
                    self.registry.specs_by_key[("service", s.robot_id, s.interface)] = s
                else:
                    ok = False
        for a in self.registry.rc.actions:
            if a.action_type and a.access_enabled:
                if self.adapter.bind_action(a):
                    self.registry.specs_by_key[("action", a.robot_id, a.interface)] = a
                else:
                    ok = False
        if not ok:
            # generation 활성화 전이므로 새 endpoint만 제거하면 기존 route에는
            # 영향이 없다. 초기 기동에서는 이 generation 전체를 폐기한다.
            self.adapter.shutdown()
            self.registry.specs_by_key.clear()
        return ok

    def handle_event(self, ev: InboundEvent) -> None:
        spec = getattr(ev, "spec", None)
        if ev.kind == "_activate_plan" and isinstance(spec, PendingBindingPlan):
            try:
                self._activate_plan(spec)
            except Exception:
                self.finish_staging()
                raise
            return
        if ev.kind == "_bind_service" and isinstance(spec, ServiceSpec):
            if spec.access_enabled and self.adapter.bind_service(spec):
                key = ("service", spec.robot_id, spec.interface)
                self.registry.specs_by_key[key] = spec
                self.inbound.publish_contract(key, spec)
            return
        if ev.kind == "_bind_action" and isinstance(spec, ActionSpec):
            if spec.access_enabled and self.adapter.bind_action(spec):
                key = ("action", spec.robot_id, spec.interface)
                self.registry.specs_by_key[key] = spec
                self.inbound.publish_contract(key, spec)
            return
        if isinstance(spec, TopicSpec):
            if spec.direction in ("observe", "both") and self.adapter.bind_observe(spec):
                self.registry.specs_by_key[("observe", spec.robot_id, spec.interface)] = spec
                # Pipeline 스펙 사전은 기동 시점 스냅숏 — 늦게 합류한 토픽을
                # 등록하지 않으면 관측 IR이 조용히 버려진다
                self.outbound.add_topic(spec)
            if (
                spec.direction in ("command", "both")
                and spec.access_enabled
                and spec.confirm == "on_first_use"
            ) or (
                spec.direction in ("command", "both")
                and spec.access_enabled
                and self.adapter.bind_command(spec)
            ):
                key = ("command", spec.robot_id, spec.interface)
                self.registry.specs_by_key[key] = spec
                self.inbound.publish_contract(key, spec)
            self.status.publish_qos(only_key=(spec.robot_id, spec.interface))

    def _activate_plan(self, pending: PendingBindingPlan) -> None:
        """S10 make-before-break: bind additions, swap generation, remove old."""
        current_generation = self.lifecycle.snapshot.generation
        if pending.base_generation is not None and pending.base_generation != current_generation:
            self.outbound.emit_event(
                "provisioningStatus",
                "warning",
                {
                    "event": "bindingPlanSuperseded",
                    "baseGeneration": pending.base_generation,
                    "activeGeneration": current_generation,
                },
            )
            self.finish_staging()
            return
        staged: list[tuple[tuple[str, str, str], Any]] = []
        self.lifecycle.set(IPEState.RUNNING, IPEPhase.BINDING)
        for key, spec in pending.additions:
            if not self._bind_plan_spec(key, spec):
                for staged_key, staged_spec in reversed(staged):
                    self._unbind_plan_spec(staged_key, staged_spec)
                self.lifecycle.set(
                    IPEState.RUNNING,
                    IPEPhase.IDLE,
                    health=IPEHealth.DEGRADED,
                    detail=f"binding rollback: {key}",
                )
                self.outbound.emit_event(
                    "provisioningStatus", "error", {"event": "bindingRollback", "binding": str(key)}
                )
                self.finish_staging()
                return
            staged.append((key, spec))

        # route/path/ResolvedConfig은 하나의 executor callback에서 세대 교체된다.
        self.registry.rc = pending.rc
        self.provisioner.rc = pending.rc
        self.absorb_provision(pending.provision)
        for key, spec in pending.removals:
            self._unbind_plan_spec(key, spec)
            self.inbound.terminate_inflight(spec.robot_id, spec.interface)

        self.lifecycle.set(
            IPEState.RUNNING, IPEPhase.IDLE, health=IPEHealth.HEALTHY, next_generation=True
        )
        for key, spec in pending.additions:
            runtime_kind = (
                "command" if key[0] == "topic" and spec.direction in ("command", "both") else key[0]
            )
            self.inbound.publish_contract((runtime_kind, spec.robot_id, spec.interface), spec)
        self.status.publish_qos()
        if pending.removals:
            self._jobs.put(("remove_interfaces", pending.removals))
        self._jobs.put(("catchup", "binding-generation"))
        self.outbound.emit_event(
            "provisioningStatus",
            "info",
            {
                "event": "bindingGenerationActivated",
                "generation": self.lifecycle.snapshot.generation,
                "added": len(pending.additions),
                "removed": len(pending.removals),
            },
        )

    def _bind_plan_spec(self, key: tuple[str, str, str], spec: Any) -> bool:
        kind = key[0]
        endpoint_key = (spec.robot_id, spec.interface)
        if kind == "service":
            if not spec.access_enabled:
                return True
            if not self.adapter.bind_service(spec):
                return False
            self.registry.specs_by_key[("service", *endpoint_key)] = spec
            return True
        if kind == "action":
            if not spec.access_enabled:
                return True
            if not self.adapter.bind_action(spec):
                return False
            self.registry.specs_by_key[("action", *endpoint_key)] = spec
            return True

        bound_observe = False
        if spec.direction in ("observe", "both"):
            if not self.adapter.bind_observe(spec):
                return False
            bound_observe = True
            self.registry.specs_by_key[("observe", *endpoint_key)] = spec
            self.outbound.add_topic(spec)
        if spec.direction in ("command", "both") and spec.access_enabled:
            if spec.confirm == "on_first_use":
                self.registry.specs_by_key[("command", *endpoint_key)] = spec
            elif not self.adapter.bind_command(spec):
                if bound_observe:
                    self.adapter.unbind_observe(endpoint_key)
                    self.registry.specs_by_key.pop(("observe", *endpoint_key), None)
                    self.outbound.remove_topic(*endpoint_key)
                return False
            else:
                self.registry.specs_by_key[("command", *endpoint_key)] = spec
        return True

    def _unbind_plan_spec(self, key: tuple[str, str, str], spec: Any) -> None:
        kind = key[0]
        endpoint_key = (spec.robot_id, spec.interface)
        if kind == "service":
            self.adapter.unbind_service(endpoint_key)
            self.registry.specs_by_key.pop(("service", *endpoint_key), None)
        elif kind == "action":
            self.adapter.unbind_action(endpoint_key)
            self.registry.specs_by_key.pop(("action", *endpoint_key), None)
        else:
            self.adapter.unbind_observe(endpoint_key)
            self.adapter.unbind_command(endpoint_key)
            self.registry.specs_by_key.pop(("observe", *endpoint_key), None)
            self.registry.specs_by_key.pop(("command", *endpoint_key), None)
            self.outbound.remove_topic(*endpoint_key)

    def _prov_worker(self) -> None:
        while not self._stop.is_set():
            try:
                job, arg = self._jobs.get(timeout=1.0)
            except queue.Empty:
                continue
            if self._stop.is_set():
                break
            try:
                if job == "reconcile":
                    restarted = self.provisioner.check_cse_identity()
                    self.absorb_provision(self.provision_staged())
                    # 재프로비저닝 후 캐시 1회 무효화(§4.8.4) — 재생성된 qos
                    # FCNT가 CREATE 초기 속성만 든 채 캐시에 가려지는 것을 막는다.
                    # 게시는 executor 틱이 수행한다(스레드 소유권).
                    self.status.request_republish()
                    if restarted:
                        self.inbound.catchup.sweep("cse-restart")
                elif job == "catchup":
                    self.inbound.catchup.sweep(str(arg or "manual"))
                elif job == "recover":
                    restarted = self.provisioner.check_cse_identity()
                    self.absorb_provision(self.provision_staged())
                    self.status.request_republish()
                    self.inbound.catchup.sweep("cse-restart" if restarted else "cse-recovered")
                elif job == "reconcile_discovery":
                    self._reconcile_discovery(arg)
                elif job == "remove_interfaces":
                    self._remove_interfaces(arg)
            except Exception:
                self.finish_staging()
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
            if len(removed) < self._removal_path_count(key, spec):
                self._resource_removal_pending[key] = spec
            else:
                self._resource_removal_pending.pop(key, None)

    def _reconcile_discovery(self, snap: dict[str, Any]) -> None:
        from ipe.runtime.planning import resolve

        if self._resource_removal_pending:
            self._remove_interfaces(list(self._resource_removal_pending.items()))
        try:
            new_rc = resolve(self.registry.rc.raw, discovered=snap)
        except Exception as e:
            log.warning("discovery re-resolve failed: %s", e)
            return
        self.inbound.apply_saved_control_approvals(new_rc)
        self.inbound.apply_saved_qos_overrides(new_rc)
        self.defer_unloadable_types(new_rc)
        self._churn_track(snap)

        active = binding_map(self.registry.rc)
        desired = binding_map(new_rc)
        grace = int(self.registry.rc.discovery.get("vanish_grace_polls", 2) or 2)
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
            result = self.provision_staged()
        finally:
            self.provisioner.rc = previous
        if not result.ok:
            self.finish_staging()
            log.warning("staged discovery provisioning failed: %s", result.errors)
            return

        pending = PendingBindingPlan(
            new_rc,
            result,
            additions,
            removals,
            base_generation=self.lifecycle.snapshot.generation,
        )
        ev = InboundEvent(
            kind="_activate_plan",
            robot_id="-",
            interface="-",
            correlation_id=None,
            event_id=f"plan:{time.time_ns()}",
            payload=None,
            ct=None,
            spec=pending,
        )
        if not self.inbound.enqueue_internal(ev):
            self.finish_staging()
            log.error("control lane full: staged binding plan was not activated")
            return

    def refresh(self) -> None:
        try:
            changed = self.adapter.rebind_changed()  # offered/requested 재조정 (§8.2)
            snap = self.adapter.snapshot()
        except Exception as e:
            log.warning("discovery snapshot failed: %s", e)
            return
        if changed:
            self.status.publish_qos()
        self._jobs.put(("reconcile_discovery", snap))

    def _churn_track(self, snap: dict[str, Any]) -> None:
        names = (
            {n for n, _ in snap.get("topics", [])}
            | {n for n, _ in snap.get("services", [])}
            | {n for n, _ in snap.get("actions", [])}
        )
        grace = int(self.registry.rc.discovery.get("vanish_grace_polls", 2) or 2)
        for key in list(self.registry.specs_by_key.keys()):
            kind, robot, iface = key
            st = self._avail.setdefault(key, {"state": "present", "miss": 0})
            if iface in names:
                if st["state"] != "present":
                    st.update(state="present", miss=0)
                    self._mark_avail(robot, iface, True)
                    self.outbound.emit_event(
                        "nodeStatus",
                        "info",
                        {"event": "rejoined", "interface": iface, "robot": robot},
                    )
                else:
                    st["miss"] = 0
                continue
            st["miss"] += 1
            if st["miss"] < grace:
                if st["state"] == "present":
                    st["state"] = "suspect"
                    self.outbound.emit_event(
                        "nodeStatus",
                        "warning",
                        {"event": "suspect", "interface": iface, "robot": robot},
                    )
            elif st["state"] != "vanished":
                st["state"] = "vanished"
                self._mark_avail(robot, iface, False)
                self.outbound.emit_event(
                    "nodeStatus",
                    "warning",
                    {"event": "vanished", "interface": iface, "robot": robot},
                )
                self.inbound.terminate_inflight(robot, iface)

    def _mark_avail(self, robot: str, iface: str, available: bool) -> None:
        path = None
        for view in ("history", "latest", "fcnt", "command", "request", "response", "result"):
            path = self.registry.path_map.get((robot, iface, view))
            if path:
                break
        if path is None:
            return
        try:
            self.prov_ops.update_lbl(
                path,
                [
                    f"ipe:available={'true' if available else 'false'}",
                    f"ipe:lastSeen={time.time():.0f}",
                ],
            )
        except Exception as e:
            log.warning("availability lbl update failed for %s: %s", path, e)
