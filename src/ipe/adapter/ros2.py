"""범용(타입 무관) ROS2 어댑터 (DESIGN §3, §8, §10-12).

모든 메서드는 반드시 executor 스레드에서 호출해야 한다. 이 모듈은 절대
블로킹하지 않고 CSE I/O도 하지 않는다 — 콜백 결과는 enqueue 전용 훅으로 넘긴다.

Humble 주의점:
- 이벤트 콜백 모듈은 Humble=`rclpy.qos_event`, Iron+/Jazzy=`rclpy.event_handler` (_event_callbacks가 폴백).
- Humble rclpy는 구독 콜백에 메시지별 publisher GID를 노출하지 않아,
  `direction: both`의 자기 에코 억제를 GID 비교 대신
  페이로드 해시 + 시간 창 매칭으로 구현했다.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from ipe.adapter.messages import TranscodeError, extract_source_ts, from_canonical, parse_message
from ipe.adapter.qos import build_qos_profile, check_compatible
from ipe.ir import TopicIR
from ipe.models import ACTION_QOS_CHANNELS, ActionSpec, QoSSpec, ServiceSpec, TopicSpec
from ipe.qos import engine as qosmod
from ipe.qos.codec import endpoint_to_peer
from ipe.qos.configuration import command_qos_violation

log = logging.getLogger(__name__)

EventHook = Callable[[str, str, dict[str, Any]], None]   # (category, severity, payload)


def _load(kind: str, type_str: str) -> Any:
    if kind == "msg":
        from rosidl_runtime_py.utilities import get_message
        return get_message(type_str)
    if kind == "srv":
        from rosidl_runtime_py.utilities import get_service
        return get_service(type_str)
    from rosidl_runtime_py.utilities import get_action
    return get_action(type_str)


def _event_callbacks(kind: str) -> Any:
    # 콜백 클래스가 Iron에서 rclpy.qos_event -> rclpy.event_handler로 개명됨 (Humble 폴백).
    try:
        from rclpy import event_handler as mod
    except ImportError:
        from rclpy import qos_event as mod
    return getattr(mod, "SubscriptionEventCallbacks" if kind == "sub"
                   else "PublisherEventCallbacks")


def _supported_axes(cb_cls: Any, axes: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """콜백 클래스 시그니처에 없는 축 제거 — Humble에는 matched/incompatible_type
    kwarg 자체가 없어 생성자가 TypeError를 낸다(§4.6.1의 배포판 격차)."""
    import inspect
    params = inspect.signature(cb_cls.__init__).parameters
    return ({k: v for k, v in axes.items() if k in params},
            [k for k in axes if k not in params])


def _qos_event_fields(info: Any) -> dict[str, Any]:
    """incompatible 계열 상태 구조체 → 구조화 필드(policy/total_count, §4.6.1)."""
    out: dict[str, Any] = {"info": str(info)}
    tc = getattr(info, "total_count", None)
    if tc is not None:
        out["total_count"] = tc
    kind = getattr(info, "last_policy_kind", None)
    if kind is not None:
        try:
            from rclpy.qos import qos_policy_name_from_kind
            out["policy"] = qos_policy_name_from_kind(kind)
        except Exception:
            out["policy"] = str(kind)
    return out


# 액션 클라이언트 kwarg 이름은 채널 정본(spec.ACTION_QOS_CHANNELS)에서 파생 —
# 채널 추가 시 spec 한 곳만 고친다. 미지 채널은 기존처럼 KeyError로 드러난다.
_ACTION_CHAN_KWARG = {ch: f"{ch}_qos_profile" for ch in ACTION_QOS_CHANNELS}


@dataclass
class _ObserveState:
    spec: TopicSpec
    subscription: Any
    applied_qos: Any = None              # reconcile+guard 결과 — rebind 비교 기준
    seq: int = 0
    last_arrival_mono: float | None = None
    stale_flagged: bool = False
    offered_peers: list[dict[str, Any]] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


@dataclass
class _CommandState:
    spec: TopicSpec
    publisher: Any
    msg_class: Any
    applied_qos: Any = None
    last_publish_mono: float | None = None
    watchdog_fired: bool = False
    recent_hashes: list[tuple[str, float]] = field(default_factory=list)
    requested_peers: list[dict[str, Any]] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


@dataclass
class _ActionState:
    spec: ActionSpec
    client: Any
    action_class: Any
    handles: dict[str, Any] = field(default_factory=dict)        # 우리 goalId -> goal handle
    sent: set[str] = field(default_factory=set)                  # 전송됐지만 handle 대기 중인 goalId
    pending_cancel: set[str] = field(default_factory=set)        # handle보다 cancel이 먼저 온 goalId


class GenericROS2Adapter:
    def __init__(self, node: Any, on_topic_ir: Callable[[TopicIR], None], on_event: EventHook,
                 qos_strictness: str = "reject", *, self_echo_window_sec: float = 0.5,
                 qos_event_coalesce_sec: float = 5.0) -> None:
        self.node = node
        self.on_topic_ir = on_topic_ir
        self.on_event = on_event
        self.qos_strictness = qos_strictness
        self.self_echo_window_sec = self_echo_window_sec
        self.qos_event_coalesce_sec = qos_event_coalesce_sec
        self.observes: dict[tuple[str, str], _ObserveState] = {}
        self.commands: dict[tuple[str, str], _CommandState] = {}
        self.services: dict[tuple[str, str], dict[str, Any]] = {}
        self.actions: dict[tuple[str, str], _ActionState] = {}
        self._reentrant = None
        self._qos_event_last: dict[tuple[str, str], float] = {}
        self._qos_dirty: set[tuple[str, str]] = set()
        self._qos_dirty_last: dict[tuple[str, str], float] = {}
        self._unsupported_event_axes_reported: set[tuple[str, str]] = set()

    def _remote_endpoint_infos(self, infos: list[Any]) -> list[Any]:
        """Exclude this IPE node's endpoints from peer QoS reconciliation."""
        try:
            own = (self.node.get_name(), self.node.get_namespace())
        except Exception:
            return list(infos)
        return [info for info in infos if (
            getattr(info, "node_name", None), getattr(info, "node_namespace", None)
        ) != own]

    @staticmethod
    def type_available(kind: str, type_str: str | None) -> bool:
        if not type_str:
            return False
        try:
            _load(kind, type_str)
            return True
        except Exception:
            return False

    def _report_unsupported_event_axis(self, endpoint: str, axis: str) -> None:
        """배포판 기능 차이는 장애가 아니므로 축별 한 번만 INFO로 기록한다."""
        key = (endpoint, axis)
        if key in self._unsupported_event_axes_reported:
            return
        self._unsupported_event_axes_reported.add(key)
        log.info(
            "ROS 2 %s QoS event axis '%s' is unavailable in this rclpy; disabled",
            endpoint,
            axis,
        )

    def _load_or_report(self, kind: str, type_str: str,
                        spec: TopicSpec | ServiceSpec | ActionSpec) -> Any | None:
        """타입 로드 — 실패는 typeLoadError 이벤트로 보고하고 None (bind_* 공용)."""
        try:
            return _load(kind, type_str)
        except Exception as e:
            self._event("provisioningStatus", "error",
                        {"event": "typeLoadError", "interface": spec.interface,
                         "robot": spec.robot_id, "type": type_str, "error": str(e)})
            return None

    # ------------------------------------------------------------------
    # 디스커버리 (폴링 스냅숏)
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """애플리케이션 endpoint를 만들기 전의 ROS graph snapshot.

        topic 방향은 원격 Publisher/Subscription endpoint 수로 계산한다. IPE
        자신의 rosout/parameter endpoint는 제외해 graph가 자기 자신 때문에
        준비 완료로 오판되지 않게 한다.
        """
        from rclpy.action import get_action_names_and_types
        try:
            from rclpy.action import get_action_server_names_and_types_by_node
        except ImportError:  # 구형 rclpy는 전역 action 목록만 제공한다.
            get_action_server_names_and_types_by_node = None

        own_name = self.node.get_name()
        own_ns = self.node.get_namespace()

        def remote(info: Any) -> bool:
            return not (getattr(info, "node_name", None) == own_name
                        and getattr(info, "node_namespace", None) == own_ns)

        def usable_namespace(value: Any) -> bool:
            namespace = str(value or "")
            # ROS graph API의 정상 node namespace는 항상 절대 이름('/...')이다.
            # Fast DDS가 bare DDS participant에 사용하는
            # ``_CREATED_BY_BARE_DDS_APP_`` 같은 placeholder를 Robot 경계로
            # 오인하지 않는다.
            return bool(namespace.startswith("/") and namespace != "/")

        try:
            remote_nodes = [
                (node_name, node_ns)
                for node_name, node_ns in self.node.get_node_names_and_namespaces()
                if not (node_name == own_name and node_ns == own_ns)
            ]
        except Exception:
            remote_nodes = []
        graph_namespaces = sorted({node_ns for _node_name, node_ns in remote_nodes
                                   if usable_namespace(node_ns)})
        has_root_nodes = any(node_ns == "/" for _node_name, node_ns in remote_nodes)
        graph_namespace = (
            graph_namespaces[0]
            if len(graph_namespaces) == 1 and not has_root_nodes
            else None
        )

        topics: list[tuple[str, list[str]]] = []
        topic_directions: dict[str, str] = {}
        topic_owners: dict[str, list[str]] = {}
        for name, types in self.node.get_topic_names_and_types():
            if "/_action/" in name:
                continue
            try:
                publishers = [x for x in self.node.get_publishers_info_by_topic(name)
                              if remote(x)]
                subscriptions = [x for x in self.node.get_subscriptions_info_by_topic(name)
                                 if remote(x)]
            except Exception:
                publishers, subscriptions = [], []
            if not publishers and not subscriptions:
                continue
            topic_directions[name] = (
                "both" if publishers and subscriptions
                else "observe" if publishers else "command"
            )
            namespaces = {getattr(x, "node_namespace", "")
                          for x in (*publishers, *subscriptions)}
            owners = sorted(x for x in namespaces if usable_namespace(x))
            # Use a graph-wide fallback only when every remote node is namespaced.
            if not owners and graph_namespace:
                owners = [graph_namespace]
            topic_owners[name] = owners
            topics.append((name, list(types)))

        services = self.node.get_service_names_and_types()
        try:
            own_services = {
                name for name, _types in
                self.node.get_service_names_and_types_by_node(own_name, own_ns)
            }
        except Exception:
            own_services = set()
        # 같은 이름을 원격 node도 제공하는 드문 경우를 보존한다.
        remote_services: set[str] = set()
        service_owners: dict[str, set[str]] = {}
        try:
            for node_name, node_ns in remote_nodes:
                try:
                    for name, _types in self.node.get_service_names_and_types_by_node(
                            node_name, node_ns):
                        remote_services.add(name)
                        service_owners.setdefault(name, set()).add(node_ns)
                except Exception:
                    continue
        except Exception:
            pass
        services = [(name, types) for name, types in services
                    if name not in own_services or name in remote_services]
        try:
            actions = get_action_names_and_types(self.node)
        except Exception:
            actions = []
        action_owners: dict[str, set[str]] = {}
        if get_action_server_names_and_types_by_node is not None:
            remote_actions: set[str] = set()
            try:
                for node_name, node_ns in remote_nodes:
                    try:
                        for name, _types in get_action_server_names_and_types_by_node(
                                self.node, node_name, node_ns):
                            remote_actions.add(name)
                            action_owners.setdefault(name, set()).add(node_ns)
                    except Exception:
                        continue
                actions = [(name, types) for name, types in actions
                           if name in remote_actions]
            except Exception:
                pass
        # 액션 내부 엔티티(/_action/)는 service에서도 숨긴다.
        services = [(n, t) for n, t in services if "/_action/" not in n]
        return {
            "topics": topics,
            "services": [(n, list(t)) for n, t in services],
            "actions": [(n, list(t)) for n, t in actions],
            "topic_directions": topic_directions,
            "owners": {
                "topics": topic_owners,
                "services": {k: sorted(x for x in v if usable_namespace(x))
                             for k, v in service_owners.items()},
                "actions": {k: sorted(x for x in v if usable_namespace(x))
                            for k, v in action_owners.items()},
            },
        }

    # ------------------------------------------------------------------
    # observe (QoS reconcile + 이벤트 + 자기 에코 억제)
    # ------------------------------------------------------------------

    def bind_observe(self, spec: TopicSpec) -> bool:
        key = (spec.robot_id, spec.interface)
        if key in self.observes:
            return True
        if not spec.msg_type:
            return False
        msg_class = self._load_or_report("msg", spec.msg_type, spec)
        if msg_class is None:
            return False

        infos = self._remote_endpoint_infos(
            self.node.get_publishers_info_by_topic(spec.interface)
        )
        offered = [i.qos_profile for i in infos]
        resolved, events = qosmod.reconcile_observe(offered, spec.qos_for("observe"),
                                                    has_explicit=spec.qos_explicit,
                                                    explicit_fields=spec.qos_explicit_fields)
        # observe에서 offered보다 엄격한 deadline/lease/liveliness는 매칭을 0으로 만든다
        guarded, strict_events = qosmod.strictness_guard(resolved, offered,
                                                         self.qos_strictness)
        for ev in strict_events:
            self._event("qosStatus", "warning",
                        {"event": ev, "interface": spec.interface, "robot": spec.robot_id})
        if strict_events and self.qos_strictness == "reject":
            self._event("provisioningStatus", "error",
                        {"event": "qosStrictnessViolation", "interface": spec.interface,
                         "robot": spec.robot_id, "violations": strict_events})
            return False
        resolved = guarded
        for ev in events:
            severity = "info" if ev == "noPublisherFallback" else "warning"
            self._event("qosStatus", severity,
                        {"event": ev, "interface": spec.interface, "robot": spec.robot_id})
        profile = build_qos_profile(resolved)

        def callback(msg: Any, _key: tuple[str, str] = key) -> None:
            self._on_observe_msg(_key, msg)

        sub = self._create_subscription_degrading(msg_class, spec, profile, callback)
        if sub is None:
            return False
        self.observes[key] = _ObserveState(
            spec=spec, subscription=sub, applied_qos=resolved,
            offered_peers=[endpoint_to_peer(i, "pub") for i in infos],
            events=list(dict.fromkeys([*events, *strict_events])))
        log.info("observe bound: %s [%s] (%s)", spec.interface, spec.msg_type, spec.robot_id)
        return True

    def rebind_changed(self) -> int:
        """디스커버리 변화 재조정 — observe·command 전 키 refresh (§8.2, 설계서 결정 #9).
        executor 스레드 전용. 반환값은 상태(applied/peers/events)가 변한 키 수."""
        changed = 0
        for key in list(self.observes.keys() | self.commands.keys()):
            if self.refresh_qos(key):
                changed += 1
        return changed

    def refresh_qos(self, key: tuple[str, str]) -> bool:
        """한 키의 offered/requested 재조회 → peers 갱신 + 실효값 변화 시 재바인딩.
        True = 게시할 변화가 있음."""
        changed = False
        if key in self.observes:
            changed = self._refresh_observe(key)
        if key in self.commands:
            changed = self._refresh_command(key) or changed
        return changed

    def _refresh_observe(self, key: tuple[str, str]) -> bool:
        st = self.observes[key]
        infos = self._remote_endpoint_infos(
            self.node.get_publishers_info_by_topic(st.spec.interface)
        )
        peers = [endpoint_to_peer(i, "pub") for i in infos]
        changed = peers != st.offered_peers
        st.offered_peers = peers
        if not infos:
            return changed   # publisher 부재는 변화가 아니다 — fallback 유지
        offered = [i.qos_profile for i in infos]
        new_spec, events = qosmod.reconcile_observe(offered, st.spec.qos_for("observe"),
                                                    has_explicit=st.spec.qos_explicit,
                                                    explicit_fields=st.spec.qos_explicit_fields)
        # applied_qos는 가드 적용 후 값 — 같은 기준으로 비교해야 demote 강등
        # 토픽이 offered 불변인데도 폴마다 rebind를 반복하지 않는다
        guarded_new, strict_events = qosmod.strictness_guard(new_spec, offered,
                                                             self.qos_strictness)
        if guarded_new == st.applied_qos:
            ev_all = list(dict.fromkeys([*events, *strict_events]))
            if ev_all != st.events:
                st.events = ev_all
                changed = True
            return changed
        seq = st.seq
        self.unbind_observe(key)
        if self.bind_observe(st.spec):
            self.observes[key].seq = seq
            self._event("qosStatus", "warning",
                        {"event": "qosRebind", "interface": st.spec.interface,
                         "robot": st.spec.robot_id})
        return True

    def _refresh_command(self, key: tuple[str, str]) -> bool:
        st = self.commands[key]
        infos = self._remote_endpoint_infos(
            self.node.get_subscriptions_info_by_topic(st.spec.interface)
        )
        peers = [endpoint_to_peer(i, "sub") for i in infos]
        changed = peers != st.requested_peers
        st.requested_peers = peers
        if not infos:
            return changed   # subscriber 부재 — fallback 유지
        new_spec, events = qosmod.reconcile_command(
            [i.qos_profile for i in infos], st.spec.qos_for("command"))
        violation = command_qos_violation(new_spec.liveliness, new_spec.deadline_ms)
        if violation:
            marker = f"unsupportedRequested{violation.title()}"
            ev_all = list(dict.fromkeys([*events, marker]))
            if ev_all != st.events:
                st.events = ev_all
                self._event(
                    "commandStatus", "error",
                    {"event": "unsupportedRequestedQoS", "interface": st.spec.interface,
                     "robot": st.spec.robot_id, "policy": violation},
                )
                changed = True
            return changed
        if new_spec == st.applied_qos:
            ev_all = list(dict.fromkeys(events))
            if ev_all != st.events:
                st.events = ev_all
                changed = True
            return changed
        self.unbind_command(key)
        if self.bind_command(st.spec):
            self._event("qosStatus", "warning",
                        {"event": "qosRebind", "interface": st.spec.interface,
                         "robot": st.spec.robot_id, "direction": "command"})
        return True

    def check_candidate(self, key: tuple[str, str], candidate: Any,
                        direction: str) -> tuple[bool, list[str]]:
        """qos_update 후보의 예측 판정(§4.5.3-3) — reconcile+guard 재실행 후
        check_compatible. (False, 이유)=거부, (True, 이유)=수락(+경고)."""
        iface = key[1]
        if direction == "observe":
            offered = [i.qos_profile for i in self._remote_endpoint_infos(
                self.node.get_publishers_info_by_topic(iface)
            )]
            resolved, _ = qosmod.reconcile_observe(offered, candidate,
                                                   has_explicit=True,
                                                   explicit_fields=frozenset(
                                                       QoSSpec.__dataclass_fields__
                                                   ) - {"profile"})
            guarded, strict = qosmod.strictness_guard(resolved, offered,
                                                      self.qos_strictness)
            if strict and self.qos_strictness == "reject":
                return False, strict
            profile = build_qos_profile(guarded)
            pairs = [(o, profile) for o in offered]
        else:
            requested = [i.qos_profile for i in self._remote_endpoint_infos(
                self.node.get_subscriptions_info_by_topic(iface)
            )]
            resolved, _ = qosmod.reconcile_command(requested, candidate)
            violation = command_qos_violation(resolved.liveliness, resolved.deadline_ms)
            if violation:
                return False, [f"unsupported command QoS requested: {violation}"]
            profile = build_qos_profile(resolved)
            pairs = [(profile, r) for r in requested]
        warnings: list[str] = []
        for pub_q, sub_q in pairs:
            ok, reasons = check_compatible(pub_q, sub_q)
            if not ok:
                return False, reasons
            warnings.extend(reasons)
        return True, list(dict.fromkeys(warnings))

    def rebind_direction(self, key: tuple[str, str], new_qos: Any,
                         direction: str) -> bool:
        """Rebind only the endpoint direction whose configured QoS changed."""
        if direction == "observe":
            ost = self.observes.get(key)
            if ost is None:
                return False
            old_qos = ost.spec.qos_for("observe")
            ost.spec.set_qos_for("observe", new_qos)
            seq = ost.seq
            self.unbind_observe(key)
            if self.bind_observe(ost.spec):
                self.observes[key].seq = seq
                return True
            ost.spec.set_qos_for("observe", old_qos)
            if self.bind_observe(ost.spec):
                self.observes[key].seq = seq
            return False
        cst = self.commands.get(key)
        if cst is None:
            return False
        old_qos = cst.spec.qos_for("command")
        cst.spec.set_qos_for("command", new_qos)
        self.unbind_command(key)
        if self.bind_command(cst.spec):
            return True
        cst.spec.set_qos_for("command", old_qos)
        self.bind_command(cst.spec)
        return False


    def rebind_interface(self, key: tuple[str, str], new_qos: Any) -> bool:
        """Rebind every existing direction for legacy callers."""
        directions = [name for name, states in (
            ("observe", self.observes), ("command", self.commands)
        ) if key in states]
        return bool(directions) and all(
            self.rebind_direction(key, new_qos, direction) for direction in directions
        )

    def qos_states(self) -> list[dict[str, Any]]:
        """게시 입력 QoSStateIR 스냅숏 — 방향별 1건 (direction=both는 2건)."""
        out: list[dict[str, Any]] = []
        for (robot, iface), observe_state in self.observes.items():
            out.append({"robot_id": robot, "interface": iface, "direction": "observe",
                        "configured": observe_state.spec.qos_for("observe"),
                        "applied": observe_state.applied_qos,
                        "peers": list(observe_state.offered_peers),
                        "events": list(observe_state.events)})
        for (robot, iface), command_state in self.commands.items():
            out.append({"robot_id": robot, "interface": iface, "direction": "command",
                        "configured": command_state.spec.qos_for("command"),
                        "applied": command_state.applied_qos,
                        "peers": list(command_state.requested_peers),
                        "events": list(command_state.events)})
        return out

    def pop_qos_dirty(self) -> set[tuple[str, str]]:
        """matched 콜백이 표시한 재조정 대상 키를 소비한다(Iron+, §4.6.1)."""
        dirty, self._qos_dirty = self._qos_dirty, set()
        return dirty

    def _mark_qos_dirty(self, key: tuple[str, str]) -> None:
        now = time.monotonic()
        last = self._qos_dirty_last.get(key)
        if last is not None and now - last < self.qos_event_coalesce_sec:
            return
        self._qos_dirty_last[key] = now
        self._qos_dirty.add(key)

    def _create_subscription_degrading(self, msg_class: Any, spec: TopicSpec,
                                       profile: Any, callback: Any) -> Any:
        """QoS 이벤트 콜백 등록 — 미지원 축은 하나씩 빼며 재시도(점진 강등)."""
        subscription_callbacks_cls = _event_callbacks("sub")

        key = (spec.robot_id, spec.interface)

        def _mk(name: str, severity: str = "warning") -> Callable[[Any], None]:
            def cb(info: Any) -> None:
                self._event("qosStatus", severity,
                            {"event": name, "interface": spec.interface,
                             "robot": spec.robot_id, **_qos_event_fields(info)})
            return cb

        axes = {"deadline": _mk("deadlineMissed"),
                "liveliness": _mk("livelinessChanged", "info"),
                "incompatible_qos": _mk("qosMismatch"),
                "message_lost": _mk("messageLost"),
                "incompatible_type": _mk("incompatibleType"),
                # matched(Iron+)는 CIN을 내지 않는 내부 재조정 트리거(§4.6.1)
                "matched": lambda _info: self._mark_qos_dirty(key)}
        axes, unsupported = _supported_axes(subscription_callbacks_cls, axes)
        for axis in unsupported:
            self._report_unsupported_event_axis("subscription", axis)
        while True:
            try:
                return self.node.create_subscription(
                    msg_class, spec.interface, callback, profile,
                    event_callbacks=subscription_callbacks_cls(**axes))
            except Exception as e:
                name = type(e).__name__
                if "UnsupportedEventType" in name and axes:
                    dropped = next(iter(axes))
                    axes.pop(dropped)
                    self._report_unsupported_event_axis("subscription", dropped)
                    continue
                self._event("provisioningStatus", "error",
                            {"event": "subscribeFailed", "interface": spec.interface,
                             "robot": spec.robot_id, "error": str(e), "key": str(key)})
                return None

    def _on_observe_msg(self, key: tuple[str, str], msg: Any) -> None:
        st = self.observes.get(key)
        if st is None:
            return
        try:
            now_mono = time.monotonic()
            st.last_arrival_mono = now_mono
            st.stale_flagged = False
            payload = parse_message(msg)

            # direction=both 자기 에코 억제 (모듈 docstring 참고)
            cmd = self.commands.get(key)
            if cmd is not None and self._is_self_echo(cmd, payload, now_mono):
                return

            sts = st.spec.source_ts
            source_ts = extract_source_ts(
                payload,
                sts.field if sts else None,
                sts.format if sts else None,
            )
            st.seq += 1
            ir = TopicIR(
                interface_type="topic",
                robot_id=st.spec.robot_id,
                interface_name=st.spec.interface,
                message_type=st.spec.msg_type or "",
                source_ts=source_ts,
                ingest_ts=time.time(),
                seq=st.seq,
                payload=payload,
                metadata={},
            )
            self.on_topic_ir(ir)
        except Exception as e:
            self._event("topicHealth", "error",
                        {"event": "observeError", "interface": st.spec.interface,
                         "robot": st.spec.robot_id, "error": str(e)})

    @staticmethod
    def _payload_hash(payload: dict[str, Any]) -> str:
        return hashlib.sha1(
            json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()

    def _is_self_echo(self, cmd: _CommandState, payload: dict[str, Any], now: float) -> bool:
        """자기 발행 1회당 정확히 1회만 억제(매칭 해시 소비) — 제3자의 동일
        페이로드가 연속 억제되는 경로를 구조적으로 차단한다(§10)."""
        h = self._payload_hash(payload)
        fresh = [(ph, ts) for ph, ts in cmd.recent_hashes
                 if now - ts < self.self_echo_window_sec]
        for i, (ph, _ts) in enumerate(fresh):
            if ph == h:
                del fresh[i]
                cmd.recent_hashes = fresh
                return True
        cmd.recent_hashes = fresh
        return False

    def unbind_observe(self, key: tuple[str, str]) -> None:
        st = self.observes.pop(key, None)
        if st is not None:
            self.node.destroy_subscription(st.subscription)

    def unbind_command(self, key: tuple[str, str]) -> None:
        st = self.commands.pop(key, None)
        if st is not None:
            self.node.destroy_publisher(st.publisher)

    # ------------------------------------------------------------------
    # command (strongest-requested QoS)
    # ------------------------------------------------------------------

    def bind_command(self, spec: TopicSpec) -> bool:
        key = (spec.robot_id, spec.interface)
        if key in self.commands:
            return True
        if not spec.msg_type:
            return False
        msg_class = self._load_or_report("msg", spec.msg_type, spec)
        if msg_class is None:
            return False

        infos = self._remote_endpoint_infos(
            self.node.get_subscriptions_info_by_topic(spec.interface)
        )
        requested = [i.qos_profile for i in infos]
        resolved, events = qosmod.reconcile_command(requested, spec.qos_for("command"))
        violation = command_qos_violation(resolved.liveliness, resolved.deadline_ms)
        if violation:
            self._event(
                "commandStatus", "error",
                {"event": "unsupportedRequestedQoS", "interface": spec.interface,
                 "robot": spec.robot_id, "policy": violation},
            )
            return False
        for ev in events:
            severity = "info" if ev == "noSubscriberFallback" else "warning"
            self._event("qosStatus", severity,
                        {"event": ev, "interface": spec.interface, "robot": spec.robot_id})
        profile = build_qos_profile(resolved)
        pub = self._create_publisher_degrading(msg_class, spec, profile, key)
        self.commands[key] = _CommandState(
            spec=spec, publisher=pub, msg_class=msg_class, applied_qos=resolved,
            requested_peers=[endpoint_to_peer(i, "sub") for i in infos],
            events=list(events))
        log.info("command bound: %s [%s] (%s)", spec.interface, spec.msg_type, spec.robot_id)
        return True

    def _create_publisher_degrading(self, msg_class: Any, spec: TopicSpec,
                                    profile: Any, key: tuple[str, str]) -> Any:
        """발행자 생성 — 구독과 동일한 이벤트 축 점진 강등(§4.6.1).
        LivelinessLost는 의도적으로 미등록: command liveliness는 AUTOMATIC
        고정이라 lost == IPE 프로세스 정지와 동치다."""
        publisher_callbacks_cls = _event_callbacks("pub")

        def _mk(category: str, severity: str, name: str) -> Callable[[Any], None]:
            def cb(info: Any) -> None:
                self._event(category, severity,
                            {"event": name, "interface": spec.interface,
                             "robot": spec.robot_id, **_qos_event_fields(info)})
            return cb

        axes = {"incompatible_qos": _mk("commandStatus", "error", "incompatibleQoS"),
                "deadline": _mk("qosStatus", "warning", "offeredDeadlineMissed"),
                "incompatible_type": _mk("qosStatus", "warning", "incompatibleType"),
                "matched": lambda _info: self._mark_qos_dirty(key)}
        axes, unsupported = _supported_axes(publisher_callbacks_cls, axes)
        for axis in unsupported:
            self._report_unsupported_event_axis("publisher", axis)
        while axes:
            try:
                return self.node.create_publisher(
                    msg_class, spec.interface, profile,
                    event_callbacks=publisher_callbacks_cls(**axes))
            except Exception as e:
                if "UnsupportedEventType" not in type(e).__name__:
                    break
                dropped = next(iter(axes))
                axes.pop(dropped)
                self._report_unsupported_event_axis("publisher", dropped)
        return self.node.create_publisher(msg_class, spec.interface, profile)

    def publish_command(self, spec: TopicSpec, canonical: dict[str, Any]) -> bool:
        key = (spec.robot_id, spec.interface)
        st = self.commands.get(key)
        if st is None:
            return False
        fields = from_canonical(canonical, st.msg_class)   # 잘못된 입력이면 TranscodeError
        msg = st.msg_class()
        from rosidl_runtime_py.set_message import set_message_fields
        set_message_fields(msg, fields)
        st.publisher.publish(msg)
        now = time.monotonic()
        st.last_publish_mono = now
        st.watchdog_fired = False
        if spec.direction == "both":
            st.recent_hashes.append((self._payload_hash(parse_message(msg)), now))
        return True

    def validate_command(self, spec: TopicSpec, canonical: dict[str, Any]) -> None:
        """Validate a command payload without creating or publishing a ROS message."""
        from rosidl_runtime_py.utilities import get_message

        if not spec.msg_type:
            raise TranscodeError("", "message type is unavailable")
        from_canonical(canonical, get_message(spec.msg_type))

    # ------------------------------------------------------------------
    # service
    # ------------------------------------------------------------------

    def _reentrant_group(self) -> Any:
        if self._reentrant is None:
            from rclpy.callback_groups import ReentrantCallbackGroup
            self._reentrant = ReentrantCallbackGroup()
        return self._reentrant

    def bind_service(self, spec: ServiceSpec) -> bool:
        key = (spec.robot_id, spec.interface)
        if key in self.services:
            return True
        if not spec.srv_type:
            return False
        srv_class = self._load_or_report("srv", spec.srv_type, spec)
        if srv_class is None:
            return False
        kwargs: dict[str, Any] = {"callback_group": self._reentrant_group()}
        if spec.qos is not None:
            kwargs["qos_profile"] = build_qos_profile(spec.qos)
        client = self.node.create_client(srv_class, spec.interface, **kwargs)
        self.services[key] = {"spec": spec, "client": client, "srv_class": srv_class}
        return True

    def unbind_service(self, key: tuple[str, str]) -> None:
        entry = self.services.pop(key, None)
        if entry is not None:
            self.node.destroy_client(entry["client"])

    def server_available(self, kind: str, key: tuple[str, str]) -> bool:
        if kind == "service":
            entry = self.services.get(key)
            return bool(entry and entry["client"].service_is_ready())
        if kind == "action":
            st = self.actions.get(key)
            return bool(st and st.client.server_is_ready())
        return False

    def call_service(
        self,
        spec: ServiceSpec,
        canonical_request: dict[str, Any],
        done_cb: Callable[[dict[str, Any] | None, str | None], None],
    ) -> bool:
        """done_cb(canonical_response, error)는 executor Task로 실행된다."""
        key = (spec.robot_id, spec.interface)
        entry = self.services.get(key)
        if entry is None:
            return False
        srv_class = entry["srv_class"]
        req = srv_class.Request()
        fields = from_canonical(canonical_request, srv_class.Request)
        from rosidl_runtime_py.set_message import set_message_fields
        set_message_fields(req, fields)
        fut = entry["client"].call_async(req)

        def _done(f: Any) -> None:
            try:
                resp = f.result()
                done_cb(parse_message(resp), None)
            except Exception as e:   # done 콜백은 절대 raise 금지 — 에러로 종료 전달
                done_cb(None, str(e))

        fut.add_done_callback(_done)
        return True

    # ------------------------------------------------------------------
    # action (pending-cancel 보류, 데드락 없는 콜백 체이닝)
    # ------------------------------------------------------------------

    def bind_action(self, spec: ActionSpec) -> bool:
        key = (spec.robot_id, spec.interface)
        if key in self.actions:
            return True
        if not spec.action_type:
            return False
        action_class = self._load_or_report("action", spec.action_type, spec)
        if action_class is None:
            return False
        from rclpy.action import ActionClient
        kwargs: dict[str, Any] = {"callback_group": self._reentrant_group()}
        for chan, q in (spec.qos or {}).items():
            kwargs[_ACTION_CHAN_KWARG[chan]] = build_qos_profile(q)
        client = ActionClient(self.node, action_class, spec.interface, **kwargs)
        self.actions[key] = _ActionState(spec=spec, client=client, action_class=action_class)
        return True

    def unbind_action(self, key: tuple[str, str]) -> None:
        state = self.actions.pop(key, None)
        if state is not None:
            state.client.destroy()

    def send_goal(
        self,
        spec: ActionSpec,
        goal_id: str,
        canonical_goal: dict[str, Any],
        on_goal_response: Callable[[str, bool], None],
        on_feedback: Callable[[str, dict[str, Any]], None],
        on_result: Callable[[str, int, dict[str, Any]], None],
    ) -> bool:
        key = (spec.robot_id, spec.interface)
        st = self.actions.get(key)
        if st is None:
            return False
        goal = st.action_class.Goal()
        fields = from_canonical(canonical_goal, st.action_class.Goal)
        from rosidl_runtime_py.set_message import set_message_fields
        set_message_fields(goal, fields)

        def _feedback(fb_msg: Any) -> None:
            try:
                on_feedback(goal_id, parse_message(fb_msg.feedback))
            except Exception as e:
                self._event("actionStatus", "error",
                            {"event": "feedbackError", "goalId": goal_id, "error": str(e)})

        st.sent.add(goal_id)
        fut = st.client.send_goal_async(goal, feedback_callback=_feedback)

        def _goal_response(f: Any) -> None:
            try:
                handle = f.result()
            except Exception as e:
                st.sent.discard(goal_id)
                on_goal_response(goal_id, False)
                self._event("actionStatus", "error",
                            {"event": "goalSendFailed", "goalId": goal_id, "error": str(e)})
                return
            st.sent.discard(goal_id)
            if not handle.accepted:
                on_goal_response(goal_id, False)
                return
            st.handles[goal_id] = handle
            on_goal_response(goal_id, True)
            # pending-cancel 보류: handle보다 cancel이 먼저 도착한 경우
            if goal_id in st.pending_cancel:
                st.pending_cancel.discard(goal_id)
                self._do_cancel(st, goal_id)
            rf = handle.get_result_async()

            def _result(rf2: Any) -> None:
                try:
                    wrapped = rf2.result()
                    st.handles.pop(goal_id, None)
                    on_result(goal_id, int(wrapped.status), parse_message(wrapped.result))
                except Exception as e:
                    st.handles.pop(goal_id, None)
                    on_result(goal_id, 0, {"error": str(e)})

            rf.add_done_callback(_result)

        fut.add_done_callback(_goal_response)
        return True

    def cancel_goal(self, spec: ActionSpec, goal_id: str) -> str:
        """'sent' | 'pending' | 'unknown' 중 하나를 반환."""
        key = (spec.robot_id, spec.interface)
        st = self.actions.get(key)
        if st is None:
            return "unknown"
        if goal_id in st.handles:
            self._do_cancel(st, goal_id)
            return "sent"
        if goal_id in st.sent:
            st.pending_cancel.add(goal_id)
            return "pending"
        return "unknown"

    def _do_cancel(self, st: _ActionState, goal_id: str) -> None:
        handle = st.handles.get(goal_id)
        if handle is None:
            return
        cf = handle.cancel_goal_async()

        def _cancel_done(f: Any) -> None:
            try:
                resp = f.result()
                accepted = len(resp.goals_canceling) > 0
            except Exception:
                accepted = False
            self._event("actionStatus", "info" if accepted else "warning",
                        {"event": "cancelAccepted" if accepted else "cancelRejected",
                         "goalId": goal_id})

        cf.add_done_callback(_cancel_done)

    # ------------------------------------------------------------------
    # 주기 tick (stale 토픽, 커맨드 워치독) — 1초 타이머
    # ------------------------------------------------------------------

    def tick(self) -> None:
        now = time.monotonic()
        for observe_state in self.observes.values():
            thr = observe_state.spec.stale_after_ms
            if (not thr or observe_state.stale_flagged
                    or observe_state.last_arrival_mono is None):
                continue
            if (now - observe_state.last_arrival_mono) * 1000.0 > thr:
                observe_state.stale_flagged = True
                self._event("topicHealth", "warning",
                            {"event": "staleTopic",
                             "interface": observe_state.spec.interface,
                             "robot": observe_state.spec.robot_id,
                             "stale_after_ms": thr})
        for command_state in self.commands.values():
            safety = command_state.spec.command
            if (not safety or not safety.watchdog_ms
                    or command_state.last_publish_mono is None):
                continue
            if command_state.watchdog_fired:
                continue
            if (now - command_state.last_publish_mono) * 1000.0 > safety.watchdog_ms:
                command_state.watchdog_fired = True
                try:
                    command_state.publisher.publish(command_state.msg_class())
                    self._event("commandStatus", "warning",
                                {"event": "watchdogStop",
                                 "interface": command_state.spec.interface,
                                 "robot": command_state.spec.robot_id})
                except Exception as e:
                    self._event("commandStatus", "error",
                                {"event": "watchdogStopFailed",
                                 "interface": command_state.spec.interface,
                                 "robot": command_state.spec.robot_id,
                                 "error": str(e)})

    def publish_safety_stops(self) -> None:
        """종료 2단계: 워치독이 걸린 모든 커맨드에 정지 페이로드 발행."""
        for st in self.commands.values():
            if st.spec.command and st.spec.command.watchdog_ms:
                with suppress(Exception):
                    st.publisher.publish(st.msg_class())

    # ------------------------------------------------------------------

    def _event(self, category: str, severity: str, payload: dict[str, Any]) -> None:
        if category == "qosStatus":
            # 플래핑 링크에서 CIN 폭주 방지 — 인터페이스/이벤트당 최소 간격
            k = (str(payload.get("interface")), str(payload.get("event")))
            now = time.monotonic()
            last = self._qos_event_last.get(k)
            if last is not None and now - last < self.qos_event_coalesce_sec:
                return
            self._qos_event_last[k] = now
        try:
            self.on_event(category, severity, payload)
        except Exception:
            log.exception("event hook failed (category=%s)", category)

    def shutdown(self) -> None:
        for key in list(self.observes):
            self.unbind_observe(key)
        for command_state in self.commands.values():
            self.node.destroy_publisher(command_state.publisher)
        self.commands.clear()
        for entry in self.services.values():
            self.node.destroy_client(entry["client"])
        self.services.clear()
        for action_state in self.actions.values():
            action_state.client.destroy()
        self.actions.clear()


# TranscodeError는 호출자 대상 계약면의 일부.
__all__ = ["GenericROS2Adapter", "TranscodeError"]
