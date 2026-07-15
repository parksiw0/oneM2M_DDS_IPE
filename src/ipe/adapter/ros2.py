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
from dataclasses import dataclass, field
from typing import Any

from ipe.adapter.transform import extract_source_ts, parse_message
from ipe.config.spec import ACTION_QOS_CHANNELS, ActionSpec, ServiceSpec, TopicSpec
from ipe.core import qos as qosmod
from ipe.core.transcode import TranscodeError, from_canonical
from ipe.ir import TopicIR

log = logging.getLogger(__name__)

SELF_ECHO_WINDOW_SEC = 0.5

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


QOS_EVENT_COALESCE_SEC = 5.0


class GenericROS2Adapter:
    def __init__(self, node: Any, on_topic_ir: Callable[[TopicIR], None], on_event: EventHook,
                 qos_strictness: str = "reject") -> None:
        self.node = node
        self.on_topic_ir = on_topic_ir
        self.on_event = on_event
        self.qos_strictness = qos_strictness
        self.observes: dict[tuple[str, str], _ObserveState] = {}
        self.commands: dict[tuple[str, str], _CommandState] = {}
        self.services: dict[tuple[str, str], dict[str, Any]] = {}
        self.actions: dict[tuple[str, str], _ActionState] = {}
        self._reentrant = None
        self._qos_event_last: dict[tuple[str, str], float] = {}
        self._qos_dirty: set[tuple[str, str]] = set()
        self._qos_dirty_last: dict[tuple[str, str], float] = {}

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

    def snapshot(self) -> dict[str, list[tuple[str, list[str]]]]:
        from rclpy.action import get_action_names_and_types

        topics = self.node.get_topic_names_and_types()
        services = self.node.get_service_names_and_types()
        try:
            actions = get_action_names_and_types(self.node)
        except Exception:
            actions = []
        action_names = {n for n, _ in actions}
        # 액션 내부 엔티티(/_action/)는 topics/services에서 숨긴다
        topics = [(n, t) for n, t in topics if "/_action/" not in n]
        services = [(n, t) for n, t in services if "/_action/" not in n]
        return {
            "topics": [(n, list(t)) for n, t in topics],
            "services": [(n, list(t)) for n, t in services],
            "actions": [(n, list(t)) for n, t in actions if n in action_names],
        }

    # ------------------------------------------------------------------
    # observe (QoS reconcile + 이벤트 + 자기 에코 억제)
    # ------------------------------------------------------------------

    def bind_observe(self, spec: TopicSpec) -> bool:
        key = (spec.robot_id, spec.interface)
        if key in self.observes:
            return True
        msg_class = self._load_or_report("msg", spec.msg_type, spec)
        if msg_class is None:
            return False

        infos = self.node.get_publishers_info_by_topic(spec.interface)
        offered = [i.qos_profile for i in infos]
        resolved, events = qosmod.reconcile_observe(offered, spec.qos,
                                                    has_explicit=True)
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
            self._event("qosStatus", "warning",
                        {"event": ev, "interface": spec.interface, "robot": spec.robot_id})
        profile = qosmod.build_qos_profile(resolved)

        def callback(msg: Any, _key: tuple[str, str] = key) -> None:
            self._on_observe_msg(_key, msg)

        sub = self._create_subscription_degrading(msg_class, spec, profile, callback)
        if sub is None:
            return False
        self.observes[key] = _ObserveState(
            spec=spec, subscription=sub, applied_qos=resolved,
            offered_peers=[qosmod.endpoint_to_peer(i, "pub") for i in infos],
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
        infos = self.node.get_publishers_info_by_topic(st.spec.interface)
        peers = [qosmod.endpoint_to_peer(i, "pub") for i in infos]
        changed = peers != st.offered_peers
        st.offered_peers = peers
        if not infos:
            return changed   # publisher 부재는 변화가 아니다 — fallback 유지
        offered = [i.qos_profile for i in infos]
        new_spec, events = qosmod.reconcile_observe(offered, st.spec.qos,
                                                    has_explicit=True)
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
        infos = self.node.get_subscriptions_info_by_topic(st.spec.interface)
        peers = [qosmod.endpoint_to_peer(i, "sub") for i in infos]
        changed = peers != st.requested_peers
        st.requested_peers = peers
        if not infos:
            return changed   # subscriber 부재 — fallback 유지
        new_spec, events = qosmod.reconcile_command(
            [i.qos_profile for i in infos], st.spec.qos)
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
            offered = [i.qos_profile for i in
                       self.node.get_publishers_info_by_topic(iface)]
            resolved, _ = qosmod.reconcile_observe(offered, candidate,
                                                   has_explicit=True)
            guarded, strict = qosmod.strictness_guard(resolved, offered,
                                                      self.qos_strictness)
            if strict and self.qos_strictness == "reject":
                return False, strict
            profile = qosmod.build_qos_profile(guarded)
            pairs = [(o, profile) for o in offered]
        else:
            requested = [i.qos_profile for i in
                         self.node.get_subscriptions_info_by_topic(iface)]
            resolved, _ = qosmod.reconcile_command(requested, candidate)
            profile = qosmod.build_qos_profile(resolved)
            pairs = [(profile, r) for r in requested]
        warnings: list[str] = []
        for pub_q, sub_q in pairs:
            ok, reasons = qosmod.check_compatible(pub_q, sub_q)
            if not ok:
                return False, reasons
            warnings.extend(reasons)
        return True, list(dict.fromkeys(warnings))

    def rebind_interface(self, key: tuple[str, str], new_qos: Any) -> bool:
        """qos_update 수락 경로(§4.5.3) — 설정 QoS 교체 후 단건 재바인딩.
        observe는 seq를 보존한다. direction=both면 양방향 모두 재생성."""
        ok = True
        ost = self.observes.get(key)
        if ost is not None:
            ost.spec.qos = new_qos
            seq = ost.seq
            self.unbind_observe(key)
            if self.bind_observe(ost.spec):
                self.observes[key].seq = seq
            else:
                ok = False
        cst = self.commands.get(key)
        if cst is not None:
            cst.spec.qos = new_qos
            self.unbind_command(key)
            ok = self.bind_command(cst.spec) and ok
        return ok

    def qos_states(self) -> list[dict[str, Any]]:
        """게시 입력 QoSStateIR 스냅숏 — 방향별 1건 (direction=both는 2건)."""
        out: list[dict[str, Any]] = []
        for (robot, iface), st in self.observes.items():
            out.append({"robot_id": robot, "interface": iface, "direction": "observe",
                        "configured": st.spec.qos, "applied": st.applied_qos,
                        "peers": list(st.offered_peers), "events": list(st.events)})
        for (robot, iface), st in self.commands.items():
            out.append({"robot_id": robot, "interface": iface, "direction": "command",
                        "configured": st.spec.qos, "applied": st.applied_qos,
                        "peers": list(st.requested_peers), "events": list(st.events)})
        return out

    def pop_qos_dirty(self) -> set[tuple[str, str]]:
        """matched 콜백이 표시한 재조정 대상 키를 소비한다(Iron+, §4.6.1)."""
        dirty, self._qos_dirty = self._qos_dirty, set()
        return dirty

    def _mark_qos_dirty(self, key: tuple[str, str]) -> None:
        now = time.monotonic()
        last = self._qos_dirty_last.get(key)
        if last is not None and now - last < QOS_EVENT_COALESCE_SEC:
            return
        self._qos_dirty_last[key] = now
        self._qos_dirty.add(key)

    def _create_subscription_degrading(self, msg_class: Any, spec: TopicSpec,
                                       profile: Any, callback: Any) -> Any:
        """QoS 이벤트 콜백 등록 — 미지원 축은 하나씩 빼며 재시도(점진 강등)."""
        SubscriptionEventCallbacks = _event_callbacks("sub")

        key = (spec.robot_id, spec.interface)

        def _mk(name: str) -> Callable[[Any], None]:
            def cb(info: Any) -> None:
                self._event("qosStatus", "warning",
                            {"event": name, "interface": spec.interface,
                             "robot": spec.robot_id, **_qos_event_fields(info)})
            return cb

        axes = {"deadline": _mk("deadlineMissed"),
                "liveliness": _mk("livelinessChanged"),
                "incompatible_qos": _mk("qosMismatch"),
                "message_lost": _mk("messageLost"),
                "incompatible_type": _mk("incompatibleType"),
                # matched(Iron+)는 CIN을 내지 않는 내부 재조정 트리거(§4.6.1)
                "matched": lambda _info: self._mark_qos_dirty(key)}
        axes, unsupported = _supported_axes(SubscriptionEventCallbacks, axes)
        for axis in unsupported:
            self._event("qosStatus", "warning",
                        {"event": "eventCallbackUnsupported", "axis": axis,
                         "interface": spec.interface, "robot": spec.robot_id})
        while True:
            try:
                return self.node.create_subscription(
                    msg_class, spec.interface, callback, profile,
                    event_callbacks=SubscriptionEventCallbacks(**axes))
            except Exception as e:
                name = type(e).__name__
                if "UnsupportedEventType" in name and axes:
                    dropped = next(iter(axes))
                    axes.pop(dropped)
                    self._event("qosStatus", "warning",
                                {"event": "eventCallbackUnsupported", "axis": dropped,
                                 "interface": spec.interface, "robot": spec.robot_id})
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
                 if now - ts < SELF_ECHO_WINDOW_SEC]
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
        msg_class = self._load_or_report("msg", spec.msg_type, spec)
        if msg_class is None:
            return False

        infos = self.node.get_subscriptions_info_by_topic(spec.interface)
        requested = [i.qos_profile for i in infos]
        resolved, events = qosmod.reconcile_command(requested, spec.qos)
        for ev in events:
            self._event("qosStatus", "warning",
                        {"event": ev, "interface": spec.interface, "robot": spec.robot_id})
        profile = qosmod.build_qos_profile(resolved)
        pub = self._create_publisher_degrading(msg_class, spec, profile, key)
        self.commands[key] = _CommandState(
            spec=spec, publisher=pub, msg_class=msg_class, applied_qos=resolved,
            requested_peers=[qosmod.endpoint_to_peer(i, "sub") for i in infos],
            events=list(events))
        log.info("command bound: %s [%s] (%s)", spec.interface, spec.msg_type, spec.robot_id)
        return True

    def _create_publisher_degrading(self, msg_class: Any, spec: TopicSpec,
                                    profile: Any, key: tuple[str, str]) -> Any:
        """발행자 생성 — 구독과 동일한 이벤트 축 점진 강등(§4.6.1).
        LivelinessLost는 의도적으로 미등록: command liveliness는 AUTOMATIC
        고정이라 lost == IPE 프로세스 정지와 동치다."""
        PublisherEventCallbacks = _event_callbacks("pub")

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
        axes, unsupported = _supported_axes(PublisherEventCallbacks, axes)
        for axis in unsupported:
            self._event("qosStatus", "warning",
                        {"event": "eventCallbackUnsupported", "axis": axis,
                         "interface": spec.interface, "robot": spec.robot_id})
        while axes:
            try:
                return self.node.create_publisher(
                    msg_class, spec.interface, profile,
                    event_callbacks=PublisherEventCallbacks(**axes))
            except Exception as e:
                if "UnsupportedEventType" not in type(e).__name__:
                    break
                dropped = next(iter(axes))
                axes.pop(dropped)
                self._event("qosStatus", "warning",
                            {"event": "eventCallbackUnsupported", "axis": dropped,
                             "interface": spec.interface, "robot": spec.robot_id})
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
        srv_class = self._load_or_report("srv", spec.srv_type, spec)
        if srv_class is None:
            return False
        kwargs: dict[str, Any] = {"callback_group": self._reentrant_group()}
        if spec.qos is not None:
            kwargs["qos_profile"] = qosmod.build_qos_profile(spec.qos)
        client = self.node.create_client(srv_class, spec.interface, **kwargs)
        self.services[key] = {"spec": spec, "client": client, "srv_class": srv_class}
        return True

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
        action_class = self._load_or_report("action", spec.action_type, spec)
        if action_class is None:
            return False
        from rclpy.action import ActionClient
        kwargs: dict[str, Any] = {"callback_group": self._reentrant_group()}
        for chan, q in (spec.qos or {}).items():
            kwargs[_ACTION_CHAN_KWARG[chan]] = qosmod.build_qos_profile(q)
        client = ActionClient(self.node, action_class, spec.interface, **kwargs)
        self.actions[key] = _ActionState(spec=spec, client=client, action_class=action_class)
        return True

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
        for st in self.observes.values():
            thr = st.spec.stale_after_ms
            if not thr or st.stale_flagged or st.last_arrival_mono is None:
                continue
            if (now - st.last_arrival_mono) * 1000.0 > thr:
                st.stale_flagged = True
                self._event("topicHealth", "warning",
                            {"event": "staleTopic", "interface": st.spec.interface,
                             "robot": st.spec.robot_id, "stale_after_ms": thr})
        for st in self.commands.values():
            safety = st.spec.command
            if not safety or not safety.watchdog_ms or st.last_publish_mono is None:
                continue
            if st.watchdog_fired:
                continue
            if (now - st.last_publish_mono) * 1000.0 > safety.watchdog_ms:
                st.watchdog_fired = True
                try:
                    st.publisher.publish(st.msg_class())   # 0값/정지 페이로드
                    self._event("commandStatus", "warning",
                                {"event": "watchdogStop", "interface": st.spec.interface,
                                 "robot": st.spec.robot_id})
                except Exception as e:
                    self._event("commandStatus", "error",
                                {"event": "watchdogStopFailed", "interface": st.spec.interface,
                                 "robot": st.spec.robot_id, "error": str(e)})

    def publish_safety_stops(self) -> None:
        """종료 2단계: 워치독이 걸린 모든 커맨드에 정지 페이로드 발행."""
        for st in self.commands.values():
            if st.spec.command and st.spec.command.watchdog_ms:
                try:
                    st.publisher.publish(st.msg_class())
                except Exception:
                    pass

    # ------------------------------------------------------------------

    def _event(self, category: str, severity: str, payload: dict[str, Any]) -> None:
        if category == "qosStatus":
            # 플래핑 링크에서 CIN 폭주 방지 — 인터페이스/이벤트당 최소 간격
            k = (str(payload.get("interface")), str(payload.get("event")))
            now = time.monotonic()
            last = self._qos_event_last.get(k)
            if last is not None and now - last < QOS_EVENT_COALESCE_SEC:
                return
            self._qos_event_last[k] = now
        try:
            self.on_event(category, severity, payload)
        except Exception:
            log.exception("event hook failed (category=%s)", category)

    def shutdown(self) -> None:
        for key in list(self.observes):
            self.unbind_observe(key)
        for st in self.commands.values():
            self.node.destroy_publisher(st.publisher)
        self.commands.clear()
        for entry in self.services.values():
            self.node.destroy_client(entry["client"])
        self.services.clear()
        for st in self.actions.values():
            st.client.destroy()
        self.actions.clear()


# TranscodeError는 호출자 대상 계약면의 일부.
__all__ = ["GenericROS2Adapter", "TranscodeError"]
