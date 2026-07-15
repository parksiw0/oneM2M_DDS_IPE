"""QoS 조정 엔진 (DESIGN §8) — IPE의 모든 QoS 결정을 이 모듈이 소유한다.

observe는 제공(offered) QoS에 대한 최약-호환 요청으로, command는 모든 요청을 지배하는
최소 제공으로 조정하고, 호환성 판정은 rclpy.qos.qos_check_compatible 단일 권위에 맡긴다.
rclpy는 함수 안에서 지연 임포트한다 — 순수 조정 로직은 ROS 환경 없이 실행돼야 한다.
제공/요청 프로파일은 덕 타이핑으로 받으며(TopicEndpointInfo.qos_profile 형태면 충분),
이벤트는 순서 보존·중복 제거된 평문 문자열 어휘로 반환된다.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from ipe.config.spec import QoSSpec

# 그래프 인트로스펙션이 보고하는 DDS/rmw 무한 duration 센티널
# (RMW_DURATION_INFINITE == 2**63 - 1 나노초).
INT64_MAX = 2**63 - 1

# 조정 비교용 강도 맵. rclpy enum 정수를 그대로 쓰면 안 된다: RELIABLE=1 <
# BEST_EFFORT=2라서 직접 비교가 의미를 뒤집는다. UNKNOWN/SYSTEM_DEFAULT는
# 의도적으로 빠져 있다 — 비교에서 제외한다.
STRENGTH: dict[str, dict[str, int]] = {
    "reliability": {"BEST_EFFORT": 0, "RELIABLE": 1},
    "durability": {"VOLATILE": 0, "TRANSIENT_LOCAL": 1},
    "liveliness": {"AUTOMATIC": 0, "MANUAL_BY_TOPIC": 1},
}


# ---------------------------------------------------------------------------
# 센티널과 단위 변환
# ---------------------------------------------------------------------------

def is_infinite(duration: Any) -> bool:
    """*duration*이 rmw 무한 센티널(또는 None=미설정)이면 True.

    rclpy Duration, 원시 int 나노초, None을 받는다. 엄격한 센티널 검사라서
    0 ns(rmw "default" 센티널)는 여기서 무한이 아니다 — ``duration_ms``가
    둘 다 None(미설정)으로 접는다.
    """
    if duration is None:
        return True
    ns = duration.nanoseconds if hasattr(duration, "nanoseconds") else int(duration)
    return ns >= INT64_MAX


def duration_ms(duration: Any) -> int | None:
    """duration -> 정수 밀리초; 미설정/무한이면 None.

    rmw 센티널 둘 다 None으로 접는다: INT64_MAX(RMW_DURATION_INFINITE)와
    0(RMW_*_DEFAULT, 기본 생성 QoSProfile이 갖는 값). 실제 유한 QoS
    duration은 0일 수 없다.
    """
    if duration is None:
        return None
    ns = duration.nanoseconds if hasattr(duration, "nanoseconds") else int(duration)
    if ns == 0 or ns >= INT64_MAX:
        return None
    return ns // 1_000_000


def _policy_name(value: Any) -> str:
    """enum 멤버 또는 문자열 -> 정준 대문자 정책 이름."""
    if isinstance(value, str):
        return value.upper()
    name = getattr(value, "name", None)
    if name is not None:
        return str(name)
    raise TypeError(f"unsupported QoS policy value: {value!r}")


def _strengths(axis: str, names: list[str]) -> list[tuple[int, str]]:
    """비교 가능한 이름들의 (강도, 이름) 쌍; UNKNOWN/SYSTEM_DEFAULT는 버린다."""
    table = STRENGTH[axis]
    return [(table[n], n) for n in names if n in table]


# ---------------------------------------------------------------------------
# QoSProfile 구성
# ---------------------------------------------------------------------------

def build_qos_profile(spec: QoSSpec):
    """8개 속성이 전부 결정된 rclpy QoSProfile을 만든다.

    enum 축과 history/depth는 항상 명시한다 — kwargs를 일부만 주면 rclpy가
    임의 기본값을 채운다. duration 축은 spec 값이 None이면 생략하며,
    생략된 kwarg == Infinite.
    """
    from rclpy.duration import Duration
    from rclpy.qos import (
        DurabilityPolicy,
        HistoryPolicy,
        LivelinessPolicy,
        QoSProfile,
        ReliabilityPolicy,
    )

    kwargs: dict[str, Any] = {
        "reliability": ReliabilityPolicy[spec.reliability],
        "durability": DurabilityPolicy[spec.durability],
        "history": HistoryPolicy[spec.history],
        "depth": spec.depth,
        "liveliness": LivelinessPolicy[spec.liveliness],
    }
    if spec.deadline_ms is not None:
        kwargs["deadline"] = Duration(nanoseconds=int(spec.deadline_ms * 1e6))
    if spec.lifespan_ms is not None:
        kwargs["lifespan"] = Duration(nanoseconds=int(spec.lifespan_ms * 1e6))
    if spec.liveliness_lease_duration_ms is not None:
        kwargs["liveliness_lease_duration"] = Duration(
            nanoseconds=int(spec.liveliness_lease_duration_ms * 1e6)
        )
    return QoSProfile(**kwargs)


# ---------------------------------------------------------------------------
# 조정: observe (IPE = 구독자, 최약-호환)
# ---------------------------------------------------------------------------

def reconcile_observe(
    offered: list[Any], configured: QoSSpec, has_explicit: bool
) -> tuple[QoSSpec, list[str]]:
    """제공된 발행자 QoS에 대해 축별 최약-호환 요청을 만든다.

    - reliability/durability/liveliness: 제공값 중 최소 강도. ``has_explicit``이면
      설정값도 풀에 합류해 명시적으로 더 약한 사용자 선택이 존중된다(명시적으로
      더 강한 값은 이길 수 없다 — 매칭이 0이 된다).
    - deadline/lease: max(제공값); 하나라도 Infinite/미설정이면 None. 설정된
      duration은 여기서 보지 않는다 — 명시적인 더 엄격한 오버라이드는
      strictness_guard 소관.
    - history/depth(그리고 조정 표에 없는 lifespan): 항상 설정값 — 그래프
      인트로스펙션은 history=UNKNOWN, depth=0을 보고한다(재구성이지 사본이 아님).
    - 제공이 비면(로봇 미기동): 설정값 그대로 + 'noPublisherFallback';
      호출자가 디스커버리 갱신 때 재평가한다.
    """
    if not offered:
        return configured, ["noPublisherFallback"]

    events: list[str] = []

    def weakest(axis: str, attr: str, configured_value: str) -> str:
        pool = _strengths(axis, [_policy_name(getattr(p, attr)) for p in offered])
        if has_explicit:
            pool += _strengths(axis, [configured_value])
        if not pool:
            return configured_value
        return min(pool)[1]

    reliability = weakest("reliability", "reliability", configured.reliability)
    durability = weakest("durability", "durability", configured.durability)
    liveliness = weakest("liveliness", "liveliness", configured.liveliness)

    if configured.durability == "TRANSIENT_LOCAL" and durability == "VOLATILE":
        # 래치 손실은 RxO 호환이라 DDS 수준에서는 조용히 지나간다 —
        # 이 이벤트가 유일한 가시성이다.
        events.append("latchedDowngraded")

    deadline_ms = _max_or_none([duration_ms(p.deadline) for p in offered])
    lease_ms = _max_or_none(
        [duration_ms(p.liveliness_lease_duration) for p in offered]
    )

    spec = replace(
        configured,
        reliability=reliability,
        durability=durability,
        liveliness=liveliness,
        deadline_ms=deadline_ms,
        liveliness_lease_duration_ms=lease_ms,
    )
    return spec, _dedupe(events)


def _max_or_none(values: list[int | None]) -> int | None:
    """제공 duration들의 max; 미설정/무한(None)이 하나라도 있으면 None이 지배."""
    if any(v is None for v in values):
        return None
    return max(values) if values else None


# ---------------------------------------------------------------------------
# 조정: command (IPE = 발행자, 최강-요청)
# ---------------------------------------------------------------------------

def reconcile_command(
    requested: list[Any], configured: QoSSpec
) -> tuple[QoSSpec, list[str]]:
    """모든 구독자 요청을 지배하는 최소 QoS를 제공한다.

    - reliability/liveliness: 요청값 중 최대 강도; 설정값이 약하면 업그레이드
      ('qosUpgraded').
    - durability: 기본은 VOLATILE 유지, TRANSIENT_LOCAL 요청이 있으면 제공을
      업그레이드. lifespan_ms 없는 TL 제공은 'transientLocalLifespanRequired'도
      낸다 — lifespan이 없으면 재부팅한 로봇이 마지막 명령을 재생한다.
    - deadline/lease: 유한한 요청값들의 min(Infinite 제외); 전부 Infinite면
      요구 없음 -> 설정값 유지. 설정값이 None이거나 요구보다 길면 단축(업그레이드).
    - 요청이 비면: 설정값 그대로 + 'noSubscriberFallback'; 호출자가 디스커버리
      갱신 때 재평가한다.
    """
    if not requested:
        return configured, ["noSubscriberFallback"]

    events: list[str] = []
    spec = configured

    def strongest(axis: str, attr: str) -> str | None:
        pool = _strengths(axis, [_policy_name(getattr(p, attr)) for p in requested])
        return max(pool)[1] if pool else None

    for axis, field in (
        ("reliability", "reliability"),
        ("durability", "durability"),
        ("liveliness", "liveliness"),
    ):
        need = strongest(axis, field)
        if need is None:
            continue
        have = getattr(spec, field)
        if have in STRENGTH[axis] and STRENGTH[axis][have] >= STRENGTH[axis][need]:
            continue
        spec = replace(spec, **{field: need})
        events.append("qosUpgraded")

    if spec.durability == "TRANSIENT_LOCAL" and spec.lifespan_ms is None:
        events.append("transientLocalLifespanRequired")

    deadline_need = _min_finite([duration_ms(p.deadline) for p in requested])
    if deadline_need is not None and (
        spec.deadline_ms is None or spec.deadline_ms > deadline_need
    ):
        spec = replace(spec, deadline_ms=deadline_need)
        events.append("qosUpgraded")

    lease_need = _min_finite(
        [duration_ms(p.liveliness_lease_duration) for p in requested]
    )
    if lease_need is not None and (
        spec.liveliness_lease_duration_ms is None
        or spec.liveliness_lease_duration_ms > lease_need
    ):
        spec = replace(spec, liveliness_lease_duration_ms=lease_need)
        events.append("qosUpgraded")

    return spec, _dedupe(events)


def _min_finite(values: list[int | None]) -> int | None:
    """유한한 요청 duration들의 min; Infinite/미설정은 아예 제외."""
    finite = [v for v in values if v is not None]
    return min(finite) if finite else None


# ---------------------------------------------------------------------------
# 호환성 판정 권위
# ---------------------------------------------------------------------------

def check_compatible(pub_profile: Any, sub_profile: Any) -> tuple[bool, list[str]]:
    """``rclpy.qos.qos_check_compatible``(단일 권위)을 감싼다.

    OK -> (True, []); WARNING(UNKNOWN/SYSTEM_DEFAULT 개입) -> (True, [이유])로
    호출자가 qosStatus 이벤트를 띄우게 하고; ERROR -> (False, [이유]).
    """
    from rclpy.qos import QoSCompatibility, qos_check_compatible

    compatibility, reason = qos_check_compatible(pub_profile, sub_profile)
    if compatibility == QoSCompatibility.OK:
        return True, []
    return compatibility != QoSCompatibility.ERROR, [reason]


# ---------------------------------------------------------------------------
# observe 방향 엄격성 가드
# ---------------------------------------------------------------------------

def strictness_guard(
    spec: QoSSpec, offered: list[Any], mode: str
) -> tuple[QoSSpec, list[str]]:
    """제공값보다 엄격한 observe 오버라이드(매칭 0의 원인)를 탐지한다.

    구독 요청이 제공보다 엄격한 경우:
    - deadline: spec은 유한한데 어떤 발행자가 Infinite를 제공하거나, 어떤
      발행자의 제공 deadline보다 짧을 때(매칭된 발행자마다
      req.period >= offered.period가 성립해야 함);
    - lease: liveliness_lease_duration에 같은 규칙;
    - liveliness: spec의 kind가 제공된 가장 약한 kind보다 강할 때.

    ``mode='reject'``: spec을 그대로 반환; 이벤트가 비어 있지 않으면 호출자가
    설정을 거부해야 한다. ``mode='demote'``: 위반 축을 제공값 중 최약-호환으로
    강등해 반환하고, 같은 이벤트를 qosStatus 보고용으로 돌려준다. *offered*가
    비면 비교 대상이 없으므로 (spec, []).
    """
    if mode not in ("reject", "demote"):
        raise ValueError(f"strictness mode must be 'reject' or 'demote', got {mode!r}")
    if not offered:
        return spec, []

    events: list[str] = []
    demoted = spec

    offered_deadlines = [duration_ms(p.deadline) for p in offered]
    if spec.deadline_ms is not None and any(
        d is None or d > spec.deadline_ms for d in offered_deadlines
    ):
        events.append("strictDeadline")
        demoted = replace(demoted, deadline_ms=_max_or_none(offered_deadlines))

    offered_leases = [duration_ms(p.liveliness_lease_duration) for p in offered]
    if spec.liveliness_lease_duration_ms is not None and any(
        d is None or d > spec.liveliness_lease_duration_ms for d in offered_leases
    ):
        events.append("strictLease")
        demoted = replace(
            demoted, liveliness_lease_duration_ms=_max_or_none(offered_leases)
        )

    pool = _strengths(
        "liveliness", [_policy_name(p.liveliness) for p in offered]
    )
    if pool and spec.liveliness in STRENGTH["liveliness"]:
        floor = min(pool)[1]
        if STRENGTH["liveliness"][spec.liveliness] > STRENGTH["liveliness"][floor]:
            events.append("strictLiveliness")
            demoted = replace(demoted, liveliness=floor)

    return (demoted if mode == "demote" else spec), events


# ---------------------------------------------------------------------------
# lbl 메타데이터 (TR-0079)
# ---------------------------------------------------------------------------

def spec_to_metadata(spec: QoSSpec) -> dict[str, str]:
    """lbl 합성용 문자열 값 8속성 레코드.

    미설정 duration은 "INF"로 기록한다(kwargs 생략 == Infinite).
    """

    def _d(ms: int | None) -> str:
        return "INF" if ms is None else str(ms)

    return {
        "reliability": spec.reliability,
        "durability": spec.durability,
        "history": spec.history,
        "depth": str(spec.depth),
        "deadline_ms": _d(spec.deadline_ms),
        "lifespan_ms": _d(spec.lifespan_ms),
        "liveliness": spec.liveliness,
        "lease_ms": _d(spec.liveliness_lease_duration_ms),
    }


def _dedupe(events: list[str]) -> list[str]:
    """순서 보존 중복 제거(이벤트는 어휘이지 카운트가 아니다)."""
    return list(dict.fromkeys(events))


# ---------------------------------------------------------------------------
# QoS flexContainer 레코드 (QoS_FCNT_설계서 §4.2)
# ---------------------------------------------------------------------------

QOS_FCNT_SVER = "1"

# peers 원소 축 ↔ dvAxs 이름. RxO 참여 5축만 — history/depth는 인트로스펙션이
# UNKNOWN/0을 보고하므로 peers에 싣지 않는다(§4.2.4).
_PEER_AXES = (("rlb", "reliability"), ("drb", "durability"),
              ("liv", "liveliness"), ("ddl", "deadline"), ("lse", "lease"))


def _inf_ms(ms: int | None) -> str:
    return "INF" if ms is None else str(ms)


def _axes(prefix: str, spec: QoSSpec) -> dict[str, Any]:
    return {
        f"{prefix}Rlb": spec.reliability,
        f"{prefix}Drb": spec.durability,
        f"{prefix}Hst": spec.history,
        f"{prefix}Dpt": spec.depth,
        f"{prefix}Ddl": _inf_ms(spec.deadline_ms),
        f"{prefix}Lsp": _inf_ms(spec.lifespan_ms),
        f"{prefix}Liv": spec.liveliness,
        f"{prefix}Lse": _inf_ms(spec.liveliness_lease_duration_ms),
    }


def spec_to_fcnt_attrs(
    *,
    direction: str,
    interface: str,
    robot_id: str,
    configured: QoSSpec,
    applied: QoSSpec | None = None,
    msg_type: str | None = None,
    smode: str | None = None,
    events: list[str] | None = None,
    peers: list[dict[str, Any]] | None = None,
    peer_count: int | None = None,
) -> dict[str, Any]:
    """총함수 FCNT 레코드 합성 — 부분 레코드를 만들 수 없는 유일한 API.

    tinyIoT UPDATE는 custom_attrs blob을 요청 포함분만으로 덮어쓰므로(T4)
    IPE의 모든 게시는 전체 속성을 실어야 한다(§4.2.2 규칙 2). applied가
    있으면(=바인딩 후 게시) ap*·evts·peers·pcnt·dvAxs를 빈 값이라도 실어
    한번 쓴 속성이 생략으로 유실되는 일이 없게 한다.
    """
    rec: dict[str, Any] = {
        "dir": direction, "iface": interface, "robot": robot_id,
        "sver": QOS_FCNT_SVER,
    }
    if msg_type:
        rec["rtype"] = msg_type
    if configured.profile:
        rec["pfRef"] = configured.profile
    rec.update(_axes("cf", configured))
    if smode:
        rec["smode"] = smode
    if applied is not None:
        rec.update(_axes("ap", applied))
        rec["evts"] = list(events or [])
        ps = list(peers or [])
        rec["peers"] = ps
        rec["pcnt"] = peer_count if peer_count is not None else len(ps)
        rec["dvAxs"] = divergent_axes(ps)
    return rec


def endpoint_to_peer(info: Any, ep: str) -> dict[str, Any]:
    """TopicEndpointInfo -> peers 원소 (§4.2.4).

    UNKNOWN/SYSTEM_DEFAULT/BEST_AVAILABLE은 관측 사실로 통과시킨다 —
    조정 비교에서만 제외될 뿐 보고 가치는 있다.
    """
    q = info.qos_profile
    node = getattr(info, "node_name", "") or ""
    ns = (getattr(info, "node_namespace", "") or "").rstrip("/")
    return {
        "ep": ep,
        "node": f"{ns}/{node}" if node else "",
        "rlb": _policy_name(q.reliability),
        "drb": _policy_name(q.durability),
        "liv": _policy_name(q.liveliness),
        "ddl": _inf_ms(duration_ms(q.deadline)),
        "lse": _inf_ms(duration_ms(q.liveliness_lease_duration)),
        "lsp": _inf_ms(duration_ms(q.lifespan)),
    }


def divergent_axes(peers: list[dict[str, Any]]) -> list[str]:
    """엔드포인트 간 값이 갈리는 RxO 축 이름 목록 — 다중 발행자 상이의 요약."""
    out: list[str] = []
    for short, name in _PEER_AXES:
        if len({p.get(short) for p in peers if p.get(short) is not None}) > 1:
            out.append(name)
    return out
