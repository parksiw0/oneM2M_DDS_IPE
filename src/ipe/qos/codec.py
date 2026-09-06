"""oneM2M QoS snapshots, labels, and ROS peer metadata encoding."""

from __future__ import annotations

from typing import Any

from ipe.qos._shared import _policy_name, duration_ms
from ipe.qos.models import QoSSpec


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


QOS_FCNT_SVER = "1"

_PEER_AXES = (
    ("rlb", "reliability"),
    ("drb", "durability"),
    ("liv", "liveliness"),
    ("ddl", "deadline"),
    ("lse", "lease"),
)


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
    interface_kind: str = "topic",
    mapping_targets: dict[str, dict[str, Any]] | None = None,
    native_policies: dict[str, Any] | None = None,
    policy_source: str = "ROS2_RMW",
    data_resource_refs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """총함수 FCNT 레코드 합성 — 부분 레코드를 만들 수 없는 유일한 API.

    tinyIoT UPDATE는 custom_attrs blob을 요청 포함분만으로 덮어쓰므로(T4)
    IPE의 모든 게시는 전체 속성을 실어야 한다(§4.2.2 규칙 2). applied가
    있으면(=바인딩 후 게시) ap*·evts·peers·pcnt·dvAxs를 빈 값이라도 실어
    한번 쓴 속성이 생략으로 유실되는 일이 없게 한다.
    """
    from ipe.qos.registry import compact_policy_groups

    rec: dict[str, Any] = {
        "dir": direction,
        "iface": interface,
        "robot": robot_id,
        "sver": QOS_FCNT_SVER,
        "mver": "2",
        "ikind": interface_kind,
        "drefs": list(data_resource_refs or []),
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
    groups = compact_policy_groups(
        configured,
        applied,
        source=policy_source,
        native_policies=native_policies,
        mapping_overrides=mapping_targets,
    )
    rec.update(groups)
    return rec


def interface_qos_fcnt_attrs(
    *,
    interface_kind: str,
    interface: str,
    robot_id: str,
    channels: dict[str, QoSSpec | None],
    msg_type: str | None = None,
    data_resource_refs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Build one management snapshot for a logical service or action interface."""
    from ipe.qos.registry import DDS_QOS_POLICY_NAMES, rmw_policy_values

    records: dict[str, Any] = {}
    for channel, profile in channels.items():
        records[channel] = {
            "configured": rmw_policy_values(profile) if profile is not None else None,
            "source": "CONFIGURED" if profile is not None else "ROS2_DEFAULT",
        }
    rec: dict[str, Any] = {
        "sver": QOS_FCNT_SVER,
        "mver": "2",
        "ikind": interface_kind,
        "iface": interface,
        "robot": robot_id,
        "channels": records,
        "drefs": list(data_resource_refs or []),
        "resourceMappings": {
            "LIFESPAN": {"target": "drefs", "result": "NOT_APPLIED_TO_ONEM2M"},
            "HISTORY": {"target": "drefs", "result": "NOT_APPLIED_TO_ONEM2M"},
            "RESOURCE_LIMITS": {
                "target": "drefs",
                "result": "DDS_VALUE_UNAVAILABLE",
            },
        },
        "behaviorStatus": {
            name: {
                "source": "channels",
                "result": "APPLIED_AT_ROS2_ENDPOINT",
            }
            for name in ("RELIABILITY", "DEADLINE", "LIVELINESS", "DURABILITY")
        },
        "ddsQoSProperties": {},
        "unavailablePolicies": [
            name
            for name in DDS_QOS_POLICY_NAMES
            if name
            not in {"RELIABILITY", "DURABILITY", "HISTORY", "DEADLINE", "LIFESPAN", "LIVELINESS"}
        ],
    }
    if msg_type:
        rec["rtype"] = msg_type
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
