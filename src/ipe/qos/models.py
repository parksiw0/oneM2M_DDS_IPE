"""QoS profiles and management-resource options shared by policy modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict


@dataclass(frozen=True)
class QoSSpec:
    reliability: str = "RELIABLE"
    durability: str = "VOLATILE"
    history: str = "KEEP_LAST"
    depth: int = 10
    deadline_ms: int | None = None
    lifespan_ms: int | None = None
    liveliness: str = "AUTOMATIC"
    liveliness_lease_duration_ms: int | None = None
    # 유래한 qos_profiles 프리셋 이름 — FCNT의 pfRef(정책 아님, 출처 표기)
    profile: str | None = None

    def merged(self, override: dict[str, Any]) -> QoSSpec:
        """필드 단위 병합: 프리셋 값 위에 인라인 키가 덮어쓴다."""
        data = {**self.__dict__}
        for k, v in override.items():
            if k == "profile":
                continue
            if k in data and v is not None:
                data[k] = _norm_enum(k, v)
        return QoSSpec(**data)


def _norm_enum(key: str, value: Any) -> Any:
    """enum형 QoS 필드를 UPPERCASE로 정규화; int/None은 그대로 통과."""
    if key in ("reliability", "durability", "history", "liveliness") and isinstance(value, str):
        return value.upper()
    return value


@dataclass(frozen=True)
class QosFcntSpec:
    """qos_fcnt 설정 블록 (QoS_FCNT_설계서 §5.2)."""

    enabled: bool = True
    type: str = "ros:tqos"
    cnd: str = "kr.ac.sejong.seslab.ros2.moduleclass.topicQos"
    service_type: str = "ros:sqos"
    service_cnd: str = "kr.ac.sejong.seslab.ros2.moduleclass.serviceQos"
    action_type: str = "ros:aqos"
    action_cnd: str = "kr.ac.sejong.seslab.ros2.moduleclass.actionQos"
    lbl_compat: bool = True
    allow_update: bool = False
    publish_min_interval_ms: int = 5000
    peers_max: int = 8

    def specialization(self, interface_kind: str) -> tuple[str, str]:
        """Return the FCNT specialization for one ROS interface kind."""
        if interface_kind == "service":
            return self.service_type, self.service_cnd
        if interface_kind == "action":
            return self.action_type, self.action_cnd
        return self.type, self.cnd


ACTION_QOS_CHANNELS = (
    "goal_service",
    "result_service",
    "cancel_service",
    "feedback_sub",
    "status_sub",
)


class QoSStateIR(TypedDict):
    """어댑터 -> 앱 QoS 상태 운반 계약 (QoS_FCNT_설계서 §5.2).

    qos FCNT 게시의 입력이며 TopicIR.metadata에는 싣지 않는다(CIN마다
    QoS를 나르지 않음).
    """

    robot_id: str
    interface: str
    direction: str               # "observe" | "command"
    configured: Any              # QoSSpec (config.py 해석 결과)
    applied: Any                 # QoSSpec | None — 바인딩 전 None
    peers: list[dict[str, Any]]  # qos.codec.endpoint_to_peer 원소
    events: list[str]            # 마지막 조정·가드 어휘 (§4.6.2)
