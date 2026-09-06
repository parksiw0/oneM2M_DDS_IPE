"""Interface specifications and topic data shared by the adapter and pipeline.

Resolved interface specifications describe how each topic, service, or action
is bridged. TopicIR carries an observed sample between processing modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

from ipe.qos.models import QoSSpec

Direction = Literal["observe", "command", "both"]
Representation = Literal["historical", "latest", "both", "sampled"]
FeedbackMode = Literal["log", "latest", "sampled", "combined"]


@dataclass(frozen=True)
class SampleSpec:
    rate_hz: float | None = None
    min_interval_ms: int | None = None

    @property
    def interval_sec(self) -> float:
        if self.min_interval_ms is not None:
            return self.min_interval_ms / 1000.0
        if self.rate_hz:
            return 1.0 / self.rate_hz
        return 0.0


@dataclass(frozen=True)
class CommandSafety:
    rate_limit_hz: float | None = None
    clamp: dict[str, tuple[float, float]] = field(default_factory=dict)
    watchdog_ms: int | None = None
    max_age_ms: int = 5000  # 수신 신선도 게이트
    liveliness_lease_ms: int | None = None  # 로봇 측 IPE 사망 감지


@dataclass(frozen=True)
class SourceTsSpec:
    """선언적 소스 타임스탬프 추출."""

    field: str | None = None  # 점 표기 경로; None이면 header.stamp 자동 탐지
    format: str = "ros_time"  # 레지스트리 이름 (ros_time/epoch_seconds/... + 어댑터 별칭)


@dataclass
class TopicSpec:
    robot_id: str
    interface: str  # 전체 ROS2 토픽 이름, 예: /tb3/odom
    msg_type: str | None  # 고정 핀 또는 디스커버리에서 확정
    direction: Direction
    representation: Representation
    qos: QoSSpec
    qos_explicit: bool = False
    qos_explicit_fields: frozenset[str] = field(default_factory=frozenset)
    command_qos: QoSSpec | None = None
    sample: SampleSpec | None = None
    filter: dict[str, Any] | None = None
    selected_fields: list[str] | None = None
    stale_after_ms: int | None = None  # 신선도 워치독 — DDS lifespan과 별개
    source_ts: SourceTsSpec | None = None
    flexcontainer: dict[str, Any] | None = None  # {type, cnd, field_map} — FCNT 게이트 조건5
    role: str | None = None
    group: str | None = None
    leaf: str = ""  # oneM2M 리프 이름 (sanitize 적용)
    rel_path: str = ""  # 브랜치 상대 경로, 예: "<robot>/<leaf>"
    command: CommandSafety | None = None
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""  # --explain용 (어느 규칙이 이겼는지)

    def qos_for(self, direction: str) -> QoSSpec:
        """Return the configured QoS baseline for one topic direction."""
        if direction == "command" and self.command_qos is not None:
            return self.command_qos
        return self.qos

    def set_qos_for(self, direction: str, qos: QoSSpec) -> None:
        """Replace one direction's configured QoS baseline."""
        if direction == "command" and self.direction == "both":
            self.command_qos = qos
        else:
            self.qos = qos


@dataclass
class ServiceSpec:
    robot_id: str
    interface: str
    srv_type: str | None
    qos: QoSSpec | None = None  # None = rclpy 서비스 기본 QoS
    timeout_ms: int = 5000
    request_fields: list[str] | None = None
    response_fields: list[str] | None = None
    request_template: dict[str, Any] = field(default_factory=dict)
    leaf: str = ""
    rel_path: str = ""
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""


@dataclass
class ActionSpec:
    robot_id: str
    interface: str
    action_type: str | None
    qos: dict[str, QoSSpec] = field(default_factory=dict)  # 채널 -> QoSSpec
    feedback: FeedbackMode = "sampled"
    feedback_sample: SampleSpec | None = None
    goal_fields: list[str] | None = None
    feedback_fields: list[str] | None = None
    result_fields: list[str] | None = None
    goal_template: dict[str, Any] = field(default_factory=dict)
    timeout_ms: int = 0  # 0 = IPE 측 타임아웃 없음
    leaf: str = ""
    rel_path: str = ""
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""


class TopicIR(TypedDict):
    """ROS2 토픽 관측 IR (ROS2 -> oneM2M)."""

    interface_type: str          # 항상 "topic"
    robot_id: str                # 소유 로봇 (경로·상관 키 스코프)
    interface_name: str          # 예: "/tb3/odom"
    message_type: str            # 예: "nav_msgs/msg/Odometry"
    source_ts: float | None      # 메시지 헤더 stamp (epoch 초), 없을 수 있음
    ingest_ts: float             # IPE 수신 시각 (epoch 초) — QoS 판정의 기준
    seq: int                     # (robot, interface)별 단조 증가 시퀀스
    payload: dict[str, Any]      # 파싱된 메시지 필드 (rosidl dict 형태)
    metadata: dict[str, Any]     # source_node, qos, sim_time 플래그 등
