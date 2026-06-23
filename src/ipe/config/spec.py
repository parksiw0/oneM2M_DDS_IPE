"""해석 완료된 설정 스펙 — 런타임을 향한 계약.

YAML 설정은 schema.py가 검증하고, resolver가 발견된 ROS2 인터페이스와 합쳐
이 완전 해석된 스펙들로 바꾼다. 하류(어댑터·정책·라이프사이클)는 raw 설정
dict가 아니라 언제나 스펙만 소비한다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Direction = Literal["observe", "command", "both"]
Representation = Literal["historical", "latest", "both", "sampled"]
FeedbackMode = Literal["log", "latest", "sampled", "combined"]


# ---------------------------------------------------------------------------
# QoS (속성 8개, 완전 해석 — UPPERCASE 정준형, 시간 단위 ms)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 샘플링 / 필터 / 명령 안전장치
# ---------------------------------------------------------------------------

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
    max_age_ms: int = 5000               # 수신 신선도 게이트
    liveliness_lease_ms: int | None = None  # 로봇 측 IPE 사망 감지


@dataclass(frozen=True)
class SourceTsSpec:
    """선언적 소스 타임스탬프 추출."""

    field: str | None = None             # 점 표기 경로; None이면 header.stamp 자동 탐지
    format: str = "ros_time"             # 레지스트리 이름 (ros_time/epoch_seconds/... + 어댑터 별칭)


# ---------------------------------------------------------------------------
# robot 식별
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RobotSpec:
    id: str
    namespace: str = ""
    ae_per_robot: bool = False
    ae_name: str | None = None  # 명시적 AE 이름 오버라이드 (기본 C<id>)


# ---------------------------------------------------------------------------
# 인터페이스 스펙 (해석 완료, robot 스코프)
# ---------------------------------------------------------------------------

@dataclass
class TopicSpec:
    robot_id: str
    interface: str                       # 전체 ROS2 토픽 이름, 예: /tb3/odom
    msg_type: str | None                 # 고정 핀 또는 디스커버리에서 확정
    direction: Direction
    representation: Representation
    qos: QoSSpec
    sample: SampleSpec | None = None
    filter: dict[str, Any] | None = None
    selected_fields: list[str] | None = None
    stale_after_ms: int | None = None    # 신선도 워치독 — DDS lifespan과 별개
    source_ts: SourceTsSpec | None = None
    flexcontainer: dict[str, Any] | None = None  # {type, cnd, field_map} — FCNT 게이트 조건5
    role: str | None = None
    group: str | None = None
    leaf: str = ""                       # oneM2M 리프 이름 (sanitize 적용)
    rel_path: str = ""                   # 브랜치 상대 경로, 예: "<robot>/<leaf>"
    command: CommandSafety | None = None
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""                # --explain용 (어느 규칙이 이겼는지)


@dataclass
class ServiceSpec:
    robot_id: str
    interface: str
    srv_type: str | None
    qos: QoSSpec | None = None           # None = rclpy 서비스 기본 QoS
    timeout_ms: int = 5000
    request_fields: list[str] | None = None
    response_fields: list[str] | None = None
    request_template: dict[str, Any] = field(default_factory=dict)
    leaf: str = ""
    rel_path: str = ""
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""


# 액션 클라이언트 QoS 채널: 키는 이 이름들로 제한
ACTION_QOS_CHANNELS = ("goal_service", "result_service", "cancel_service",
                       "feedback_sub", "status_sub")


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
    timeout_ms: int = 0                  # 0 = IPE 측 타임아웃 없음
    leaf: str = ""
    rel_path: str = ""
    access_enabled: bool = False
    confirm: str = "auto"
    source_rule: str = ""


# ---------------------------------------------------------------------------
# 최상위 해석 결과
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MqttSpec:
    """MQTT 바인딩 설정 (protocol: mqtt). 브로커 접속 + 토픽/QoS/TLS."""

    host: str = "127.0.0.1"
    port: int = 1883
    client_id: str = "ros2-ipe"
    keepalive: int = 60
    qos: int = 1
    clean_session: bool = False
    topic_prefix: str = ""
    response_timeout_ms: int = 5000
    connect_timeout_ms: int = 10000
    max_payload: int = 65536
    tls: bool = False
    tls_ca: str | None = None
    tls_cert: str | None = None
    tls_key: str | None = None
    tls_insecure: bool = False
    username: str | None = None
    password: str | None = None


@dataclass
class CSESpec:
    endpoint: str                        # http 바인딩 베이스 URL (mqtt면 빈 문자열)
    cse_base: str                        # CSE 리소스 이름(CSE_BASE_NAME) — to 경로 루트
    ae_name: str
    protocol: str = "http"
    cse_id: str = ""                     # MQTT 토픽 receiver(CSE_BASE_RI) — mqtt 필수
    origin: str = "CAdmin"
    rvi: str = "3"
    poa: str = ""
    mqtt: MqttSpec | None = None


@dataclass
class ResolvedConfig:
    instance_id: str
    cse: CSESpec
    notification_host: str
    notification_port: int
    robots: dict[str, RobotSpec]
    qos_profiles: dict[str, QoSSpec]
    naming: dict[str, Any]
    discovery: dict[str, Any]
    defaults: dict[str, Any]
    policy: dict[str, Any]
    recovery: dict[str, Any]
    dispatch: dict[str, Any] = field(default_factory=lambda: {"drain_budget": 32})
    storage: dict[str, Any] = field(default_factory=dict)
    logging: dict[str, Any] = field(default_factory=dict)
    robots_strict: bool = False
    topics: list[TopicSpec] = field(default_factory=list)
    services: list[ServiceSpec] = field(default_factory=list)
    actions: list[ActionSpec] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
