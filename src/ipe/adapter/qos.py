"""ROS-specific QoSProfile construction and compatibility checks."""

from __future__ import annotations

from typing import Any

from ipe.qos.models import QoSSpec


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
