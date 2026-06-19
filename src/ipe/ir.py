"""Adapter와 Core 사이의 중간 표현(IR) 계약.

모든 IR은 robot_id를 실어 멀티 로봇 경로/상관 키 충돌을 막고, 인터페이스별
단조 seq를 가진다 — CIN 순서의 기준은 CSE 생성 순서이지 페이로드 안의
벽시계 타임스탬프가 아니다.
"""

from __future__ import annotations

from typing import Any, TypedDict


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
