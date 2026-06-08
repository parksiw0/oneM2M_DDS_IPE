"""Intermediate Representation (IR) between Adapter and Core.

IR is the contract that platform-specific Adapter produces and
platform-independent Core consumes. Strongly typed via TypedDict.
"""

from __future__ import annotations

from typing import Any, TypedDict


class TopicIR(TypedDict):
    """IR for ROS2 Topic data."""

    interface_type: str          # always "topic"
    interface_name: str          # e.g. "/fmu/out/sensor_combined"
    message_type: str            # e.g. "px4_msgs/msg/SensorCombined"
    timestamp: float             # seconds since epoch
    payload: dict[str, Any]      # parsed message fields
    metadata: dict[str, Any]     # source_node, qos info, sequence, etc.


class ServiceIR(TypedDict):
    """IR for ROS2 Service request/response."""

    interface_type: str          # always "service"
    interface_name: str          # e.g. "/set_mode"
    service_type: str            # e.g. "std_srvs/srv/SetBool"
    correlation_id: str          # requestId for correlation
    direction: str               # "request" or "response"
    timestamp: float
    payload: dict[str, Any]


class ActionIR(TypedDict):
    """IR for ROS2 Action goal/feedback/result events."""

    interface_type: str          # always "action"
    interface_name: str          # e.g. "/navigate_to_pose"
    action_type: str             # e.g. "nav2_msgs/action/NavigateToPose"
    goal_id: str                 # UUID for correlation
    event: str                   # "goal_accepted" | "feedback" | "result" | "cancelled"
    timestamp: float
    payload: dict[str, Any]
