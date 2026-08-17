"""ROS 2 Type Support 누락 진단 정보 생성."""

from __future__ import annotations

import os
from typing import Any


def type_support_requirement(
    binding_kind: str,
    robot_id: str,
    interface: str,
    ros_type: str | None,
) -> dict[str, Any]:
    """누락된 Type Support를 운영자가 조치할 수 있는 형태로 설명한다."""
    item: dict[str, Any] = {
        "kind": binding_kind,
        "robot": robot_id,
        "interface": interface,
        "rosType": ros_type,
        "reason": "typeSupportUnavailable" if ros_type else "ambiguousType",
    }
    if not ros_type:
        return item

    ros_package = ros_type.split("/", 1)[0]
    item["rosPackage"] = ros_package
    distro = os.environ.get("ROS_DISTRO", "").strip().lower()
    if distro:
        item["installPackage"] = (
            f"ros-{distro}-{ros_package.lower().replace('_', '-')}"
        )
    return item
