from __future__ import annotations

from types import SimpleNamespace

from ipe.runtime.app import IPEApp
from ipe.runtime.type_support import type_support_requirement


def test_install_package_is_derived_from_ros_type(monkeypatch):
    monkeypatch.setenv("ROS_DISTRO", "humble")

    requirement = type_support_requirement(
        "topic", "robot", "/sensor_state",
        "turtlebot3_msgs/msg/SensorState",
    )

    assert requirement == {
        "kind": "topic",
        "robot": "robot",
        "interface": "/sensor_state",
        "rosType": "turtlebot3_msgs/msg/SensorState",
        "reason": "typeSupportUnavailable",
        "rosPackage": "turtlebot3_msgs",
        "installPackage": "ros-humble-turtlebot3-msgs",
    }


def test_ambiguous_type_has_no_hardcoded_package_hint(monkeypatch):
    monkeypatch.setenv("ROS_DISTRO", "humble")

    requirement = type_support_requirement(
        "service", "robot", "/unknown", None,
    )

    assert requirement["reason"] == "ambiguousType"
    assert "installPackage" not in requirement


def test_unavailable_binding_is_deferred_and_exposed_as_status(monkeypatch):
    monkeypatch.setenv("ROS_DISTRO", "humble")
    topic = SimpleNamespace(
        robot_id="robot-a",
        interface="/custom_data",
        msg_type="custom_robot_msgs/msg/State",
    )
    events = []

    class Harness:
        adapter = SimpleNamespace(type_available=lambda *_args: False)
        status_paths = {"provisioningStatus": "/status/provisioning"}
        _deferred_type_support = {}

        def emit_event(self, category, severity, payload):
            events.append((category, severity, payload))

        _publish_deferred_type_support_status = (
            IPEApp._publish_deferred_type_support_status
        )

    harness = Harness()
    rc = SimpleNamespace(topics=[topic], services=[], actions=[])

    IPEApp._defer_unloadable_types(harness, rc)

    assert rc.topics == []
    requirement = harness._deferred_type_support[
        ("topic", "robot-a", "/custom_data")
    ]
    assert requirement["installPackage"] == "ros-humble-custom-robot-msgs"
    assert events == [
        (
            "provisioningStatus",
            "warning",
            {"event": "typeSupportUnavailable", **requirement},
        )
    ]
