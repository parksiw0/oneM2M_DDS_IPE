"""설정 비의존 ROS graph 계획과 기동 readiness 검증."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.config.runtime_config import discovery_runtime_config
from ipe.runtime.discovery import GraphNotReady, await_graph_convergence
from ipe.runtime.lifecycle import IPEPhase, IPEState, Lifecycle


def _args(**overrides):
    base = {
        "cse_endpoint": None, "cse_base": None, "ae_name": None,
        "instance_id": None, "robot_id": None, "robot_namespace": None,
        "refresh_sec": None, "domain_id": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_configless_plan_uses_discovered_graph_and_endpoint_direction():
    raw = validate_config(discovery_runtime_config(_args(), {}))
    assert resolve(raw).topics == []

    snap = {
        "topics": [
            ("/scan", ["sensor_msgs/msg/LaserScan"]),
            ("/cmd_vel", ["geometry_msgs/msg/Twist"]),
            ("/rosout", ["rcl_interfaces/msg/Log"]),
        ],
        "services": [("/reset", ["std_srvs/srv/Trigger"])],
        "actions": [("/navigate", ["nav2_msgs/action/NavigateToPose"])],
        "topic_directions": {
            "/scan": "observe", "/cmd_vel": "command", "/rosout": "observe",
        },
    }
    rc = resolve(raw, discovered=snap)
    topics = {x.interface: x for x in rc.topics}

    assert set(topics) == {"/scan", "/cmd_vel"}  # ROS infrastructure topic은 내장 deny
    assert topics["/scan"].direction == "observe"
    assert topics["/cmd_vel"].direction == "command"
    assert topics["/cmd_vel"].access_enabled is True
    assert topics["/scan"].rel_path == "scan"
    assert [x.interface for x in rc.services] == ["/reset"]
    assert [x.interface for x in rc.actions] == ["/navigate"]


def test_graph_must_be_nonempty_and_stable():
    empty = {"topics": [], "services": [], "actions": []}
    ready = {"topics": [("/scan", ["sensor_msgs/msg/LaserScan"])],
             "services": [], "actions": [],
             "topic_directions": {"/scan": "observe"}}
    snapshots = iter([empty, ready, ready])

    state = await_graph_convergence(lambda: next(snapshots), timeout_sec=1,
                                    stable_polls=2, poll_sec=0.001)
    assert state.snapshot == ready
    assert state.samples == 3

    with pytest.raises(GraphNotReady):
        await_graph_convergence(lambda: empty, timeout_sec=0.002,
                                stable_polls=2, poll_sec=0.001)


def test_endpoint_namespace_creates_a_robot_boundary_without_yaml():
    raw = validate_config(discovery_runtime_config(_args(), {}))
    snap = {
        "topics": [("/tb3/scan", ["sensor_msgs/msg/LaserScan"])],
        "services": [], "actions": [],
        "topic_directions": {"/tb3/scan": "observe"},
        "owners": {"topics": {"/tb3/scan": ["/tb3"]}},
    }

    rc = resolve(raw, discovered=snap)

    assert set(rc.robots) == {"tb3"}
    assert rc.topics[0].robot_id == "tb3"
    assert rc.topics[0].rel_path == "scan"


def test_bare_dds_placeholder_namespace_uses_fallback_robot():
    raw = validate_config(
        discovery_runtime_config(_args(robot_id="px4_sitl"), {})
    )
    snap = {
        "topics": [("/fmu/out/vehicle_status", ["px4_msgs/msg/VehicleStatus"])],
        "services": [], "actions": [],
        "topic_directions": {"/fmu/out/vehicle_status": "observe"},
        "owners": {
            "topics": {
                "/fmu/out/vehicle_status": ["_CREATED_BY_BARE_DDS_APP_"],
            },
        },
    }

    rc = resolve(raw, discovered=snap)

    assert set(rc.robots) == {"px4_sitl"}
    assert rc.topics[0].robot_id == "px4_sitl"


@pytest.mark.parametrize(
    ("robot_id", "prefixed_interface", "expected_leaf"),
    [
        ("tb3", "/tb3/mock_scan", "mock_scan"),
        ("warehouse_bot_7", "/warehouse_bot_7/camera/status", "camera__status"),
    ],
)
def test_fallback_robot_prefix_is_not_duplicated_in_resource_name(
    robot_id, prefixed_interface, expected_leaf,
):
    raw = validate_config(discovery_runtime_config(_args(robot_id=robot_id), {}))
    snap = {
        "topics": [
            (prefixed_interface, ["std_msgs/msg/String"]),
            ("/scan", ["sensor_msgs/msg/LaserScan"]),
        ],
        "services": [], "actions": [],
        "topic_directions": {
            prefixed_interface: "observe",
            "/scan": "observe",
        },
        "owners": {
            "topics": {
                prefixed_interface: ["/"],
                "/scan": ["/"],
            },
        },
    }

    rc = resolve(raw, discovered=snap)
    topics = {topic.interface: topic for topic in rc.topics}

    assert topics[prefixed_interface].robot_id == robot_id
    assert topics[prefixed_interface].rel_path == expected_leaf
    assert topics["/scan"].rel_path == "scan"


def test_interface_namespace_is_robot_fallback_when_rmw_hides_node_owner():
    raw = validate_config(discovery_runtime_config(_args(), {}))
    snap = {
        "topics": [
            ("/warehouse_bot_7/camera/status", ["std_msgs/msg/String"]),
        ],
        "services": [], "actions": [],
        "topic_directions": {
            "/warehouse_bot_7/camera/status": "observe",
        },
        "owners": {
            "topics": {
                "/warehouse_bot_7/camera/status": [],
            },
        },
    }

    rc = resolve(raw, discovered=snap)

    assert set(rc.robots) == {"warehouse_bot_7"}
    assert rc.topics[0].robot_id == "warehouse_bot_7"
    assert rc.topics[0].rel_path == "camera__status"


def test_action_server_namespace_creates_the_same_robot_boundary():
    raw = validate_config(discovery_runtime_config(_args(), {}))
    snap = {
        "topics": [], "services": [],
        "actions": [("/tb3/navigate", ["nav2_msgs/action/NavigateToPose"])],
        "owners": {"actions": {"/tb3/navigate": ["/tb3"]}},
    }

    rc = resolve(raw, discovered=snap)

    assert "tb3" in rc.robots
    assert rc.actions[0].robot_id == "tb3"
    assert rc.actions[0].rel_path == "navigate"


def test_graph_fingerprint_includes_robot_ownership():
    first = {
        "topics": [("/scan", ["sensor_msgs/msg/LaserScan"])],
        "services": [], "actions": [],
        "topic_directions": {"/scan": "observe"},
        "owners": {"topics": {"/scan": ["/robot_a"]}},
    }
    second = {
        **first,
        "owners": {"topics": {"/scan": ["/robot_b"]}},
    }

    from ipe.runtime.discovery import graph_fingerprint

    assert graph_fingerprint(first) != graph_fingerprint(second)


def test_lifecycle_persists_closed_state_vocabulary():
    class MemoryState:
        def __init__(self):
            self.values = {}

        def set_kv(self, key, value):
            self.values[key] = value

    state = MemoryState()
    lifecycle = Lifecycle(state)
    lifecycle.set(IPEState.PREPARING, IPEPhase.GRAPH_DISCOVERED,
                  next_generation=True)

    saved = state.values["ipe_lifecycle"]
    assert saved["ipe_state"] == "PREPARING"
    assert saved["ipe_phase"] == "GRAPH_DISCOVERED"
    assert saved["generation"] == 1
