from __future__ import annotations

from types import SimpleNamespace

import rclpy.action

from ipe.adapter.ros2 import GenericROS2Adapter


class _GraphNode:
    def get_name(self):
        return "ros2_onem2m_ipe"

    def get_namespace(self):
        return "/"

    def get_topic_names_and_types(self):
        return [
            ("/scan", ["sensor_msgs/msg/LaserScan"]),
            ("/cmd_vel", ["geometry_msgs/msg/Twist"]),
            ("/parameter_events", ["rcl_interfaces/msg/ParameterEvent"]),
        ]

    def get_publishers_info_by_topic(self, name):
        if name == "/scan":
            return [SimpleNamespace(node_name="lidar", node_namespace="/tb3")]
        if name == "/parameter_events":
            return [SimpleNamespace(node_name=self.get_name(), node_namespace="/")]
        return []

    def get_subscriptions_info_by_topic(self, name):
        if name == "/cmd_vel":
            return [SimpleNamespace(node_name="controller", node_namespace="/tb3")]
        return []

    def get_service_names_and_types(self):
        return [
            ("/ros2_onem2m_ipe/get_parameters", ["rcl_interfaces/srv/GetParameters"]),
            ("/reset", ["std_srvs/srv/Trigger"]),
        ]

    def get_service_names_and_types_by_node(self, node_name, _node_ns):
        if node_name == self.get_name():
            return [("/ros2_onem2m_ipe/get_parameters",
                     ["rcl_interfaces/srv/GetParameters"])]
        return [("/reset", ["std_srvs/srv/Trigger"])]

    def get_node_names_and_namespaces(self):
        return [(self.get_name(), "/"), ("controller", "/tb3")]


def test_snapshot_reports_remote_direction_and_robot_owners(monkeypatch):
    monkeypatch.setattr(
        rclpy.action,
        "get_action_names_and_types",
        lambda _node: [
            ("/navigate", ["nav2_msgs/action/NavigateToPose"]),
            ("/client_only", ["example_interfaces/action/Fibonacci"]),
        ],
    )
    monkeypatch.setattr(
        rclpy.action,
        "get_action_server_names_and_types_by_node",
        lambda _node, node_name, _node_ns: (
            [("/navigate", ["nav2_msgs/action/NavigateToPose"])]
            if node_name == "controller" else []
        ),
    )
    adapter = GenericROS2Adapter(_GraphNode(), lambda _ir: None, lambda *_args: None)

    snapshot = adapter.snapshot()

    assert snapshot["topic_directions"] == {
        "/scan": "observe",
        "/cmd_vel": "command",
    }
    assert snapshot["topics"] == [
        ("/scan", ["sensor_msgs/msg/LaserScan"]),
        ("/cmd_vel", ["geometry_msgs/msg/Twist"]),
    ]
    assert snapshot["services"] == [("/reset", ["std_srvs/srv/Trigger"])]
    assert snapshot["actions"] == [
        ("/navigate", ["nav2_msgs/action/NavigateToPose"]),
    ]
    assert snapshot["owners"] == {
        "topics": {"/scan": ["/tb3"], "/cmd_vel": ["/tb3"]},
        "services": {"/reset": ["/tb3"]},
        "actions": {"/navigate": ["/tb3"]},
    }
