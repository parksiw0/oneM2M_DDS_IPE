from __future__ import annotations

import rclpy
from rclpy.node import Node


def create_node(name: str = "ros2_onem2m_ipe") -> Node:
    if not rclpy.ok():
        rclpy.init()
    return Node(name)


def spin(node: Node) -> None:
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
