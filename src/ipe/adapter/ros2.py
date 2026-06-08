from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    LivelinessPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from rosidl_runtime_py.utilities import get_message

from ipe.adapter.base import AdapterBase
from ipe.adapter.transform import extract_timestamp, make_topic_ir, parse_message
from ipe.config.lookup import LookupTables
from ipe.ir import TopicIR

log = logging.getLogger(__name__)


RELIABILITY_MAP = {
    "RELIABLE": ReliabilityPolicy.RELIABLE,
    "BEST_EFFORT": ReliabilityPolicy.BEST_EFFORT,
}

DURABILITY_MAP = {
    "VOLATILE": DurabilityPolicy.VOLATILE,
    "TRANSIENT_LOCAL": DurabilityPolicy.TRANSIENT_LOCAL,
}

HISTORY_MAP = {
    "KEEP_LAST": HistoryPolicy.KEEP_LAST,
    "KEEP_ALL": HistoryPolicy.KEEP_ALL,
}

LIVELINESS_MAP = {
    "AUTOMATIC": LivelinessPolicy.AUTOMATIC,
    "MANUAL_BY_TOPIC": LivelinessPolicy.MANUAL_BY_TOPIC,
}


def build_qos_profile(qos_dict: dict[str, Any]) -> QoSProfile:
    return QoSProfile(
        reliability=RELIABILITY_MAP[qos_dict["reliability"]],
        durability=DURABILITY_MAP[qos_dict["durability"]],
        history=HISTORY_MAP.get(
            qos_dict.get("history", "KEEP_LAST"), HistoryPolicy.KEEP_LAST
        ),
        depth=qos_dict.get("depth", 10),
        liveliness=LIVELINESS_MAP.get(
            qos_dict.get("liveliness", "AUTOMATIC"), LivelinessPolicy.AUTOMATIC
        ),
    )


class GenericROS2Adapter(AdapterBase):
    def __init__(self, node: Node, tables: LookupTables) -> None:
        self.node = node
        self.tables = tables
        self.config = tables.config
        self.subscriptions: list[Any] = []
        self.on_topic_ir: Callable[[TopicIR], None] | None = None

    def discover(self) -> list[tuple[str, str]]:
        topics = self.node.get_topic_names_and_types()
        return [(name, types[0]) for name, types in topics if types]

    def bind_topics(self, on_topic_ir: Callable[[TopicIR], None]) -> None:
        self.on_topic_ir = on_topic_ir
        for topic_cfg in self.config.get("topics", []):
            self._bind_one(topic_cfg)

    def _bind_one(self, topic_cfg: dict[str, Any]) -> None:
        topic_name = topic_cfg["name"]
        msg_type_str = topic_cfg["message_type"]
        qos_name = topic_cfg["qos_profile"]
        qos_dict = self.tables.get_qos_profile(qos_name)
        if qos_dict is None:
            log.error("QoS profile '%s' not found for topic %s", qos_name, topic_name)
            return

        try:
            msg_class = get_message(msg_type_str)
        except Exception as e:
            log.error("Cannot load message type %s: %s", msg_type_str, e)
            return

        qos = build_qos_profile(qos_dict)

        def callback(msg: Any, cfg: dict[str, Any] = topic_cfg) -> None:
            self._on_message(msg, cfg)

        sub = self.node.create_subscription(msg_class, topic_name, callback, qos)
        self.subscriptions.append(sub)
        log.info("Subscribed: %s [%s]", topic_name, msg_type_str)

    def _on_message(self, msg: Any, topic_cfg: dict[str, Any]) -> None:
        try:
            payload = parse_message(msg)
            ts = extract_timestamp(
                payload,
                topic_cfg.get("timestamp_field"),
                topic_cfg.get("timestamp_format"),
            )
            ir = make_topic_ir(
                topic_name=topic_cfg["name"],
                message_type=topic_cfg["message_type"],
                payload=payload,
                timestamp=ts,
                metadata={
                    "source_qos": topic_cfg["qos_profile"],
                    "semantic_category": topic_cfg["semantic_category"],
                    "resource_alias": topic_cfg["resource_alias"],
                },
            )
            if self.on_topic_ir is not None:
                self.on_topic_ir(ir)
        except Exception as e:
            log.exception("Error processing message on %s: %s", topic_cfg["name"], e)

    def shutdown(self) -> None:
        for sub in self.subscriptions:
            self.node.destroy_subscription(sub)
        self.subscriptions.clear()
