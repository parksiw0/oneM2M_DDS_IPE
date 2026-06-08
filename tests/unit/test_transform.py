from __future__ import annotations

from ipe.adapter.transform import (
    _get_nested,
    extract_timestamp,
    make_topic_ir,
)


class TestGetNested:
    def test_simple_key(self):
        assert _get_nested({"a": 1}, "a") == 1

    def test_dot_notation(self):
        assert _get_nested({"a": {"b": 2}}, "a.b") == 2

    def test_three_levels(self):
        assert _get_nested({"a": {"b": {"c": 3}}}, "a.b.c") == 3

    def test_missing_top_key(self):
        assert _get_nested({"a": 1}, "b") is None

    def test_missing_nested_key(self):
        assert _get_nested({"a": {"b": 2}}, "a.c") is None

    def test_non_dict_intermediate(self):
        assert _get_nested({"a": "string_value"}, "a.b") is None


class TestExtractTimestamp:
    def test_px4_microseconds(self):
        payload = {"timestamp": 1_000_000_000}
        assert extract_timestamp(payload, "timestamp", "px4_microseconds") == 1000.0

    def test_epoch_seconds(self):
        payload = {"ts": 1234.5}
        assert extract_timestamp(payload, "ts", "epoch_seconds") == 1234.5

    def test_ros_time_dict(self):
        payload = {"header": {"stamp": {"sec": 100, "nanosec": 500_000_000}}}
        result = extract_timestamp(payload, "header.stamp", "ros_time")
        assert result == 100.5

    def test_no_field_specified(self):
        assert extract_timestamp({"a": 1}, None, None) is None

    def test_missing_field(self):
        assert extract_timestamp({"a": 1}, "missing", "px4_microseconds") is None

    def test_unknown_format(self):
        assert extract_timestamp({"a": 1}, "a", "unknown_format") is None


class TestMakeTopicIR:
    def test_basic(self):
        ir = make_topic_ir("/test", "px4_msgs/msg/X", {"a": 1}, timestamp=123.4)
        assert ir["interface_type"] == "topic"
        assert ir["interface_name"] == "/test"
        assert ir["message_type"] == "px4_msgs/msg/X"
        assert ir["timestamp"] == 123.4
        assert ir["payload"] == {"a": 1}
        assert ir["metadata"] == {}

    def test_with_metadata(self):
        ir = make_topic_ir(
            "/t", "X/msg/Y", {}, timestamp=0.0, metadata={"qos": "be"}
        )
        assert ir["metadata"] == {"qos": "be"}

    def test_auto_timestamp(self):
        ir = make_topic_ir("/t", "X/msg/Y", {})
        assert ir["timestamp"] > 0
