from __future__ import annotations

from ipe.config.schema import (
    CONFIG_SCHEMA,
    QOS_PROFILE_SCHEMA,
    SAMPLING_SCHEMA,
    TOPIC_SCHEMA,
)


def test_top_level_keys():
    expected = {
        "platform",
        "cse",
        "qos_profiles",
        "topics",
        "services",
        "actions",
        "notification_server",
        "recovery",
    }
    assert set(CONFIG_SCHEMA.keys()) == expected


def test_topic_required_fields():
    required = {k for k, v in TOPIC_SCHEMA.items() if v.get("required")}
    assert required == {
        "name",
        "message_type",
        "qos_profile",
        "semantic_category",
        "resource_alias",
        "representation_policy",
    }


def test_qos_profile_required_fields():
    required = {k for k, v in QOS_PROFILE_SCHEMA.items() if v.get("required")}
    assert "reliability" in required
    assert "durability" in required


def test_semantic_category_allowed_values():
    assert set(TOPIC_SCHEMA["semantic_category"]["allowed"]) == {
        "sensors",
        "entities",
        "events",
    }


def test_representation_policy_allowed_values():
    assert set(TOPIC_SCHEMA["representation_policy"]["allowed"]) == {
        "historical_only",
        "latest_only",
        "historical_and_latest",
        "sampled",
    }


def test_sampling_interval_required():
    assert SAMPLING_SCHEMA["interval_sec"].get("required") is True
