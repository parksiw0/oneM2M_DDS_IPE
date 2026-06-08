"""Cerberus schema for IPE configuration YAML.

Single source of truth for what a valid config looks like.
Used by loader.validate_config().
"""

from __future__ import annotations

from typing import Any

QOS_PROFILE_SCHEMA: dict[str, Any] = {
    "reliability": {
        "type": "string",
        "allowed": ["RELIABLE", "BEST_EFFORT"],
        "required": True,
    },
    "durability": {
        "type": "string",
        "allowed": ["VOLATILE", "TRANSIENT_LOCAL"],
        "required": True,
    },
    "history": {
        "type": "string",
        "allowed": ["KEEP_LAST", "KEEP_ALL"],
        "default": "KEEP_LAST",
    },
    "depth": {
        "type": "integer",
        "min": 1,
        "default": 10,
    },
    "deadline_ms": {"type": "integer", "min": 0, "required": False},
    "liveliness": {
        "type": "string",
        "allowed": ["AUTOMATIC", "MANUAL_BY_TOPIC"],
        "default": "AUTOMATIC",
    },
}

SAMPLING_SCHEMA: dict[str, Any] = {
    "interval_sec": {"type": "float", "min": 0.0, "required": True},
    "min_change": {"type": "float", "min": 0.0, "required": False},
}

FLEXCONTAINER_SCHEMA: dict[str, Any] = {
    "type": {
        "type": "string",
        "required": True,
        "regex": r"^[A-Za-z0-9_]+:[A-Za-z0-9_]+$",
    },
    "cnd": {"type": "string", "required": True, "empty": False},
    "field_map": {
        "type": "dict",
        "required": True,
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "string"},
        "minlength": 1,
    },
}

TOPIC_SCHEMA: dict[str, Any] = {
    "name": {"type": "string", "required": True, "empty": False},
    "message_type": {
        "type": "string",
        "required": True,
        "regex": r"^[a-z][a-z0-9_]*/msg/[A-Z][A-Za-z0-9]*$",
    },
    "qos_profile": {"type": "string", "required": True},
    "semantic_category": {
        "type": "string",
        "allowed": ["sensors", "entities", "events"],
        "required": True,
    },
    "resource_alias": {"type": "string", "required": True, "empty": False},
    "representation_policy": {
        "type": "string",
        "allowed": [
            "historical_only",
            "latest_only",
            "historical_and_latest",
            "sampled",
        ],
        "required": True,
    },
    "sampling": {"type": "dict", "schema": SAMPLING_SCHEMA, "required": False},
    "selected_fields": {
        "type": "list",
        "schema": {"type": "string"},
        "required": False,
    },
    "timestamp_field": {"type": "string", "required": False},
    "timestamp_format": {
        "type": "string",
        "allowed": ["px4_microseconds", "ros_time", "epoch_seconds"],
        "required": False,
    },
    "frame_convention": {
        "type": "string",
        "allowed": ["NED", "ENU", "FRD", "FLU"],
        "required": False,
    },
    "flexcontainer": {
        "type": "dict",
        "schema": FLEXCONTAINER_SCHEMA,
        "required": False,
    },
}

CONFIG_SCHEMA: dict[str, Any] = {
    "platform": {
        "type": "dict",
        "required": True,
        "schema": {
            "name": {"type": "string", "required": True, "empty": False},
            "description": {"type": "string", "required": False},
            "message_packages": {
                "type": "list",
                "schema": {"type": "string"},
                "required": True,
                "minlength": 1,
            },
        },
    },
    "cse": {
        "type": "dict",
        "required": True,
        "schema": {
            "endpoint": {
                "type": "string",
                "required": True,
                "regex": r"^https?://.+",
            },
            "cse_base": {"type": "string", "required": True, "empty": False},
            "ae_name": {"type": "string", "required": True, "empty": False},
            "protocol": {
                "type": "string",
                "allowed": ["http", "mqtt"],
                "default": "http",
            },
            "origin": {"type": "string", "default": "admin"},
            "poa": {"type": "string", "required": False},
        },
    },
    "notification_server": {
        "type": "dict",
        "required": False,
        "schema": {
            "host": {"type": "string", "default": "0.0.0.0"},
            "port": {
                "type": "integer",
                "min": 1,
                "max": 65535,
                "default": 5050,
            },
        },
    },
    "qos_profiles": {
        "type": "dict",
        "required": True,
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "dict", "schema": QOS_PROFILE_SCHEMA},
        "minlength": 1,
    },
    "topics": {
        "type": "list",
        "schema": {"type": "dict", "schema": TOPIC_SCHEMA},
        "required": False,
        "default": [],
    },
    "services": {
        "type": "list",
        "required": False,
        "default": [],
    },
    "actions": {
        "type": "list",
        "required": False,
        "default": [],
    },
    "recovery": {
        "type": "dict",
        "required": False,
        "schema": {
            "retry_count": {"type": "integer", "min": 0, "default": 3},
            "retry_delay_sec": {"type": "float", "min": 0.0, "default": 1.0},
            "on_failure": {
                "type": "string",
                "allowed": ["skip", "retry", "rebind", "reprovision"],
                "default": "skip",
            },
        },
        "default": {},
    },
}
