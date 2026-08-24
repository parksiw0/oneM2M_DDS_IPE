"""Build runtime defaults for discovery-driven operation."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from typing import Any

from ipe.config.identity import sanitize_segment
from ipe.config.loader import ConfigError


def _as_float(key: str, value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} must be a number, got {value!r}") from exc
    if not math.isfinite(parsed):
        raise ConfigError(f"{key} must be finite, got {value!r}")
    return parsed


def _as_int(key: str, value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} must be an integer, got {value!r}") from exc


def discovery_runtime_config(args: Any, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return runtime settings without declaring any ROS interfaces.

    The live graph supplies interfaces and QoS. Control interfaces are enabled
    unless the operator selects observe-only mode.
    """
    values = os.environ if env is None else env

    def arg_or_env(name: str, key: str, default: Any) -> Any:
        value = getattr(args, name, None)
        return value if value not in (None, "") else values.get(key, default)

    def env_bool(key: str, default: bool = False) -> bool:
        value = values.get(key)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    protocol = values.get("IPE_CSE_PROTOCOL", "http").strip().lower()
    cse_base = arg_or_env("cse_base", "IPE_CSE_BASE", "TinyIoT")
    cse_timezone = arg_or_env("cse_timezone", "IPE_CSE_TIMEZONE", "local")
    ae_name = arg_or_env("ae_name", "IPE_AE_NAME", "ros2-ipe")
    origin = values.get("IPE_CSE_ORIGIN") or f"C{sanitize_segment(ae_name)}"
    robot_id = arg_or_env("robot_id", "IPE_ROBOT_ID", "robot")
    robot_namespace = arg_or_env("robot_namespace", "IPE_ROBOT_NAMESPACE", "")
    refresh_sec = _as_float(
        "IPE_REFRESH_SEC", arg_or_env("refresh_sec", "IPE_REFRESH_SEC", 5.0)
    )
    domain_id = _as_int("ROS_DOMAIN_ID", arg_or_env("domain_id", "ROS_DOMAIN_ID", 0))
    control_enabled = not (
        bool(getattr(args, "observe_only", False)) or env_bool("IPE_OBSERVE_ONLY")
    )

    cse = {
        "cse_base": cse_base,
        "timezone": cse_timezone,
        "ae_name": ae_name,
        "origin": origin,
        "rvi": values.get("IPE_RVI", "3"),
        "protocol": protocol,
    }
    if protocol == "http":
        cse["endpoint"] = arg_or_env(
            "cse_endpoint", "IPE_CSE_ENDPOINT", "http://127.0.0.1:3000"
        )
    elif protocol == "mqtt":
        cse["cse_id"] = values.get("IPE_CSE_ID", "")
        cse["mqtt"] = {
            "host": values.get("IPE_MQTT_HOST", "127.0.0.1"),
            "port": _as_int("IPE_MQTT_PORT", values.get("IPE_MQTT_PORT", "1883")),
            "client_id": values.get("IPE_MQTT_CLIENT_ID", str(ae_name)),
            "qos": _as_int("IPE_MQTT_QOS", values.get("IPE_MQTT_QOS", "1")),
            "username": values.get("IPE_MQTT_USERNAME"),
            "password": values.get("IPE_MQTT_PASSWORD"),
            "tls": env_bool("IPE_MQTT_TLS"),
            "tls_ca": values.get("IPE_MQTT_TLS_CA"),
            "tls_cert": values.get("IPE_MQTT_TLS_CERT"),
            "tls_key": values.get("IPE_MQTT_TLS_KEY"),
            "tls_insecure": env_bool("IPE_MQTT_TLS_INSECURE"),
        }

    return {
        "ipe": {"instance_id": arg_or_env("instance_id", "IPE_INSTANCE_ID", "ros2-ipe")},
        "cse": cse,
        "robots": [{"id": robot_id, "namespace": robot_namespace}],
        "discovery": {
            "mode": "auto-expose",
            "domain_id": domain_id,
            "allow": ["/**"],
            "deny": [],
            "refresh_sec": refresh_sec,
            "graph_settle_timeout_sec": _as_float(
                "IPE_GRAPH_TIMEOUT_SEC", values.get("IPE_GRAPH_TIMEOUT_SEC", "10")
            ),
            "graph_stable_polls": _as_int(
                "IPE_GRAPH_STABLE_POLLS", values.get("IPE_GRAPH_STABLE_POLLS", "2")
            ),
            "graph_poll_sec": _as_float(
                "IPE_GRAPH_POLL_SEC", values.get("IPE_GRAPH_POLL_SEC", "0.5")
            ),
        },
        "qos_profiles": {
            "sensor_data": {
                "reliability": "BEST_EFFORT",
                "durability": "VOLATILE",
                "history": "KEEP_LAST",
                "depth": 5,
            },
        },
        "defaults": {
            "topic_observe": {"representation": "latest"},
            "topic_command": {"access": {"enabled": control_enabled}},
            "service": {"access": {"enabled": control_enabled}},
            "action": {"access": {"enabled": control_enabled}},
        },
        "naming": {"path_style": "flat", "sanitize": "_"},
        "bridge": {"topics": [], "services": [], "actions": []},
        "storage": {"state_db": values.get("IPE_STATE_DB", "ipe_state.db")},
    }


__all__ = ["discovery_runtime_config"]
