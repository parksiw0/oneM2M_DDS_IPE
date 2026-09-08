"""Compose deployment settings layered over the repository's config.py.

The image installs this file as /etc/ipe/config.py and the original configuration
as /opt/ipe/base_config.py. Native execution continues to load config.py directly.
"""

from __future__ import annotations

import os
from copy import deepcopy

import base_config as base

USE_DOCKER = False
DOCKER_IMAGE = base.DOCKER_IMAGE
RUN_MODE = os.environ.get("IPE_RUN_MODE", base.RUN_MODE)
RESET_AE = base.RESET_AE
TIMEZONE = base.TIMEZONE
OBSERVE_ONLY = base.OBSERVE_ONLY
# Compose keeps IPE state in its own SQLite volume. The CSE's database is separate.
POSTGRES_PASSWORD = ""
CONFIG = deepcopy(base.CONFIG)
CONFIG["storage"].update(backend="sqlite", state_db="/var/lib/ipe/state.db")
CONFIG["cse"]["endpoint"] = os.environ.get(
    "IPE_CSE_ENDPOINT", CONFIG["cse"]["endpoint"]
)
CONFIG["cse"]["poa"] = os.environ.get(
    "IPE_CSE_POA", CONFIG["cse"].get("poa", "http://127.0.0.1:5050")
)

for key, variable, convert in (
    ("domain_id", "IPE_ROS_DOMAIN_ID", int),
    ("rmw_implementation", "IPE_RMW_IMPLEMENTATION", str),
    ("ros_peer", "IPE_ROS_PEER", str),
    ("graph_settle_timeout_sec", "IPE_GRAPH_TIMEOUT_SEC", float),
):
    if variable in os.environ:
        CONFIG["discovery"][key] = convert(os.environ[variable])

# Opt-in TurtleBot demo: keep the sensor view small and declare the command
# contract explicitly, including its existing IPE safety gates.
if os.environ.get("IPE_DEMO", "0") == "1":
    CONFIG["robots"] = [{"id": "robot", "namespace": ""}]
    CONFIG["discovery"].update(allow=["/odom", "/cmd_vel"], deny=[])
    CONFIG["naming"] = {"path_style": "flat", "sanitize": "_"}
    CONFIG["bridge"] = {
        "topics": [
            {
                "name": "/odom", "type": "nav_msgs/msg/Odometry",
                "direction": "observe", "representation": "sampled",
                "sample": {"min_interval_ms": 1000},
                "qos": {"history": "KEEP_LAST", "depth": 5},
            },
            {
                "name": "/cmd_vel", "type": "geometry_msgs/msg/Twist",
                "direction": "both", "representation": "historical",
                "qos": {"reliability": "RELIABLE", "history": "KEEP_LAST", "depth": 5},
                "access": {"enabled": not OBSERVE_ONLY, "confirm": "auto"},
                "command": {
                    "max_age_ms": 2000,
                    "watchdog_ms": 1000,
                    "clamp": {"linear.x": [-0.1, 0.1], "angular.z": [-0.5, 0.5]},
                },
            },
        ],
        "services": [], "actions": [],
    }
