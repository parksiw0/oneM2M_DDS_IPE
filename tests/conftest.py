from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def minimal_config() -> dict[str, Any]:
    return {
        "platform": {
            "name": "test",
            "message_packages": ["px4_msgs"],
        },
        "cse": {
            "endpoint": "http://localhost:3000",
            "cse_base": "TinyIoT",
            "ae_name": "ros2-ipe",
        },
        "qos_profiles": {
            "default": {
                "reliability": "BEST_EFFORT",
                "durability": "TRANSIENT_LOCAL",
            }
        },
        "topics": [
            {
                "name": "/test/topic",
                "message_type": "px4_msgs/msg/SensorCombined",
                "qos_profile": "default",
                "semantic_category": "sensors",
                "resource_alias": "test_imu",
                "representation_policy": "latest_only",
            }
        ],
    }


@pytest.fixture
def px4_config_path() -> Path:
    return Path(__file__).parent.parent / "config" / "px4.yaml"
