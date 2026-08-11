"""설정 파일 없이 실행할 때 사용하는 범용 런타임 설정.

ROS 2 인터페이스 목록은 의도적으로 포함하지 않는다. topic/service/action은
DDS Domain 참여 뒤 얻은 ROS graph snapshot만으로 해석한다.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


def discovery_runtime_config(args: Any, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    values = os.environ if env is None else env

    def arg_or_env(name: str, key: str, default: Any) -> Any:
        value = getattr(args, name, None)
        return value if value not in (None, "") else values.get(key, default)

    endpoint = arg_or_env("cse_endpoint", "IPE_CSE_ENDPOINT", "http://127.0.0.1:3000")
    cse_base = arg_or_env("cse_base", "IPE_CSE_BASE", "TinyIoT")
    ae_name = arg_or_env("ae_name", "IPE_AE_NAME", "ros2-ipe")
    robot_id = arg_or_env("robot_id", "IPE_ROBOT_ID", "robot")
    robot_namespace = arg_or_env("robot_namespace", "IPE_ROBOT_NAMESPACE", "")
    refresh_sec = float(arg_or_env("refresh_sec", "IPE_REFRESH_SEC", 5.0))
    domain_id = int(arg_or_env("domain_id", "ROS_DOMAIN_ID", 0))

    return {
        "ipe": {"instance_id": arg_or_env("instance_id", "IPE_INSTANCE_ID", "ros2-ipe")},
        "cse": {
            "endpoint": endpoint,
            "cse_base": cse_base,
            "ae_name": ae_name,
            "origin": values.get("IPE_CSE_ORIGIN", "CAdmin"),
            "rvi": values.get("IPE_RVI", "3"),
        },
        "robots": [{"id": robot_id, "namespace": robot_namespace}],
        "discovery": {
            "mode": "auto-expose",
            "domain_id": domain_id,
            "allow": ["/**"],
            "deny": [],
            "refresh_sec": refresh_sec,
            "graph_settle_timeout_sec": float(values.get("IPE_GRAPH_TIMEOUT_SEC", "10")),
            "graph_stable_polls": int(values.get("IPE_GRAPH_STABLE_POLLS", "2")),
            "graph_poll_sec": float(values.get("IPE_GRAPH_POLL_SEC", "0.5")),
        },
        # 인터페이스별 QoS 핀은 없다. 이 값은 endpoint 정보가 아직 없는 경우의
        # fail-safe이며 실제 바인딩 때 offered/requested QoS와 reconcile된다.
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
            "service": {"access": {"enabled": True}},
            "action": {"access": {"enabled": True}},
        },
        # interface마다 독립 subtree를 보장해 제거 reconcile이 인접 interface를
        # 함께 지우지 않도록 한다(/foo와 /foo/bar prefix 충돌 방지).
        "naming": {"path_style": "flat", "sanitize": "_"},
        "bridge": {"topics": [], "services": [], "actions": []},
        "storage": {"state_db": values.get("IPE_STATE_DB", "ipe_state.db")},
    }


__all__ = ["discovery_runtime_config"]
