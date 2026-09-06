"""IPE 실행 설정. 값을 수정한 뒤 IPE를 다시 시작하면 적용됩니다.

실행 인자나 환경변수 대신 이 파일에서 모든 실행 파라미터를 정합니다.
토픽·서비스·액션 목록과 endpoint QoS는 실행 중인 ROS graph에서 발견합니다.
"""

from pathlib import Path

USE_DOCKER = False
DOCKER_IMAGE = "ipe:humble"
RUN_MODE = "run"                 # run / discover / explain / bootstrap
RESET_AE = False                 # True: 기동할 때 기존 AE 삭제
TIMEZONE = "Asia/Seoul"
OBSERVE_ONLY = False             # True: 관찰만 허용, 제어 비활성화

# 기존 비밀번호 파일을 사용합니다. 파일 경로나 비밀번호 변경도 여기서 관리합니다.
POSTGRES_PASSWORD_FILE = Path(__file__).with_name(".ipe-postgres-password")
POSTGRES_PASSWORD = (
    POSTGRES_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    if POSTGRES_PASSWORD_FILE.is_file() else ""
)

CONFIG = {
    "ipe": {"instance_id": "ros2-ipe"},

    # oneM2M CSE 연결. MQTT 사용 시 protocol과 cse_id를 함께 설정합니다.
    "cse": {
        "protocol": "http",                         # http / mqtt
        "endpoint": "http://127.0.0.1:3000",
        "cse_base": "TinyIoT",
        "cse_id": "",                               # MQTT CSE-ID
        "ae_name": "ros2-ipe",
        "origin": "Cros2-ipe",
        "rvi": "3",
        "http_timeout_sec": 5.0,
        "http_max_payload": 65536,                 # 인코딩된 요청 본문 바이트
        "timezone": "local",                        # 예: Asia/Seoul
        "mqtt": {
            "host": "127.0.0.1",
            "port": 1883,
            "client_id": "ros2-ipe",
            "qos": 1,
            "keepalive": 60,
            "clean_session": False,
            "topic_prefix": "",
            "max_payload": 65536,
            "response_timeout_ms": 5000,
            "connect_timeout_ms": 10000,
            "username": None,
            "password": None,
            "tls": False,
            "tls_ca": None,
            "tls_cert": None,
            "tls_key": None,
            "tls_insecure": False,
        },
    },
    "notification_server": {"host": "0.0.0.0", "port": 5050},

    # ROS namespace가 없는 graph에 사용할 로봇 이름입니다.
    "robots": [{"id": "robot", "namespace": ""}],
    "robots_strict": False,
    "discovery": {
        "mode": "auto-expose",
        "domain_id": 30,                             # 현재 TurtleBot3와 동일한 ROS_DOMAIN_ID
        "ros_peer": "",                             # Cyclone DDS peer IP, 빈 값은 자동 발견
        "rmw_implementation": "",                   # 빈 값은 endpoint vendor에서 자동 선택
        "allow": ["/**"],
        "deny": [],
        "refresh_sec": 5.0,
        "vanish_grace_polls": 2,
        "graph_settle_timeout_sec": 10.0,
        "graph_stable_polls": 2,
        "graph_poll_sec": 0.5,
    },

    # 상태 저장소. SQLite 사용 시 backend를 sqlite로 바꾸면 됩니다.
    # PostgreSQL 비밀번호는 위의 POSTGRES_PASSWORD를 사용합니다.
    "storage": {
        "backend": "postgresql",                    # sqlite / postgresql
        "state_db": "ipe_state.db",                 # SQLite 파일 경로
        "dsn": "postgresql://ipeuser@127.0.0.1:5432/ipedb",
        "schema": "ipe_ros2_ipe",
        "pool_min_size": 1,
        "pool_max_size": 8,
        "max_spool_entries": 10000,
        "max_spool_mb": 64,
    },

    # CSE 전송·재시도·큐 용량.
    "recovery": {
        "outbound_workers": 8,                      # 1~8
        "outbound_max": 5000,
        "inbound_max": 1000,
        "control_lane_max": 64,
        "retry_count": 3,
        "retry_delay_ms": 500,
        "backoff": "exponential",                  # fixed / exponential
        "cancel_orphan_goals": False,
        "catch_up_sec": 0,                          # 0: 주기적 catch-up 비활성화
        "reconcile_sec": 0,                         # 0: 주기적 CSE reconcile 비활성화
        "dedup_retention_days": 7,
        "cleanup_interval_sec": 3600,
    },
    "dispatch": {"drain_budget": 32},
    "policy": {
        "confirmation": "auto",
        "qos_strictness": "reject",                 # reject / demote
        "history_keep_all_limit": 1000,             # KEEP_ALL의 CSE 보관 상한(mni)
        "default_stale_after_ms": 5000,
        "stale_deadline_multiplier": 2,
        "stale_exempt_topics": ["robot_description", "tf_static"],
        "self_echo_window_sec": 0.5,
        "qos_event_coalesce_sec": 5.0,
        "max_total_write_hz": 0,                    # 0: 전역 쓰기 속도 제한 없음
        "suitability": {"large_payload_bytes": 49152},
    },
    "logging": {
        "level": "INFO",                            # DEBUG / INFO / WARNING / ERROR
        "heartbeat_sec": 30,
        "status_severity_min": "info",
    },
    "qos_fcnt": {
        "enabled": True,
        "type": "ros:tqos",
        "cnd": "kr.ac.sejong.seslab.ros2.moduleclass.topicQos",
        "service_type": "ros:sqos",
        "service_cnd": "kr.ac.sejong.seslab.ros2.moduleclass.serviceQos",
        "action_type": "ros:aqos",
        "action_cnd": "kr.ac.sejong.seslab.ros2.moduleclass.actionQos",
        "lbl_compat": True,
        "allow_update": False,
        "publish_min_interval_ms": 5000,
        "peers_max": 8,
    },

    # 자동 발견한 인터페이스의 기본 표현·제어 정책.
    "qos_profiles": {
        "sensor_data": {
            "reliability": "BEST_EFFORT",
            "durability": "VOLATILE",
            "history": "KEEP_LAST",
            "depth": 5,
        },
        "command": {
            "reliability": "RELIABLE",
            "durability": "VOLATILE",
            "history": "KEEP_LAST",
            "depth": 10,
        },
    },
    "defaults": {
        # qos를 생략하면 sensor_data를 기준으로 publisher와 자동 조정합니다.
        # qos를 명시하면 해당 필드는 사용자가 요청한 값으로 취급합니다.
        "topic_observe": {"representation": "latest"},
        "topic_command": {
            "qos": "command",
            "command": {"max_age_ms": 5000},
            "access": {"enabled": not OBSERVE_ONLY},
        },
        "service": {"timeout_ms": 5000, "access": {"enabled": not OBSERVE_ONLY}},
        "action": {
            "timeout_ms": 0,                        # 0: 서버 소멸을 discovery로 감지
            "feedback": "sampled",
            "feedback_sample": {"min_interval_ms": 500},
            "access": {"enabled": not OBSERVE_ONLY},
        },
    },
    "naming": {"path_style": "flat", "sanitize": "_"},
    "bridge": {"topics": [], "services": [], "actions": []},
}
