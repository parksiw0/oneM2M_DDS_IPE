"""IPE 설정 YAML(schema_version: 2)의 Cerberus 스키마 — 유효한 v2 설정의 단일 기준.

enum 값은 UPPERCASE로 검증한다(케이스 정규화는 loader가 검증 전에 수행).
교차 필드 규칙은 여기가 아니라 loader에 있다.

불변식: `defaults` 블록 안에서 재사용되는 서브 스키마에는 cerberus `default:`를
두면 안 된다 — 정규화가 모든 부분 규칙 dict에 기본값을 주입해, 필드 단위
deep-merge에서 사용자의 `defaults:` 블록을 조용히 이겨 버린다(B3 confirm 강등
버그). 해당 필드의 기본값은 resolver 소관.
"""

from __future__ import annotations

from typing import Any

from ipe.config.spec import ACTION_QOS_CHANNELS

# --- QoS (속성 8개; 프로파일에선 전부 선택, 병합이 기본값을 채움) ----------

QOS_FIELDS: dict[str, Any] = {
    "reliability": {"type": "string", "allowed": ["RELIABLE", "BEST_EFFORT"]},
    "durability": {"type": "string", "allowed": ["VOLATILE", "TRANSIENT_LOCAL"]},
    "history": {"type": "string", "allowed": ["KEEP_LAST", "KEEP_ALL"]},
    "depth": {"type": "integer", "min": 1},
    "deadline_ms": {"type": "integer", "min": 0},
    "lifespan_ms": {"type": "integer", "min": 0},
    "liveliness": {"type": "string", "allowed": ["AUTOMATIC", "MANUAL_BY_TOPIC"]},
    "liveliness_lease_duration_ms": {"type": "integer", "min": 0},
}

# 인터페이스의 `qos:` = 프리셋 이름(문자열) 또는 {profile, ...오버라이드}.
QOS_INLINE_SCHEMA: dict[str, Any] = {
    "profile": {"type": "string"},
    **QOS_FIELDS,
}

SAMPLE_SCHEMA: dict[str, Any] = {
    "rate_hz": {"type": "float", "min": 0.0},
    "min_interval_ms": {"type": "integer", "min": 0},
}

FILTER_SCHEMA: dict[str, Any] = {
    "type": {"type": "string", "allowed": ["delta", "window", "anomaly"], "required": True},
    "fields": {"type": "list", "schema": {"type": "string"}},
    "min_change": {"type": "float", "min": 0.0},
    "max_interval_ms": {"type": "integer", "min": 0},
    "mode": {"type": "string", "allowed": ["count", "time"], "default": "count"},
    "size": {"type": ["integer", "float"], "min": 0},
    "aggregations": {
        "type": "list",
        "schema": {"type": "string", "allowed": ["mean", "min", "max", "std", "last"]},
    },
    # anomaly 전용(§7.4)
    "detector": {"type": "string", "allowed": ["isolation_forest", "mad"]},
    "anomaly_mode": {"type": "string", "allowed": ["tag", "escalate", "suppress"]},
    "window": {"type": "integer", "min": 8},
    "retrain_every": {"type": "integer", "min": 1},
    "contamination": {"type": "float", "min": 0.001, "max": 0.5},
    "min_samples": {"type": "integer", "min": 1},
    "threshold": {"type": "float", "min": 0.1},
}

COMMAND_SAFETY_SCHEMA: dict[str, Any] = {
    "rate_limit_hz": {"type": "float", "min": 0.0},
    "clamp": {
        "type": "dict",
        "keysrules": {"type": "string"},
        "valuesrules": {
            "type": "list",
            "items": [{"type": ["integer", "float"]}, {"type": ["integer", "float"]}],
        },
    },
    "watchdog_ms": {"type": "integer", "min": 0},
    "max_age_ms": {"type": "integer", "min": 0},           # 수신 신선도 게이트
    "liveliness_lease_ms": {"type": "integer", "min": 1},  # 로봇 측 IPE 사망 감지
}

# cerberus default 금지(모듈 docstring 참고) — 기본값은 resolver가 적용
ACCESS_SCHEMA: dict[str, Any] = {
    "enabled": {"type": "boolean"},
    "confirm": {"type": "string", "allowed": ["auto", "required"]},
}

SOURCE_TS_SCHEMA: dict[str, Any] = {
    "field": {"type": "string", "empty": False},     # 메시지 내 점 표기 경로
    "format": {"type": "string", "empty": False},    # 레지스트리 이름; 어댑터가 별칭 추가 가능
}

_QOS_FIELD = {"type": ["string", "dict"], "schema": QOS_INLINE_SCHEMA}

# --- 브리지 인터페이스 항목 (name XOR match; loader가 강제) ------------------

TOPIC_ITEM_SCHEMA: dict[str, Any] = {
    "name": {"type": "string", "empty": False},
    "match": {"type": "string", "empty": False},
    "type": {"type": "string"},                       # 선택적 타입 고정; 로드 프로브 대상
    "direction": {"type": "string", "allowed": ["observe", "command", "both"]},
    "representation": {
        "type": "string",
        "allowed": ["historical", "latest", "both", "sampled"],
    },
    "qos": _QOS_FIELD,
    "sample": {"type": "dict", "schema": SAMPLE_SCHEMA},
    "filter": {"type": "dict", "schema": FILTER_SCHEMA},
    "selected_fields": {"type": "list", "schema": {"type": "string"}},
    "stale_after_ms": {"type": "integer", "min": 0},   # 신선도 워치독
    "source_ts": {"type": "dict", "schema": SOURCE_TS_SCHEMA},
    "role": {"type": "string"},
    "group": {"type": "string"},
    "alias": {"type": "string"},
    "alias_template": {"type": "string"},
    "path": {"type": "string"},                        # {capture} 포함 가능
    "command": {"type": "dict", "schema": COMMAND_SAFETY_SCHEMA},
    "access": {"type": "dict", "schema": ACCESS_SCHEMA},
    "robot": {"type": "string"},                       # 명시적 robot 오버라이드
    # FCNT 매핑 선언 — representation latest/both에서만 (loader 교차검증)
    "flexcontainer": {
        "type": "dict",
        "schema": {
            "type": {"type": "string", "required": True, "empty": False},
            "cnd": {"type": "string", "required": True, "empty": False},
            "field_map": {
                "type": "dict", "required": True, "minlength": 1,
                "keysrules": {"type": "string"},
                "valuesrules": {"type": "string"},
            },
        },
    },
}

SERVICE_ITEM_SCHEMA: dict[str, Any] = {
    "name": {"type": "string", "empty": False},
    "match": {"type": "string", "empty": False},
    "type": {"type": "string"},
    "timeout_ms": {"type": "integer", "min": 0},
    "qos": _QOS_FIELD,                                 # rclpy Client의 qos_profile
    "request_fields": {"type": "list", "schema": {"type": "string"}},
    "response_fields": {"type": "list", "schema": {"type": "string"}, "nullable": True},
    "request_template": {"type": "dict"},
    "alias": {"type": "string"},
    "path": {"type": "string"},
    "access": {"type": "dict", "schema": ACCESS_SCHEMA},
    "robot": {"type": "string"},
}

# 액션 클라이언트 채널별 QoS 오버라이드; 채널 집합은 spec의 목록에 고정
ACTION_QOS_SCHEMA: dict[str, Any] = {ch: _QOS_FIELD for ch in ACTION_QOS_CHANNELS}

ACTION_ITEM_SCHEMA: dict[str, Any] = {
    "name": {"type": "string", "empty": False},
    "match": {"type": "string", "empty": False},
    "type": {"type": "string"},
    "feedback": {
        "type": "string",
        "allowed": ["log", "latest", "sampled", "combined"],
    },
    "feedback_sample": {"type": "dict", "schema": SAMPLE_SCHEMA},
    "goal_fields": {"type": "list", "schema": {"type": "string"}},
    "feedback_fields": {"type": "list", "schema": {"type": "string"}},
    "result_fields": {"type": "list", "schema": {"type": "string"}},
    "goal_template": {"type": "dict"},
    "timeout_ms": {"type": "integer", "min": 0},
    "qos": {"type": "dict", "schema": ACTION_QOS_SCHEMA},
    "alias": {"type": "string"},
    "path": {"type": "string"},
    "access": {"type": "dict", "schema": ACCESS_SCHEMA},
    "robot": {"type": "string"},
}

# --- defaults 블록 (해석 시 적용되는 클래스별 기본 정책) ---------------------
# 항목 자체와 같은 필드 스키마로 검증한다(B7). 클래스 전체 기본값으로 의미가
# 없는 식별/선택 키만 제외.

_NOT_IN_DEFAULTS = ("name", "match", "robot", "path", "alias", "alias_template",
                    "direction", "type")


def _defaults_fields(item_schema: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in item_schema.items() if k not in _NOT_IN_DEFAULTS}


DEFAULTS_SCHEMA: dict[str, Any] = {
    "topic_observe": {"type": "dict", "schema": _defaults_fields(TOPIC_ITEM_SCHEMA)},
    "topic_command": {"type": "dict", "schema": _defaults_fields(TOPIC_ITEM_SCHEMA)},
    "service": {"type": "dict", "schema": _defaults_fields(SERVICE_ITEM_SCHEMA)},
    "action": {"type": "dict", "schema": _defaults_fields(ACTION_ITEM_SCHEMA)},
}

CONFIG_SCHEMA: dict[str, Any] = {
    "schema_version": {"type": "integer", "required": True, "allowed": [2]},
    "ipe": {
        "type": "dict",
        "required": False,
        "schema": {"instance_id": {"type": "string", "default": "ros2-ipe"}},
        "default": {},
    },
    "cse": {
        "type": "dict",
        "required": True,
        "schema": {
            "endpoint": {"type": "string", "required": True, "regex": r"^https?://.+"},
            "cse_base": {"type": "string", "required": True, "empty": False},
            "ae_name": {"type": "string", "required": True, "empty": False},
            "protocol": {"type": "string", "allowed": ["http", "mqtt"], "default": "http"},
            "origin": {"type": "string", "default": "CAdmin"},
            "rvi": {"type": "string", "default": "3"},
            "poa": {"type": "string", "required": False},
        },
    },
    "notification_server": {
        "type": "dict",
        "required": False,
        "schema": {
            "host": {"type": "string", "default": "0.0.0.0"},
            "port": {"type": "integer", "min": 1, "max": 65535, "default": 5050},
        },
        "default": {},
    },
    "robots": {
        "type": "list",
        "required": False,
        "minlength": 1,
        "schema": {
            "type": "dict",
            "schema": {
                "id": {"type": "string", "required": True, "empty": False},
                "namespace": {"type": "string", "default": ""},
                "ae_per_robot": {"type": "boolean", "default": False},
                "ae_name": {"type": "string", "required": False},
            },
        },
        "default": [{"id": "default", "namespace": "", "ae_per_robot": False}],
    },
    "robots_strict": {"type": "boolean", "default": False},   # 미등록 {robot} 거부
    "discovery": {
        "type": "dict",
        "required": False,
        "schema": {
            "mode": {
                "type": "string",
                "allowed": ["config-only", "auto-expose", "hybrid"],
                "default": "hybrid",
            },
            "domain_id": {"type": "integer", "min": 0, "default": 0},
            "allow": {"type": "list", "schema": {"type": "string"}, "default": ["/**"]},
            "deny": {"type": "list", "schema": {"type": "string"}, "default": []},
            "refresh_sec": {"type": ["integer", "float"], "min": 0, "default": 5},
            "vanish_grace_polls": {"type": "integer", "min": 1, "default": 2},  # 소멸 디바운스
        },
        "default": {},
    },
    "naming": {
        "type": "dict",
        "required": False,
        "schema": {
            "path_style": {
                "type": "string",
                "allowed": ["flat", "nested", "aliased"],
                "default": "nested",
            },
            "sanitize": {"type": "string", "default": "_"},
        },
        "default": {},
    },
    "qos_profiles": {
        "type": "dict",
        "required": True,
        "keysrules": {"type": "string"},
        "valuesrules": {"type": "dict", "schema": QOS_FIELDS},
        "minlength": 1,
    },
    "defaults": {"type": "dict", "required": False, "schema": DEFAULTS_SCHEMA, "default": {}},
    "bridge": {
        "type": "dict",
        "required": False,
        "schema": {
            "topics": {"type": "list", "schema": {"type": "dict", "schema": TOPIC_ITEM_SCHEMA}, "default": []},
            "services": {"type": "list", "schema": {"type": "dict", "schema": SERVICE_ITEM_SCHEMA}, "default": []},
            "actions": {"type": "list", "schema": {"type": "dict", "schema": ACTION_ITEM_SCHEMA}, "default": []},
        },
        "default": {"topics": [], "services": [], "actions": []},
    },
    "policy": {
        "type": "dict",
        "required": False,
        "schema": {
            "suitability": {
                "type": "dict",
                "schema": {
                    "high_rate_hz": {"type": ["integer", "float"], "min": 0, "default": 20},
                    # tinyIoT 하드 리밋 65536에서 인코딩 여유분을 뺀 값
                    "large_payload_bytes": {"type": "integer", "min": 0, "default": 49152},
                    "realtime_critical_deny": {"type": "boolean", "default": True},
                },
                "default": {},
            },
            "confirmation": {"type": "string", "allowed": ["auto", "required"], "default": "auto"},
            "max_total_write_hz": {"type": ["integer", "float"], "min": 0, "default": 0},
            # observe 방향 QoS 엄격성 가드 동작
            "qos_strictness": {"type": "string", "allowed": ["reject", "demote"], "default": "reject"},
        },
        "default": {},
    },
    "dispatch": {
        "type": "dict",
        "required": False,
        "schema": {"drain_budget": {"type": "integer", "min": 1, "default": 32}},
        "default": {},
    },
    "storage": {
        "type": "dict",
        "required": False,
        "schema": {
            "state_db": {"type": "string", "default": "ipe_state.db"},
            "max_spool_entries": {"type": "integer", "min": 0, "default": 10000},
            "max_spool_mb": {"type": "integer", "min": 0, "default": 64},
        },
        "default": {},
    },
    "logging": {
        "type": "dict",
        "required": False,
        "schema": {
            "heartbeat_sec": {"type": ["integer", "float"], "min": 1, "default": 30},
            "level": {
                "type": "string",
                "allowed": ["DEBUG", "INFO", "WARNING", "ERROR"],
                "default": "INFO",
            },
            "status_severity_min": {
                "type": "string",
                "allowed": ["info", "warning", "error"],
                "default": "info",
            },
        },
        "default": {},
    },
    "transfer": {
        "type": "dict",
        "required": False,
        "schema": {"default_unit": {"type": "string", "default": "sampled"}},
        "default": {},
    },
    "recovery": {
        "type": "dict",
        "required": False,
        "schema": {
            "retry_count": {"type": "integer", "min": 0, "default": 3},
            "retry_delay_ms": {"type": "integer", "min": 0, "default": 500},
            "backoff": {"type": "string", "allowed": ["fixed", "exponential"], "default": "exponential"},
            "on_failure": {
                "type": "string",
                "allowed": ["skip", "retry", "rebind", "reprovision"],
                "default": "retry",
            },
            "queue_overflow": {"type": "string", "allowed": ["reject", "drop_oldest"], "default": "reject"},
            "inbound_max": {"type": "integer", "min": 1, "default": 1000},
            "control_lane_max": {"type": "integer", "min": 1, "default": 64},   # 2-레인 제어 큐
            "outbound_max": {"type": "integer", "min": 1, "default": 5000},
            "catch_up_sec": {"type": ["integer", "float"], "min": 0, "default": 0},
            "reconcile_sec": {"type": ["integer", "float"], "min": 0, "default": 0},
            "cancel_orphan_goals": {"type": "boolean", "default": False},
            "dedup_retention_days": {"type": "integer", "min": 0, "default": 7},
        },
        "default": {},
    },
    "schema_validation": {
        "type": "dict",
        "required": False,
        "schema": {"enabled": {"type": "boolean", "default": False}},
        "default": {},
    },
    "expose": {"type": "list", "required": False, "default": []},  # 예약 필드(D2)
}

# 검증 전에 UPPERCASE로 케이스 정규화되는 enum 필드.
QOS_ENUM_KEYS = ("reliability", "durability", "history", "liveliness")
