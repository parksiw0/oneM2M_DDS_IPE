"""Validate and normalize settings generated for the discovery runtime."""

from __future__ import annotations

import copy
import logging
from collections.abc import Iterator
from typing import Any, cast

from cerberus import Validator

from ipe.core.transaction import termination_load_message, termination_violation
from ipe.onem2m.client import CSE_SETTINGS_FIELD
from ipe.qos.configuration import (
    QOS_FCNT_FIELD,
    QOS_PROFILES_FIELD,
    QoSConfigError,
    _normalize_qos_case,
    validate_qos_config,
)
from ipe.runtime.naming import compile_pattern
from ipe.runtime.planning import (
    ACTION_ITEM_SCHEMA,
    DEFAULTS_SCHEMA,
    SERVICE_ITEM_SCHEMA,
    TOPIC_ITEM_SCHEMA,
)

log = logging.getLogger("ipe.runtime.settings")


class ConfigError(Exception):
    pass


def load_config() -> dict[str, Any]:
    """Validate the settings declared in the project's config.py."""
    import config as settings

    return validate_config(settings.CONFIG)


def validate_config(raw: dict[str, Any]) -> dict[str, Any]:
    cfg = copy.deepcopy(raw)
    _normalize_qos_case(cfg)

    v = Validator(CONFIG_SCHEMA, purge_unknown=False)
    if not v.validate(cfg):
        raise ConfigError(f"Schema validation failed: {_fmt_errors(v.errors)}")
    normalized = v.normalized(cfg)

    _normalize_robot_namespaces(normalized)
    _check_semantics(normalized)
    _probe_type_pins(normalized)
    return cast(dict[str, Any], normalized)


def _normalize_robot_namespaces(cfg: dict[str, Any]) -> None:
    """비어 있지 않은 namespace에 선행 '/'를 붙인다 — `namespace: robot1`
    표기에서 prefix 매칭이 조용히 실패하지 않도록."""
    for r in cfg.get("robots", []):
        ns = r.get("namespace", "")
        if ns and not ns.startswith("/"):
            r["namespace"] = "/" + ns


# ---------------------------------------------------------------------------
# 의미론적 교차 필드 검사
# ---------------------------------------------------------------------------


def _check_semantics(cfg: dict[str, Any]) -> None:
    robots = cfg.get("robots", [])
    ids = [r["id"] for r in robots]
    if len(ids) != len(set(ids)):
        raise ConfigError(f"Duplicate robot id in 'robots': {ids}")

    _check_cse_protocol(cfg)
    storage = cfg["storage"]
    if storage["pool_max_size"] < storage["pool_min_size"]:
        raise ConfigError("storage.pool_max_size must be >= storage.pool_min_size")
    _check_patterns(cfg)
    _check_mode_semantics(cfg)
    _check_filters_and_sampling(cfg)
    try:
        validate_qos_config(cfg)
    except QoSConfigError as exc:
        raise ConfigError(str(exc)) from exc
    _check_termination_invariant(cfg)
    _check_flexcontainer(cfg)


def _check_cse_protocol(cfg: dict[str, Any]) -> None:
    """프로토콜별 cse 요건 (단일 protocol 필드가 양방향 전송을 지배 — mixed 미지원).

    http: endpoint 필수. mqtt: cse_id(토픽 receiver 세그먼트) 필수 — cse_base(CSE
    리소스 이름)와 다를 수 있어 추측하지 않는다.
    """
    cse = cfg.get("cse", {})
    proto = cse.get("protocol", "http")
    if proto == "http" and not cse.get("endpoint"):
        raise ConfigError(
            "cse.endpoint is required when cse.protocol is 'http' (e.g. http://localhost:3000)."
        )
    if proto == "mqtt" and not cse.get("cse_id"):
        raise ConfigError(
            "cse.cse_id is required when cse.protocol is 'mqtt' — it is the MQTT "
            "topic receiver segment (CSE-ID / tinyIoT CSE_BASE_RI) and may differ "
            "from cse.cse_base (the CSE resource name). "
            "e.g. cse_base: TinyIoT, cse_id: tinyiot"
        )


def _check_flexcontainer(cfg: dict[str, Any]) -> None:
    # FCNT는 latest-state 의미 전용 — historical/sampled에 선언하면 오류
    defaults_rep = ((cfg.get("defaults") or {}).get("topic_observe") or {}).get("representation")
    for kind, where, _i, item in _bridge_items(cfg):
        if kind != "topics" or "flexcontainer" not in item:
            continue
        rep = item.get("representation") or defaults_rep
        if rep not in ("latest", "both"):
            raise ConfigError(
                f"{where}: flexcontainer requires representation latest|both "
                f"(got {rep!r}) — FCNT is latest-state only"
            )


def _bridge_items(cfg: dict[str, Any]) -> Iterator[tuple[str, str, int, dict[str, Any]]]:
    bridge = cfg.get("bridge", {})
    for kind in ("topics", "services", "actions"):
        for i, item in enumerate(bridge.get(kind, []) or []):
            yield kind, f"bridge.{kind}[{i}]", i, item


def _label(item: dict[str, Any]) -> str:
    return item.get("name") or item.get("match") or "?"


def _check_patterns(cfg: dict[str, Any]) -> None:
    """모든 패턴을 선행 컴파일 — 잘못된 캡처 이름이나 미종결 중괄호는
    런타임 re.error 크래시가 아니라 설정 오류여야 한다(B5)."""
    for _kind, where, _i, item in _bridge_items(cfg):
        has_name = "name" in item
        has_match = "match" in item
        if has_name == has_match:
            raise ConfigError(
                f"{where} must have exactly one of 'name' or 'match' "
                f"(got name={has_name}, match={has_match})."
            )
        if has_match:
            try:
                compile_pattern(item["match"])
            except ValueError as e:
                raise ConfigError(f"{where}: {e}") from e
    disc = cfg.get("discovery", {})
    for key in ("allow", "deny"):
        for j, pat in enumerate(disc.get(key, []) or []):
            try:
                compile_pattern(pat)
            except ValueError as e:
                raise ConfigError(f"discovery.{key}[{j}]: {e}") from e


def _check_mode_semantics(cfg: dict[str, Any]) -> None:
    """config-only 모드는 match 규칙 금지(죽은 패턴이 조용히 묻힌다),
    모든 name 항목에 type 고정 요구(미발견 상태에서도 동작해야 하므로)."""
    if cfg.get("discovery", {}).get("mode", "hybrid") != "config-only":
        return
    for _kind, where, _i, item in _bridge_items(cfg):
        if "match" in item:
            raise ConfigError(
                f"{where}: 'match' rules are not allowed in discovery.mode "
                f"'config-only' — patterns would silently never fire (§5.1). "
                f"Use mode 'hybrid' or list interfaces by 'name'."
            )
        if not item.get("type"):
            raise ConfigError(
                f"{where} '{_label(item)}': discovery.mode 'config-only' requires a "
                f"'type' pin — the interface must work even when not discovered (§5.1)."
            )


def _check_filters_and_sampling(cfg: dict[str, Any]) -> None:
    defaults = cfg.get("defaults") or {}
    bridge = cfg.get("bridge", {})

    for i, item in enumerate(bridge.get("topics", []) or []):
        where = f"bridge.topics[{i}]"
        label = _label(item)
        flt = item.get("filter")
        if flt:
            if flt["type"] == "anomaly" and not flt.get("fields"):
                raise ConfigError(
                    f"{where} '{label}' filter 'anomaly' requires 'fields' "
                    f"(numeric fields to score)."
                )
            if flt["type"] == "delta" and "min_change" not in flt:
                raise ConfigError(f"{where} '{label}' filter 'delta' requires 'min_change'.")
            if flt["type"] == "window":
                missing = [k for k in ("size", "aggregations") if k not in flt]
                if missing:
                    raise ConfigError(f"{where} '{label}' filter 'window' requires {missing}.")
        rep = item.get("representation")
        dblock = defaults.get(
            "topic_command" if item.get("direction") == "command" else "topic_observe", {}
        )
        if rep == "sampled" and "sample" not in item and "sample" not in dblock:
            raise ConfigError(
                f"{where} '{label}' has representation 'sampled' but no 'sample' "
                f"block (and the defaults block has none)."
            )
    # defaults 블록 자체도 자기 정합적이어야 한다
    tob = defaults.get("topic_observe") or {}
    if tob.get("representation") == "sampled" and "sample" not in tob:
        raise ConfigError(
            "defaults.topic_observe: representation 'sampled' requires a 'sample' block."
        )

    for i, item in enumerate(bridge.get("actions", []) or []):
        where = f"bridge.actions[{i}]"
        if (
            item.get("feedback") == "sampled"
            and "feedback_sample" not in item
            and "feedback_sample" not in (defaults.get("action") or {})
        ):
            raise ConfigError(
                f"{where} '{_label(item)}' feedback 'sampled' requires 'feedback_sample'."
            )


def _check_termination_invariant(cfg: dict[str, Any]) -> None:
    """모든 서비스/액션에는 종료 메커니즘이 최소 하나 있어야 한다.
    timeout_ms: 0(무제한)은 디스커버리 폴링(refresh_sec > 0)이 서버 소멸을
    감지할 수 있을 때만 허용. 술어는 rules.termination_violation(resolver와 공유)."""
    refresh = cfg.get("discovery", {}).get("refresh_sec", 5)
    defaults = cfg.get("defaults") or {}
    builtin = {"services": ("service", 5000), "actions": ("action", 0)}
    for kind, where, _i, item in _bridge_items(cfg):
        if kind == "topics":
            continue
        dname, fallback = builtin[kind]
        eff = item.get("timeout_ms", (defaults.get(dname) or {}).get("timeout_ms", fallback))
        if termination_violation(eff, refresh):
            raise ConfigError(termination_load_message(where, _label(item)))


# ---------------------------------------------------------------------------
# 타입 로드 프로브 훅. `type:` 고정의 판정 기준은 rosidl이 실제로 import할 수
# 있는가이다(정규식 게이트 없음). 프로브는 best-effort:
#   - rosidl import 불가(CI, ROS 환경 없음)      -> 전체 건너뜀
#   - 타입의 *패키지*가 이 환경에 없음           -> 경고 + 건너뜀 (px4_msgs 없는
#     검증 호스트는 아무것도 증명 못 함; 바인딩이 런타임에 강제)
#   - 그 외(오타 난 메시지, 잘못된 형식)         -> 로더의 실제 오류를 담은
#     ConfigError
# ---------------------------------------------------------------------------


def _probe_type_pins(cfg: dict[str, Any]) -> None:
    try:
        from rosidl_runtime_py.utilities import get_action, get_message, get_service
    except ImportError:
        return
    getters = {"topics": get_message, "services": get_service, "actions": get_action}
    for kind, where, _i, item in _bridge_items(cfg):
        pin = item.get("type")
        if not pin:
            continue
        try:
            getters[kind](pin)
        except (ModuleNotFoundError, ImportError) as e:
            log.warning(
                "%s '%s': type pin '%s' not loadable in this environment (%s) — "
                "skipping probe; binding will enforce it at runtime (§3.2)",
                where,
                _label(item),
                pin,
                e,
            )
        except Exception as e:
            raise ConfigError(
                f"{where} '{_label(item)}': type pin '{pin}' failed the load probe "
                f"(§3.2): {type(e).__name__}: {e}"
            ) from e


def _fmt_errors(errors: Any, prefix: str = "") -> str:
    """Cerberus 오류 dict를 읽기 좋은 'key.path: message' 줄들로 평탄화."""
    lines: list[str] = []

    def walk(e: Any, path: str) -> None:
        if isinstance(e, dict):
            for k, v in e.items():
                walk(v, f"{path}.{k}" if path else str(k))
        elif isinstance(e, list):
            for item in e:
                if isinstance(item, dict | list):
                    walk(item, path)
                else:
                    lines.append(f"{path}: {item}")
        else:
            lines.append(f"{path}: {e}")

    walk(errors, prefix)
    return "; ".join(lines)


# Process-level settings. Domain-specific schemas are supplied by their owners.
CONFIG_SCHEMA: dict[str, Any] = {
    "ipe": {
        "type": "dict",
        "required": False,
        "schema": {"instance_id": {"type": "string", "default": "ros2-ipe"}},
        "default": {},
    },
    "cse": CSE_SETTINGS_FIELD,
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
    "robots_strict": {"type": "boolean", "default": False},  # 미등록 {robot} 거부
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
            "ros_peer": {"type": "string", "default": ""},
            "rmw_implementation": {"type": "string", "default": ""},
            "allow": {"type": "list", "schema": {"type": "string"}, "default": ["/**"]},
            "deny": {"type": "list", "schema": {"type": "string"}, "default": []},
            "refresh_sec": {"type": ["integer", "float"], "min": 0, "default": 5},
            "vanish_grace_polls": {"type": "integer", "min": 1, "default": 2},  # 소멸 디바운스
            "graph_settle_timeout_sec": {
                "type": ["integer", "float"],
                "min": 0.1,
                "default": 10,
            },
            "graph_stable_polls": {"type": "integer", "min": 1, "default": 2},
            "graph_poll_sec": {
                "type": ["integer", "float"],
                "min": 0.05,
                "default": 0.5,
            },
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
    "qos_profiles": QOS_PROFILES_FIELD,
    "defaults": {"type": "dict", "required": False, "schema": DEFAULTS_SCHEMA, "default": {}},
    "bridge": {
        "type": "dict",
        "required": False,
        "schema": {
            "topics": {
                "type": "list",
                "schema": {"type": "dict", "schema": TOPIC_ITEM_SCHEMA},
                "default": [],
            },
            "services": {
                "type": "list",
                "schema": {"type": "dict", "schema": SERVICE_ITEM_SCHEMA},
                "default": [],
            },
            "actions": {
                "type": "list",
                "schema": {"type": "dict", "schema": ACTION_ITEM_SCHEMA},
                "default": [],
            },
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
            "qos_strictness": {
                "type": "string",
                "allowed": ["reject", "demote"],
                "default": "reject",
            },
            "history_keep_all_limit": {"type": "integer", "min": 1, "default": 1000},
            "default_stale_after_ms": {"type": "integer", "min": 0, "default": 5000},
            "stale_deadline_multiplier": {"type": "integer", "min": 1, "default": 2},
            "stale_exempt_topics": {
                "type": "list",
                "schema": {"type": "string"},
                "default": ["robot_description", "tf_static"],
            },
            "self_echo_window_sec": {"type": ["integer", "float"], "min": 0, "default": 0.5},
            "qos_event_coalesce_sec": {"type": ["integer", "float"], "min": 0, "default": 5.0},
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
            "backend": {
                "type": "string",
                "allowed": ["sqlite", "postgresql"],
                "default": "sqlite",
            },
            "state_db": {"type": "string", "default": "ipe_state.db"},
            "dsn": {"type": "string", "required": False, "nullable": True},
            "schema": {"type": "string", "empty": False, "default": "ipe_state"},
            "pool_min_size": {"type": "integer", "min": 1, "default": 1},
            "pool_max_size": {"type": "integer", "min": 1, "default": 8},
            "max_spool_entries": {"type": "integer", "min": 1, "default": 10000},
            "max_spool_mb": {"type": "integer", "min": 1, "default": 64},
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
            "backoff": {
                "type": "string",
                "allowed": ["fixed", "exponential"],
                "default": "exponential",
            },
            "on_failure": {
                "type": "string",
                "allowed": ["skip", "retry", "rebind", "reprovision"],
                "default": "retry",
            },
            "queue_overflow": {
                "type": "string",
                "allowed": ["reject", "drop_oldest"],
                "default": "reject",
            },
            "inbound_max": {"type": "integer", "min": 1, "default": 1000},
            "control_lane_max": {"type": "integer", "min": 1, "default": 64},  # 2-레인 제어 큐
            "outbound_max": {"type": "integer", "min": 1, "default": 5000},
            "outbound_workers": {"type": "integer", "min": 1, "max": 8, "default": 8},
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
    # QoS flexContainer 게시 (QoS_FCNT_설계서 §4~5)
    "qos_fcnt": QOS_FCNT_FIELD,
    "expose": {"type": "list", "required": False, "default": []},  # 예약 필드(D2)
}
