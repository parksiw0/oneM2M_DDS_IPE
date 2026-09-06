"""QoS configuration schema, profile resolution, and shared validation."""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from typing import Any

from ipe.qos.models import ACTION_QOS_CHANNELS, QoSSpec
from ipe.qos.registry import ACTIVE_POLICIES

QOS_FIELDS = {key: rule for policy in ACTIVE_POLICIES for key, rule in policy.SCHEMA.items()}

log = logging.getLogger(__name__)


class QoSConfigError(ValueError):
    pass


QOS_INLINE_SCHEMA: dict[str, Any] = {
    "profile": {"type": "string"},
    **QOS_FIELDS,
}

QOS_ENUM_KEYS = ("reliability", "durability", "history", "liveliness")

QOS_PROFILES_FIELD = {
    "type": "dict",
    "required": True,
    "keysrules": {"type": "string"},
    "valuesrules": {"type": "dict", "schema": QOS_FIELDS},
    "minlength": 1,
}

QOS_FCNT_FIELD = {
    "type": "dict",
    "required": False,
    "schema": {
        "enabled": {"type": "boolean", "default": True},
        "type": {"type": "string", "empty": False, "default": "ros:tqos"},
        "cnd": {
            "type": "string",
            "empty": False,
            "default": "kr.ac.sejong.seslab.ros2.moduleclass.topicQos",
        },
        "service_type": {"type": "string", "empty": False, "default": "ros:sqos"},
        "service_cnd": {
            "type": "string",
            "empty": False,
            "default": "kr.ac.sejong.seslab.ros2.moduleclass.serviceQos",
        },
        "action_type": {"type": "string", "empty": False, "default": "ros:aqos"},
        "action_cnd": {
            "type": "string",
            "empty": False,
            "default": "kr.ac.sejong.seslab.ros2.moduleclass.actionQos",
        },
        "lbl_compat": {"type": "boolean", "default": True},
        "allow_update": {"type": "boolean", "default": False},
        "publish_min_interval_ms": {"type": "integer", "min": 0, "default": 5000},
        "peers_max": {"type": "integer", "min": 0, "default": 8},
    },
    "default": {},
}


# ---------------------------------------------------------------------------
# §8.5 — command 퍼블리셔 금지 QoS (MANUAL liveliness / 유한 deadline)
# ---------------------------------------------------------------------------


def command_qos_violation(liveliness: Any, deadline_ms: Any) -> str | None:
    """위반 종류 판정: 'liveliness' | 'deadline' | None.

    검사 순서가 곧 보고 순서다 — 둘 다 위반이면 liveliness가 이긴다(기존 동작).
    """
    if liveliness == "MANUAL_BY_TOPIC":
        return "liveliness"
    if deadline_ms is not None:
        return "deadline"
    return None


_COMMAND_QOS_LOAD_MSG = {
    "liveliness": (
        "{where}: liveliness MANUAL_BY_TOPIC is not allowed on command "
        "topics (§8.5) — the IPE does not assert liveliness; use "
        "command.liveliness_lease_ms for robot-side IPE-death detection."
    ),
    "deadline": (
        "{where}: a finite deadline_ms is not allowed on command topics "
        "(§8.5) — commands are aperiodic; use command.watchdog_ms instead."
    ),
}

_COMMAND_QOS_RESOLVE_MSG = {
    "liveliness": (
        "command topic '{interface}': liveliness MANUAL_BY_TOPIC is not "
        "allowed (§8.5); use command.liveliness_lease_ms instead"
    ),
    "deadline": (
        "command topic '{interface}': finite deadline_ms is not allowed "
        "(§8.5); use command.watchdog_ms instead"
    ),
}


def command_qos_load_message(violation: str, where: str) -> str:
    return _COMMAND_QOS_LOAD_MSG[violation].format(where=where)


def command_qos_resolve_message(violation: str, interface: str) -> str:
    return _COMMAND_QOS_RESOLVE_MSG[violation].format(interface=interface)


# ---------------------------------------------------------------------------
# QoS 프로파일 참조 — 문자열 참조와 인라인 맵의 'profile' 베이스 모두 검사
# ---------------------------------------------------------------------------


def undefined_qos_ref(
    value: Any, names: Collection[str], *, empty_base_violates: bool
) -> tuple[str, str] | None:
    """미정의 프로파일 참조 검출: ('profile'|'base', 참조 이름) 또는 None.

    함정: 빈 문자열 베이스(`profile: ""`)는 loader가 거짓값으로 보고 통과시키고
    resolver는 거부한다(resolver의 `if base_name else QoSSpec()` 폴백과 짝).
    두 동작을 모두 보존하려고 empty_base_violates를 호출자가 명시한다.
    """
    if isinstance(value, str):
        return ("profile", value) if value not in names else None
    if isinstance(value, dict):
        base = value.get("profile")
        present = (base is not None) if empty_base_violates else bool(base)
        if present and base not in names:
            return "base", str(base)
    return None


def qos_ref_load_message(kind: str, name: str, where: str) -> str:
    what = "qos profile" if kind == "profile" else "qos.profile"
    return f"{where} references undefined {what} '{name}'."


def qos_ref_resolve_message(kind: str, name: str) -> str:
    if kind == "profile":
        return f"qos references undefined profile '{name}'"
    return f"qos.profile references undefined profile '{name}'"


def _label(item: dict[str, Any]) -> str:
    return item.get("name") or item.get("match") or "?"


def _upper_qos(d: dict[str, Any]) -> None:
    for k in QOS_ENUM_KEYS:
        if isinstance(d.get(k), str):
            d[k] = d[k].upper()


def _iter_qos_values(cfg: dict[str, Any]) -> Iterator[tuple[str, str, Any]]:
    """qos_profiles 밖의 모든 `qos:` 값을 전수 순회(문자열 프로파일 참조 포함) —
    토픽/서비스 항목, 액션 채널 맵, defaults 블록까지 빠짐없이 포함해야 한다.

    (정규화·lease 검사용 라벨, 참조 검사용 라벨, 값)을 낸다. 라벨이 둘인 이유:
    기존 오류 문구가 검사 종류별로 다른 좌표 표기를 쓴다 — 문구 보존.
    인라인 dict만 필요한 소비자는 isinstance로 거른다(검증 전 호출되는
    케이스 정규화가 임의 타입을 만나도 안전해야 한다).
    """
    bridge = cfg.get("bridge") or {}
    for kind in ("topics", "services"):
        for i, item in enumerate(bridge.get(kind, []) or []):
            q = item.get("qos")
            if q is not None:
                yield f"bridge.{kind}[{i}].qos", f"bridge.{kind}[{i}] '{_label(item)}'", q
    for i, item in enumerate(bridge.get("actions", []) or []):
        q = item.get("qos")
        if isinstance(q, dict):  # 액션 qos는 채널 맵만 유효 — 그 외는 스키마 몫
            lab = f"bridge.actions[{i}] '{_label(item)}'"
            for ch, chq in q.items():
                yield f"bridge.actions[{i}].qos.{ch}", f"{lab} qos.{ch}", chq
    for bname, block in (cfg.get("defaults") or {}).items():
        if not isinstance(block, dict):
            continue
        q = block.get("qos")
        if q is None:
            continue
        if bname == "action" and isinstance(q, dict):  # action defaults는 채널 맵
            for ch, chq in q.items():
                yield f"defaults.action.qos.{ch}", f"defaults.action.qos.{ch}", chq
        else:
            yield f"defaults.{bname}.qos", f"defaults.{bname}.qos", q


def _normalize_qos_case(cfg: dict[str, Any]) -> None:
    for prof in (cfg.get("qos_profiles") or {}).values():
        if isinstance(prof, dict):
            _upper_qos(prof)
    for _, _ref, q in _iter_qos_values(cfg):
        if isinstance(q, dict):
            _upper_qos(q)


def _check_qos_references(cfg: dict[str, Any]) -> None:
    qos_names = set((cfg.get("qos_profiles") or {}).keys())
    for _, ref_label, q in _iter_qos_values(cfg):
        bad = undefined_qos_ref(q, qos_names, empty_base_violates=False)
        if bad:
            raise QoSConfigError(qos_ref_load_message(*bad, where=ref_label))


def _check_qos_lease_and_history(cfg: dict[str, Any]) -> None:
    """MANUAL_BY_TOPIC => lease 필수 — qos_profiles뿐 아니라 인라인/defaults
    QoS 맵에도 적용해야 한다(B8). KEEP_ALL+depth는 경고만."""
    profiles = cfg.get("qos_profiles") or {}

    def check(d: dict[str, Any], where: str) -> None:
        if d.get("liveliness") == "MANUAL_BY_TOPIC" and "liveliness_lease_duration_ms" not in d:
            base = profiles.get(d.get("profile") or "") or {}
            if "liveliness_lease_duration_ms" not in base:
                raise QoSConfigError(
                    f"{where}: liveliness MANUAL_BY_TOPIC requires 'liveliness_lease_duration_ms'."
                )
        if d.get("history") == "KEEP_ALL" and "depth" in d:
            log.warning("%s: depth is ignored with history KEEP_ALL", where)

    for name, prof in profiles.items():
        if isinstance(prof, dict):
            check(prof, f"qos_profiles.{name}")
    for label, _ref, q in _iter_qos_values(cfg):
        if isinstance(q, dict):
            check(q, label)


def _effective_qos_dict(value: Any, profiles: dict[str, Any]) -> dict[str, Any]:
    """qos 참조(프로파일 이름 또는 인라인 맵)를 dict 하나로 평탄화."""
    if isinstance(value, str):
        return dict(profiles.get(value) or {})
    if isinstance(value, dict):
        base = dict(profiles.get(value.get("profile") or "") or {})
        base.update({k: v for k, v in value.items() if k != "profile"})
        return base
    return {}


def _check_command_qos(cfg: dict[str, Any]) -> None:
    """command 퍼블리셔는 MANUAL_BY_TOPIC liveliness나 유한 deadline을 요구하면
    안 된다 — IPE에는 assert_liveliness 주기가 없어서 그런 오퍼는 구조적으로
    전달 불가. 술어는 rules.command_qos_violation(resolver와 공유)."""
    profiles = cfg.get("qos_profiles") or {}
    default_cmd_qos = (cfg.get("defaults") or {}).get("topic_command", {}).get("qos")
    for i, item in enumerate(cfg.get("bridge", {}).get("topics") or []):
        if item.get("direction") != "command":
            continue
        eff = _effective_qos_dict(item.get("qos", default_cmd_qos), profiles)
        violation = command_qos_violation(eff.get("liveliness"), eff.get("deadline_ms"))
        if violation:
            raise QoSConfigError(
                command_qos_load_message(violation, f"bridge.topics[{i}] '{_label(item)}'")
            )


def _parse_qos_profiles(raw: dict[str, Any]) -> dict[str, QoSSpec]:
    out: dict[str, QoSSpec] = {}
    for name, d in raw.items():
        # 내장 기본값 위에 프로파일 키가 덮어씀; profile=출처 이름(pfRef)
        out[name] = QoSSpec(profile=name).merged(d)
    return out


def _resolve_qos(value: Any, profiles: dict[str, QoSSpec], missing: QoSSpec) -> QoSSpec:
    """`qos:` 값 해석. 마법의 'default' 프로파일은 없다: 베이스 없는 인라인
    맵은 내장 안전 베이스 QoSSpec()에서 시작하고, 값이 아예 없으면 호출자가
    고른 fail-safe `missing`을 쓴다."""
    if value is None:
        return missing
    bad = undefined_qos_ref(value, profiles, empty_base_violates=True)
    if bad:
        raise QoSConfigError(qos_ref_resolve_message(*bad))
    if isinstance(value, str):
        return profiles[value]
    if isinstance(value, dict):
        base_name = value.get("profile")
        base = profiles[base_name] if base_name else QoSSpec()
        return base.merged(value)
    raise QoSConfigError(f"qos must be a profile name or a map, got {type(value).__name__}")


def _explicit_qos_fields(value: Any) -> frozenset[str]:
    """Return QoS fields explicitly supplied by a topic rule or profile reference."""
    if isinstance(value, str):
        return frozenset(QoSSpec.__dataclass_fields__) - {"profile"}
    if isinstance(value, dict):
        fields = {key for key in value if key in QoSSpec.__dataclass_fields__}
        if value.get("profile"):
            fields.update(QoSSpec.__dataclass_fields__)
        fields.discard("profile")
        return frozenset(fields)
    return frozenset()


def _action_qos(value: Any, profiles: dict[str, QoSSpec], interface: str) -> dict[str, QoSSpec]:
    """액션 클라이언트 채널별 QoS. 지정하지 않은 채널은 부재로 남긴다
    (= rclpy 채널 기본값)."""
    if not value:
        return {}
    if not isinstance(value, dict):
        raise QoSConfigError(f"action '{interface}': qos must be a channel map (§8.5)")
    out: dict[str, QoSSpec] = {}
    for ch, v in value.items():
        if ch not in ACTION_QOS_CHANNELS:
            raise QoSConfigError(
                f"action '{interface}': unknown qos channel '{ch}' "
                f"(allowed: {', '.join(ACTION_QOS_CHANNELS)})"
            )
        out[ch] = _resolve_qos(v, profiles, QoSSpec())
    return out


def validate_qos_config(config: dict[str, Any]) -> None:
    _check_qos_references(config)
    _check_qos_lease_and_history(config)
    _check_command_qos(config)
