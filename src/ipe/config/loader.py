"""v2 IPE 설정 로드 + 검증.

`load_config`는 검증·케이스 정규화·env 치환이 끝난 설정 dict를 돌려준다.
런타임 스펙 변환은 resolver 몫 — 로드 후 `resolve(config, discovered)`를 부른다.

검증 파이프라인은 순서가 중요하다: schema_version 즉시 실패 → env 치환(B2) →
QoS enum 케이스 정규화 → Cerberus 구조 검증+기본값(B7) → 의미론적 교차
검사(DESIGN §18.4) → 타입 로드 프로브(§3.2, rosidl import 가능할 때만).
"""

from __future__ import annotations

import copy
import logging
import os
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import yaml
from cerberus import Validator

from ipe.config.identity import compile_pattern
from ipe.config.rules import (
    command_qos_load_message,
    command_qos_violation,
    qos_ref_load_message,
    termination_load_message,
    termination_violation,
    undefined_qos_ref,
)
from ipe.config.schema import CONFIG_SCHEMA, QOS_ENUM_KEYS

log = logging.getLogger("ipe.config.loader")


class ConfigError(Exception):
    pass


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ConfigError(f"Config root must be a mapping, got {type(raw).__name__}")
    return validate_config(raw, env=env)


def validate_config(raw: dict[str, Any], env: Mapping[str, str] | None = None) -> dict[str, Any]:
    sv = raw.get("schema_version")
    if sv != 2:
        raise ConfigError(
            f"Unsupported or missing schema_version: {sv!r}. This loader requires "
            f"`schema_version: 2`. Legacy (v1) configs must be migrated — see "
            f"docs/design/DESIGN.md §18.2 for the v1->v2 value map."
        )

    cfg = copy.deepcopy(raw)
    cfg = _substitute_env(cfg, os.environ if env is None else env)
    _normalize_qos_case(cfg)

    v = Validator(CONFIG_SCHEMA, purge_unknown=False)
    if not v.validate(cfg):
        raise ConfigError(f"Schema validation failed: {_fmt_errors(v.errors)}")
    normalized = v.normalized(cfg)

    _normalize_robot_namespaces(normalized)
    _check_semantics(normalized)
    _probe_type_pins(normalized)
    return normalized


# ---------------------------------------------------------------------------
# env 참조 치환(B2): 문자열 전체가 ${VAR}일 때만 환경 변수로 치환하고, 미설정
# 변수는 부팅을 중단한다(즉시 실패). 부분 보간("prefix-${VAR}")은 의도적 미지원.
# ---------------------------------------------------------------------------

_ENV_REF = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _substitute_env(node: Any, env: Mapping[str, str], path: str = "") -> Any:
    if isinstance(node, dict):
        return {k: _substitute_env(v, env, f"{path}.{k}" if path else str(k))
                for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_env(v, env, f"{path}[{i}]") for i, v in enumerate(node)]
    if isinstance(node, str):
        m = _ENV_REF.match(node)
        if m:
            var = m.group(1)
            if var not in env:
                raise ConfigError(
                    f"{path}: references environment variable '{var}' which is not set. "
                    f"env-ref substitution is fail-fast (DESIGN §18, B2) — export {var} "
                    f"or replace the placeholder with a literal value."
                )
            return env[var]
    return node


# ---------------------------------------------------------------------------
# 케이스 정규화 (best_effort / BEST_EFFORT 둘 다 허용)
# ---------------------------------------------------------------------------

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
        if isinstance(q, dict):   # 액션 qos는 채널 맵만 유효 — 그 외는 스키마 몫
            lab = f"bridge.actions[{i}] '{_label(item)}'"
            for ch, chq in q.items():
                yield f"bridge.actions[{i}].qos.{ch}", f"{lab} qos.{ch}", chq
    for bname, block in (cfg.get("defaults") or {}).items():
        if not isinstance(block, dict):
            continue
        q = block.get("qos")
        if q is None:
            continue
        if bname == "action" and isinstance(q, dict):   # action defaults는 채널 맵
            for ch, chq in q.items():
                yield f"defaults.action.qos.{ch}", f"defaults.action.qos.{ch}", chq
        else:
            yield f"defaults.{bname}.qos", f"defaults.{bname}.qos", q


def _normalize_qos_case(cfg: dict[str, Any]) -> None:
    for prof in (cfg.get("qos_profiles") or {}).values():
        if isinstance(prof, dict):
            _upper_qos(prof)
    for _label_, _ref, q in _iter_qos_values(cfg):
        if isinstance(q, dict):
            _upper_qos(q)


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

    _check_patterns(cfg)
    _check_mode_semantics(cfg)
    _check_qos_references(cfg)
    _check_filters_and_sampling(cfg)
    _check_qos_lease_and_history(cfg)
    _check_command_qos(cfg)
    _check_termination_invariant(cfg)
    _check_flexcontainer(cfg)


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
    for kind, where, _i, item in _bridge_items(cfg):
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


def _check_qos_references(cfg: dict[str, Any]) -> None:
    qos_names = set((cfg.get("qos_profiles") or {}).keys())
    for _label_, ref_label, q in _iter_qos_values(cfg):
        bad = undefined_qos_ref(q, qos_names, empty_base_violates=False)
        if bad:
            raise ConfigError(qos_ref_load_message(*bad, where=ref_label))


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
        if item.get("feedback") == "sampled" and "feedback_sample" not in item:
            if "feedback_sample" not in (defaults.get("action") or {}):
                raise ConfigError(
                    f"{where} '{_label(item)}' feedback 'sampled' requires 'feedback_sample'."
                )


def _check_qos_lease_and_history(cfg: dict[str, Any]) -> None:
    """MANUAL_BY_TOPIC => lease 필수 — qos_profiles뿐 아니라 인라인/defaults
    QoS 맵에도 적용해야 한다(B8). KEEP_ALL+depth는 경고만."""
    profiles = cfg.get("qos_profiles") or {}

    def check(d: dict[str, Any], where: str) -> None:
        if d.get("liveliness") == "MANUAL_BY_TOPIC" and "liveliness_lease_duration_ms" not in d:
            base = profiles.get(d.get("profile") or "") or {}
            if "liveliness_lease_duration_ms" not in base:
                raise ConfigError(
                    f"{where}: liveliness MANUAL_BY_TOPIC requires "
                    f"'liveliness_lease_duration_ms'."
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
    for i, item in enumerate((cfg.get("bridge", {}).get("topics") or [])):
        if item.get("direction") != "command":
            continue
        eff = _effective_qos_dict(item.get("qos", default_cmd_qos), profiles)
        violation = command_qos_violation(eff.get("liveliness"), eff.get("deadline_ms"))
        if violation:
            raise ConfigError(command_qos_load_message(
                violation, f"bridge.topics[{i}] '{_label(item)}'"))


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
                where, _label(item), pin, e,
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
                if isinstance(item, (dict, list)):
                    walk(item, path)
                else:
                    lines.append(f"{path}: {item}")
        else:
            lines.append(f"{path}: {e}")
    walk(errors, prefix)
    return "; ".join(lines)
