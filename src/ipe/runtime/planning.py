"""결정론적 선택 resolver: 검증된 v2 설정(+발견된 인터페이스) ->
robot 스코프·충돌 검사 완료된 TopicSpec/ServiceSpec/ActionSpec.

우선순위(DESIGN §5.2): 명시적 name > 가장 구체적인 match > defaults >
디스커버리 기본값. 병합은 이 순서의 재귀적 필드 단위 deep-merge(B3: 이긴
규칙은 defaults의 개별 필드만 덮어쓰지, 블록 전체를 갈아치우지 않는다).
유일성은 소유 AE와 표현 뷰까지 포함한 최종 절대 oneM2M 경로로 검사한다
(§4.5, B1) — robot_id는 경로 입력일 뿐 키가 아니다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ipe.core.common import deep_merge as _deep_merge
from ipe.core.models import (
    ActionSpec,
    CommandSafety,
    SampleSpec,
    ServiceSpec,
    SourceTsSpec,
    TopicSpec,
)
from ipe.core.transaction import termination_resolve_message, termination_violation
from ipe.onem2m.client import CSESpec, MqttSpec
from ipe.qos.configuration import (
    QOS_INLINE_SCHEMA,
    QoSConfigError,
    _explicit_qos_fields,
    _parse_qos_profiles,
    command_qos_resolve_message,
    command_qos_violation,
)
from ipe.qos.configuration import (
    _action_qos as _qos_channels,
)
from ipe.qos.configuration import (
    _resolve_qos as _qos_profile,
)
from ipe.qos.models import ACTION_QOS_CHANNELS, QosFcntSpec, QoSSpec
from ipe.runtime.naming import (
    RobotSpec,
    apply_captures,
    interface_segments,
    match_pattern,
    pattern_specificity,
    resolve_robot,
    robot_ae,
    sanitize_segment,
    unresolved_captures,
)

log = logging.getLogger("ipe.runtime.planning")

Discovered = dict[str, Any]


@dataclass
class ResolvedConfig:
    instance_id: str
    cse: CSESpec
    notification_host: str
    notification_port: int
    robots: dict[str, RobotSpec]
    qos_profiles: dict[str, QoSSpec]
    naming: dict[str, Any]
    discovery: dict[str, Any]
    defaults: dict[str, Any]
    policy: dict[str, Any]
    recovery: dict[str, Any]
    dispatch: dict[str, Any] = field(default_factory=lambda: {"drain_budget": 32})
    storage: dict[str, Any] = field(default_factory=dict)
    logging: dict[str, Any] = field(default_factory=dict)
    robots_strict: bool = False
    topics: list[TopicSpec] = field(default_factory=list)
    services: list[ServiceSpec] = field(default_factory=list)
    actions: list[ActionSpec] = field(default_factory=list)
    qos_fcnt: QosFcntSpec = field(default_factory=QosFcntSpec)
    raw: dict[str, Any] = field(default_factory=dict)


class ResolveError(Exception):
    pass


# 어떤 규칙도 선택하지 않은 인터페이스용 내장 fail-safe 기본값(B9):
# BEST_EFFORT는 RELIABLE/BEST_EFFORT 퍼블리셔 모두와 매칭되므로, qos 없는
# observe 토픽이 조용히 0건 매칭이 되는 일은 없다.
_BUILTIN_SENSOR_DATA = QoSSpec(
    reliability="BEST_EFFORT", durability="VOLATILE", history="KEEP_LAST", depth=5
)
_DISCOVERY_REPRESENTATION = "latest"
_BUILTIN_FEEDBACK_SAMPLE_MS = 500

# 내장 deny: 숨김 인터페이스('_'로 시작하는 세그먼트 — */_action/* 내부 포함)와
# 노드별 파라미터 서비스 6종. 모드와 무관하게 주입되며 명시적 `name` 항목은
# 여전히 이긴다.
_PARAM_SERVICE_SUFFIXES = frozenset(
    {
        "get_parameters",
        "set_parameters",
        "list_parameters",
        "describe_parameters",
        "get_parameter_types",
        "set_parameters_atomically",
    }
)


def _builtin_denied(kind: str, interface: str) -> bool:
    segs = [s for s in interface.split("/") if s]
    if any(s.startswith("_") for s in segs):
        return True
    if kind == "topics" and interface in ("/rosout", "/parameter_events"):
        return True
    return kind == "services" and bool(segs) and segs[-1] in _PARAM_SERVICE_SUFFIXES


# ---------------------------------------------------------------------------
# QoS
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 규칙 수집 + 병합
# ---------------------------------------------------------------------------


def _matching_rules(
    interface: str, rules: list[dict[str, Any]]
) -> list[tuple[int, int, dict[str, Any], dict[str, str]]]:
    """`interface`를 선택하는 규칙들의 [(rank, order, rule, captures)].

    rank: 명시적 name = 2M, 패턴 매치 = 1M + 구체성. 이후 (rank, order)로
    정렬해 승자를 결정론적으로 만든다.
    """
    out: list[tuple[int, int, dict[str, Any], dict[str, str]]] = []
    for order, rule in enumerate(rules):
        if "name" in rule:
            if rule["name"] == interface:
                out.append((2_000_000, order, rule, {}))
        elif "match" in rule:
            caps = match_pattern(rule["match"], interface)
            if caps is not None:
                out.append((1_000_000 + pattern_specificity(rule["match"]), order, rule, caps))
    return out


def _merge_rules(
    interface: str,
    matches: list[tuple[int, int, dict[str, Any], dict[str, str]]],
) -> tuple[dict[str, Any], dict[str, str], str]:
    """우선순위 순서의 재귀적 필드 단위 deep-merge — 높은 쪽이 이긴다.

    (병합된 필드, 병합된 캡처, 승자 규칙 라벨)을 반환.
    """
    # 낮은 우선순위부터 적용해 최고 우선순위가 마지막에 덮어쓰게 한다
    ordered = sorted(matches, key=lambda m: (m[0], -m[1]))
    merged: dict[str, Any] = {}
    captures: dict[str, str] = {}
    for _rank, _order, rule, caps in ordered:
        fields = {k: v for k, v in rule.items() if k not in ("name", "match")}
        merged = _deep_merge(merged, fields)
        captures = {**captures, **caps}
    if len(ordered) >= 2 and ordered[-1][0] == ordered[-2][0]:
        log.warning(
            "rules '%s' and '%s' match '%s' with equal specificity — "
            "list order decides (later entry wins)",
            ordered[-2][2].get("name") or ordered[-2][2].get("match"),
            ordered[-1][2].get("name") or ordered[-1][2].get("match"),
            interface,
        )
    winner = ordered[-1][2]
    src = winner.get("name") or winner.get("match") or "?"
    return merged, captures, src


# ---------------------------------------------------------------------------
# 후보 인터페이스 집합
# ---------------------------------------------------------------------------


def _candidates(
    kind: str,
    rules: list[dict[str, Any]],
    discovered: Discovered | None,
    mode: str,
    allow: list[str],
    deny: list[str],
) -> dict[str, list[str]]:
    """interface -> 광고된 타입 목록. 명시적 name은 항상 포함. deny(내장+설정)는
    발견된 후보는 이기지만 명시적 name은 절대 못 이긴다(경고로만 드러낸다)."""
    cand: dict[str, list[str]] = {}
    explicit = {r["name"]: r for r in rules if "name" in r}
    disc = dict(discovered.get(kind, []) if discovered else [])

    for name, rule in explicit.items():
        # config-only에서는 명시 목록 자체가 전체 권위다. 내장 auto-discovery
        # deny(파라미터 서비스 등)를 의도적으로 재정의하는 정상 사용이므로 매
        # refresh마다 경고하지 않는다. 사용자가 적은 deny와 충돌하면 계속 경고한다.
        builtin_conflict = mode != "config-only" and _builtin_denied(kind, name)
        if builtin_conflict or _denied(name, deny):
            log.warning(
                "explicit %s entry '%s' matches a deny pattern — explicit names are "
                "never denied (§5.2); remove the entry to stop bridging it",
                kind,
                name,
            )
        cand[name] = disc.get(name, [rule["type"]] if rule.get("type") else [])

    if mode in ("auto-expose", "hybrid") and discovered is not None:
        for name, types in disc.items():
            if name in cand:
                continue
            if _builtin_denied(kind, name) or _denied(name, deny) or not _allowed(name, allow):
                continue
            # hybrid: 패턴이 선택한 발견 항목만 노출; auto-expose: 전부
            if mode == "hybrid" and not any(
                "match" in r and match_pattern(r["match"], name) for r in rules
            ):
                continue
            cand[name] = types
    return cand


def _allowed(interface: str, allow: list[str]) -> bool:
    return any(match_pattern(p, interface) is not None for p in allow)


def _denied(interface: str, deny: list[str]) -> bool:
    return any(match_pattern(p, interface) is not None for p in deny)


# ---------------------------------------------------------------------------
# robot 식별: robot: > {robot} 캡처 > namespace prefix > 기본 robot
# ---------------------------------------------------------------------------


def _robot_for(
    merged: dict[str, Any],
    interface: str,
    captures: dict[str, str],
    robots: list[RobotSpec],
    by_id: dict[str, RobotSpec],
    strict: bool,
    owner_namespaces: list[str] | None = None,
    infer_interface_namespace: bool = False,
) -> RobotSpec:
    rid = merged.get("robot")
    if rid:
        if rid not in by_id:
            raise ResolveError(f"interface '{interface}' references unknown robot '{rid}'")
        return by_id[rid]
    cap = captures.get("robot")
    if cap:
        if cap in by_id:
            return by_id[cap]
        # 등록된 robot의 namespace 세그먼트를 가리키는 캡처는 새 robot이 아니라
        # 그 robot이다 (generic 프로파일이 r1 <- /robot1 식으로 등록)
        for r in robots:
            if r.namespace.rstrip("/") == "/" + cap:
                return r
        if strict:
            raise ResolveError(
                f"interface '{interface}': captured robot '{cap}' is not in robots[] "
                f"and robots_strict is true (known: {sorted(by_id)})"
            )
        ns = "/" + cap if (interface == "/" + cap or interface.startswith("/" + cap + "/")) else ""
        dyn = RobotSpec(id=cap, namespace=ns)
        by_id[cap] = dyn
        robots.append(dyn)
        log.info(
            "dynamic robot '%s' registered from {robot} capture (interface '%s')", cap, interface
        )
        return dyn
    # 설정 비의존 discovery에서는 원격 endpoint node namespace가 robot 경계다.
    # root namespace('/')는 robot 식별 정보를 주지 않으므로 catch-all robot을 쓴다.
    namespaces = sorted(
        {x.rstrip("/") for x in (owner_namespaces or []) if x and x.startswith("/") and x != "/"},
        key=len,
        reverse=True,
    )
    if len(namespaces) == 1:
        namespace = namespaces[0]
        for robot in robots:
            if robot.namespace.rstrip("/") == namespace:
                return robot
        if not strict:
            robot_id = sanitize_segment(namespace.strip("/").replace("/", "_"))
            if robot_id in by_id:
                return by_id[robot_id]
            dyn = RobotSpec(id=robot_id, namespace=namespace)
            by_id[robot_id] = dyn
            robots.append(dyn)
            log.info(
                "dynamic robot '%s' registered from endpoint namespace '%s'", robot_id, namespace
            )
            return dyn
    fallback = resolve_robot(interface, robots)
    # 일부 RMW는 endpoint의 node namespace를 UNKNOWN으로만 제공한다. 설정 없는
    # 자동 Discovery에서는 /{robot}/... 형태의 ROS 이름이 가진 첫 namespace를
    # robot 경계로 사용할 수 있다. 단일 세그먼트(/scan)는 추론하지 않는다.
    parts = [part for part in interface.strip("/").split("/") if part]
    if (
        infer_interface_namespace
        and not strict
        and len(parts) > 1
        and fallback.id == "robot"
        and not fallback.namespace
    ):
        namespace = f"/{parts[0]}"
        robot_id = sanitize_segment(parts[0])
        if robot_id in by_id:
            return by_id[robot_id]
        dyn = RobotSpec(id=robot_id, namespace=namespace)
        by_id[robot_id] = dyn
        robots.append(dyn)
        log.info("dynamic robot '%s' inferred from interface namespace '%s'", robot_id, namespace)
        return dyn
    return fallback


# ---------------------------------------------------------------------------
# 경로
# ---------------------------------------------------------------------------


def _substituted(template: str, captures: dict[str, str], interface: str, what: str) -> str:
    out = apply_captures(template, captures)
    leftover = unresolved_captures(out)
    if leftover:
        raise ResolveError(
            f"interface '{interface}': {what} '{template}' has unresolved "
            f"capture(s) {leftover} — captures must come from this rule's "
            f"'match' pattern (§4.4.3)"
        )
    return out


def _rel_path(
    robot: RobotSpec,
    interface: str,
    merged: dict[str, Any],
    naming: dict[str, Any],
    captures: dict[str, str],
) -> tuple[str, str]:
    sanitize = naming.get("sanitize", "_")
    style = naming.get("path_style", "nested")
    if merged.get("path"):
        templ = _substituted(merged["path"], captures, interface, "path")
        segs = [sanitize_segment(s, sanitize) for s in templ.strip("/").split("/") if s]
    else:
        alias = merged.get("alias") or merged.get("alias_template")
        if alias:
            alias = _substituted(alias, captures, interface, "alias")
        segs = interface_segments(robot, interface, style, sanitize, alias)
    leaf = segs[-1] if segs else sanitize_segment(robot.id)
    return "/".join(segs), leaf


# ---------------------------------------------------------------------------
# 소형 필드 빌더
# ---------------------------------------------------------------------------


def _sample(d: dict[str, Any] | None) -> SampleSpec | None:
    if not d:
        return None
    return SampleSpec(rate_hz=d.get("rate_hz"), min_interval_ms=d.get("min_interval_ms"))


def _source_ts(d: dict[str, Any] | None) -> SourceTsSpec | None:
    if not d:
        return None
    return SourceTsSpec(field=d.get("field"), format=d.get("format", "ros_time"))


def _access(merged: dict[str, Any], default_confirm: str) -> tuple[bool, str]:
    acc = merged.get("access") or {}
    return bool(acc.get("enabled", False)), acc.get("confirm", default_confirm)


def _command_safety(d: dict[str, Any] | None) -> CommandSafety | None:
    if not d:
        return None
    clamp = {k: (float(v[0]), float(v[1])) for k, v in (d.get("clamp") or {}).items()}
    return CommandSafety(
        rate_limit_hz=d.get("rate_limit_hz"),
        clamp=clamp,
        watchdog_ms=d.get("watchdog_ms"),
        max_age_ms=d.get("max_age_ms", 5000),
        liveliness_lease_ms=d.get("liveliness_lease_ms"),
    )


def _pick_type(merged: dict[str, Any], types: list[str]) -> tuple[str | None, bool]:
    """(확정 타입, 모호 여부). 고정 핀이 이기고, 아니면 유일한 발견 타입, 아니면 None."""
    pin = merged.get("type")
    if pin:
        return pin, False
    if len(types) == 1:
        return types[0], False
    if len(types) > 1:
        return None, True
    return None, False


def _type_and_source(merged: dict[str, Any], types: list[str], src: str) -> tuple[str | None, str]:
    """(바인딩 타입, source_rule 라벨) — 모호하면 (None, 'AMBIGUOUS_TYPE:t1,t2')."""
    mtype, ambiguous = _pick_type(merged, types)
    if ambiguous:
        return None, "AMBIGUOUS_TYPE:" + ",".join(types)
    return mtype, src


# ---------------------------------------------------------------------------
# 최상위 resolve
# ---------------------------------------------------------------------------


def _mqtt_spec(cse_c: dict[str, Any]) -> MqttSpec:
    m = cse_c.get("mqtt") or {}
    return MqttSpec(
        host=m.get("host", "127.0.0.1"),
        port=int(m.get("port", 1883)),
        client_id=m.get("client_id", "ros2-ipe"),
        keepalive=int(m.get("keepalive", 60)),
        qos=int(m.get("qos", 1)),
        clean_session=bool(m.get("clean_session", False)),
        topic_prefix=m.get("topic_prefix", ""),
        response_timeout_ms=int(m.get("response_timeout_ms", 5000)),
        connect_timeout_ms=int(m.get("connect_timeout_ms", 10000)),
        max_payload=int(m.get("max_payload", 65536)),
        tls=bool(m.get("tls", False)),
        tls_ca=m.get("tls_ca"),
        tls_cert=m.get("tls_cert"),
        tls_key=m.get("tls_key"),
        tls_insecure=bool(m.get("tls_insecure", False)),
        username=m.get("username"),
        password=m.get("password"),
    )


def resolve(config: dict[str, Any], discovered: Discovered | None = None) -> ResolvedConfig:
    cse_c = config["cse"]
    protocol = cse_c.get("protocol", "http")
    cse_timezone = cse_c.get("timezone", "local")
    if cse_timezone != "local":
        try:
            from zoneinfo import ZoneInfo

            ZoneInfo(cse_timezone)
        except (KeyError, ValueError) as e:
            raise ResolveError(f"invalid cse.timezone: {cse_timezone!r}") from e
    cse = CSESpec(
        endpoint=cse_c.get("endpoint", ""),
        cse_base=cse_c["cse_base"],
        ae_name=cse_c["ae_name"],
        timezone=cse_timezone,
        protocol=protocol,
        cse_id=cse_c.get("cse_id", ""),
        origin=cse_c.get("origin", "CAdmin"),
        rvi=cse_c.get("rvi", "3"),
        poa=cse_c.get("poa", ""),
        mqtt=_mqtt_spec(cse_c) if protocol == "mqtt" else None,
        http_timeout_sec=cse_c.get("http_timeout_sec", 5.0),
        http_max_payload=cse_c.get("http_max_payload", 65536),
    )
    robots = []
    for r in config.get("robots", [{"id": "default"}]):
        ns = r.get("namespace", "")
        if ns and not ns.startswith("/"):  # raw dict가 직접 들어오는 경우 방어
            ns = "/" + ns
        robots.append(
            RobotSpec(
                id=r["id"],
                namespace=ns,
                ae_per_robot=r.get("ae_per_robot", False),
                ae_name=r.get("ae_name"),
            )
        )
    by_id = {r.id: r for r in robots}
    strict = bool(config.get("robots_strict", False))
    profiles = _parse_qos_profiles(config["qos_profiles"])
    naming = config.get("naming", {"path_style": "nested", "sanitize": "_"})
    discovery = config.get("discovery", {"mode": "hybrid", "allow": ["/**"], "deny": []})
    defaults = config.get("defaults", {})
    policy = config.get("policy", {})
    mode = discovery.get("mode", "hybrid")
    allow = discovery.get("allow", ["/**"])
    deny = discovery.get("deny", [])
    refresh = discovery.get("refresh_sec", 5) or 0
    default_confirm = policy.get("confirmation", "auto")

    bridge = config.get("bridge", {})
    ctx = _Ctx(
        robots,
        by_id,
        strict,
        profiles,
        naming,
        defaults,
        default_confirm,
        mode,
        allow,
        deny,
        refresh,
        policy,
    )
    topics = _resolve_topics(bridge.get("topics", []), discovered, ctx)
    services = _resolve_services(bridge.get("services", []), discovered, ctx)
    actions = _resolve_actions(bridge.get("actions", []), discovered, ctx)

    _check_collisions(topics, services, actions, by_id, cse.ae_name)
    if discovered is not None and mode == "auto-expose":
        # 설정 없는 실행에서는 실제 Binding Plan에 포함된 Robot만 Skeleton을
        # 만든다. Namespace에서 동적으로 식별된 경우 기본 catch-all Robot은
        # 리소스 트리에 남기지 않는다.
        used_robot_ids = {spec.robot_id for group in (topics, services, actions) for spec in group}
        by_id = {robot_id: robot for robot_id, robot in by_id.items() if robot_id in used_robot_ids}

    qf = config.get("qos_fcnt", {})
    qos_fcnt = QosFcntSpec(
        enabled=bool(qf.get("enabled", True)),
        type=qf.get("type", "ros:tqos"),
        cnd=qf.get("cnd", "kr.ac.sejong.seslab.ros2.moduleclass.topicQos"),
        service_type=qf.get("service_type", "ros:sqos"),
        service_cnd=qf.get("service_cnd", "kr.ac.sejong.seslab.ros2.moduleclass.serviceQos"),
        action_type=qf.get("action_type", "ros:aqos"),
        action_cnd=qf.get("action_cnd", "kr.ac.sejong.seslab.ros2.moduleclass.actionQos"),
        lbl_compat=bool(qf.get("lbl_compat", True)),
        allow_update=bool(qf.get("allow_update", False)),
        publish_min_interval_ms=int(qf.get("publish_min_interval_ms", 5000)),
        peers_max=int(qf.get("peers_max", 8)),
    )

    return ResolvedConfig(
        instance_id=config.get("ipe", {}).get("instance_id", "ros2-ipe"),
        cse=cse,
        notification_host=config.get("notification_server", {}).get("host", "0.0.0.0"),
        notification_port=config.get("notification_server", {}).get("port", 5050),
        robots=by_id,
        qos_profiles=profiles,
        naming=naming,
        discovery=discovery,
        defaults=defaults,
        policy=policy,
        recovery=config.get("recovery", {}),
        dispatch=config.get("dispatch", {"drain_budget": 32}),
        storage=config.get("storage", {}),
        logging=config.get("logging", {}),
        robots_strict=strict,
        topics=topics,
        services=services,
        actions=actions,
        qos_fcnt=qos_fcnt,
        raw=config,
    )


class _Ctx:
    """세 종류별 resolver가 공유하는 해석 컨텍스트."""

    __slots__ = (
        "robots",
        "by_id",
        "strict",
        "profiles",
        "naming",
        "defaults",
        "default_confirm",
        "mode",
        "allow",
        "deny",
        "refresh",
        "policy",
    )

    def __init__(
        self,
        robots,
        by_id,
        strict,
        profiles,
        naming,
        defaults,
        default_confirm,
        mode,
        allow,
        deny,
        refresh,
        policy,
    ):
        self.robots = robots
        self.by_id = by_id
        self.strict = strict
        self.profiles = profiles
        self.naming = naming
        self.defaults = defaults
        self.default_confirm = default_confirm
        self.mode = mode
        self.allow = allow
        self.deny = deny
        self.refresh = refresh
        self.policy = policy


def _merged_for(
    interface: str,
    rules: list[dict[str, Any]],
    mode: str,
) -> tuple[dict[str, Any], dict[str, str], str] | None:
    matches = _matching_rules(interface, rules)
    if matches:
        return _merge_rules(interface, matches)
    if mode == "config-only":
        return None
    return {}, {}, "discovery-default"


def _iter_candidates(kind, rules, discovered, ctx: _Ctx, dblock, check=None):
    """Merge discovery candidates with defaults before resolving robot paths."""
    cand = _candidates(kind, rules, discovered, ctx.mode, ctx.allow, ctx.deny)
    for interface, types in cand.items():
        hit = _merged_for(interface, rules, ctx.mode)
        if hit is None:
            continue
        merged, caps, src = hit
        if kind == "topics" and "direction" not in merged and discovered is not None:
            direction = discovered.get("topic_directions", {}).get(interface)
            if direction in ("observe", "command", "both"):
                merged["direction"] = direction
        block = dblock(merged) if callable(dblock) else dblock
        merged = _deep_merge(block, merged)
        if check is not None:
            check(interface, merged)
        owners = (discovered or {}).get("owners", {}).get(kind, {}).get(interface, [])
        robot = _robot_for(
            merged,
            interface,
            caps,
            ctx.robots,
            ctx.by_id,
            ctx.strict,
            owner_namespaces=owners,
            infer_interface_namespace=(src == "discovery-default"),
        )
        rel, leaf = _rel_path(robot, interface, merged, ctx.naming, caps)
        yield interface, types, merged, caps, src, robot, rel, leaf


def _resolve_topics(rules, discovered, ctx: _Ctx) -> list[TopicSpec]:
    """Resolve topic bindings, including both-direction control defaults."""

    def dblock(fields: dict[str, Any]) -> dict[str, Any]:
        direction = fields.get("direction", "observe")
        observe = dict(ctx.defaults.get("topic_observe", {}))
        command = dict(ctx.defaults.get("topic_command", {}))
        if direction == "command":
            return command
        if direction == "both":
            command_controls = {
                key: command[key] for key in ("access", "command") if key in command
            }
            return _deep_merge(observe, command_controls)
        return observe

    def check(interface: str, merged: dict[str, Any]) -> None:
        rep = merged.get("representation", _DISCOVERY_REPRESENTATION)
        if rep == "sampled" and not merged.get("sample"):
            raise ResolveError(
                f"topic '{interface}': merged representation is 'sampled' but no "
                f"'sample' block survives the merge (§18.4 ①)"
            )

    out: list[TopicSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
        "topics", rules, discovered, ctx, dblock, check
    ):
        direction = merged.get("direction", "observe")
        representation = merged.get("representation", _DISCOVERY_REPRESENTATION)

        raw_qos = merged.get("qos")
        observe_base = ctx.profiles.get("sensor_data", _BUILTIN_SENSOR_DATA)
        command_base = _resolve_qos(
            ctx.defaults.get("topic_command", {}).get("qos"), ctx.profiles, QoSSpec()
        )
        if direction == "command":
            qos = _resolve_qos(raw_qos, ctx.profiles, command_base)
            command_qos = None
        else:
            qos = _resolve_qos(raw_qos, ctx.profiles, observe_base)
            command_qos = (
                _resolve_qos(raw_qos, ctx.profiles, command_base)
                if direction == "both" and raw_qos is not None
                else command_base
                if direction == "both"
                else None
            )

        if direction in ("command", "both"):
            command_effective = command_qos or qos
            violation = command_qos_violation(
                command_effective.liveliness, command_effective.deadline_ms
            )
            if violation:
                raise ResolveError(command_qos_resolve_message(violation, interface))
        if direction in ("observe", "both") and qos.lifespan_ms is not None:
            log.warning(
                "observe topic '%s': lifespan_ms on a subscription expires samples by "
                "SOURCE timestamp — clock-skewed robots may silently drop everything; "
                "prefer stale_after_ms (§8.4)",
                interface,
            )

        stale = merged.get("stale_after_ms")
        topic_leaf = interface.rsplit("/", 1)[-1]
        exempt_topics = ctx.policy.get("stale_exempt_topics", ["robot_description", "tf_static"])
        if stale is None and direction in ("observe", "both") and topic_leaf not in exempt_topics:
            stale = (
                qos.deadline_ms * ctx.policy.get("stale_deadline_multiplier", 2)
                if qos.deadline_ms
                else ctx.policy.get("default_stale_after_ms", 5000)
            )

        mtype, source_rule = _type_and_source(merged, types, src)
        enabled, confirm = _access(merged, ctx.default_confirm)
        if src == "discovery-default" and direction == "both" and confirm == "auto":
            confirm = "on_first_use"
        out.append(
            TopicSpec(
                robot_id=robot.id,
                interface=interface,
                msg_type=mtype,
                direction=direction,
                representation=representation,
                qos=qos,
                qos_explicit="qos" in merged,
                qos_explicit_fields=_explicit_qos_fields(raw_qos),
                command_qos=command_qos,
                sample=_sample(merged.get("sample")),
                filter=merged.get("filter"),
                selected_fields=merged.get("selected_fields"),
                stale_after_ms=stale,
                source_ts=_source_ts(merged.get("source_ts")),
                flexcontainer=merged.get("flexcontainer"),
                role=merged.get("role"),
                group=merged.get("group"),
                leaf=leaf,
                rel_path=rel,
                command=_command_safety(merged.get("command")),
                access_enabled=enabled,
                confirm=confirm,
                source_rule=source_rule,
            )
        )
    return out


def _resolve_services(rules, discovered, ctx: _Ctx) -> list[ServiceSpec]:
    out: list[ServiceSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
        "services", rules, discovered, ctx, ctx.defaults.get("service", {})
    ):
        timeout = merged.get("timeout_ms", 5000)
        if termination_violation(timeout, ctx.refresh):
            raise ResolveError(termination_resolve_message("service", interface))
        qv = merged.get("qos")
        sqos = _resolve_qos(qv, ctx.profiles, QoSSpec()) if qv is not None else None
        mtype, source_rule = _type_and_source(merged, types, src)
        enabled, confirm = _access(merged, ctx.default_confirm)
        out.append(
            ServiceSpec(
                robot_id=robot.id,
                interface=interface,
                srv_type=mtype,
                qos=sqos,
                timeout_ms=timeout,
                request_fields=merged.get("request_fields"),
                response_fields=merged.get("response_fields"),
                request_template=merged.get("request_template", {}),
                leaf=leaf,
                rel_path=rel,
                access_enabled=enabled,
                confirm=confirm,
                source_rule=source_rule,
            )
        )
    return out


def _resolve_actions(rules, discovered, ctx: _Ctx) -> list[ActionSpec]:
    out: list[ActionSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
        "actions", rules, discovered, ctx, ctx.defaults.get("action", {})
    ):
        timeout = merged.get("timeout_ms", 0)
        if termination_violation(timeout, ctx.refresh):
            raise ResolveError(termination_resolve_message("action", interface))
        feedback = merged.get("feedback", "sampled")
        fsample = _sample(merged.get("feedback_sample"))
        if feedback == "sampled" and fsample is None:
            # sampled 피드백이 항상 게이트를 갖게 하는 디스커버리 기본 폴백
            fsample = SampleSpec(min_interval_ms=_BUILTIN_FEEDBACK_SAMPLE_MS)
        mtype, source_rule = _type_and_source(merged, types, src)
        enabled, confirm = _access(merged, ctx.default_confirm)
        out.append(
            ActionSpec(
                robot_id=robot.id,
                interface=interface,
                action_type=mtype,
                qos=_action_qos(merged.get("qos"), ctx.profiles, interface),
                feedback=feedback,
                feedback_sample=fsample,
                goal_fields=merged.get("goal_fields"),
                feedback_fields=merged.get("feedback_fields"),
                result_fields=merged.get("result_fields"),
                goal_template=merged.get("goal_template", {}),
                timeout_ms=timeout,
                leaf=leaf,
                rel_path=rel,
                access_enabled=enabled,
                confirm=confirm,
                source_rule=source_rule,
            )
        )
    return out


# ---------------------------------------------------------------------------
# 유일성 불변식(B1): 키는 AE까지 포함한 최종 절대 oneM2M 경로 + 뷰 세그먼트이며
# robot_id는 경로 입력일 뿐이다. `both` 표현은 기본 경로와 latest/history 뷰를
# 모두 점유한다.
# ---------------------------------------------------------------------------


def _check_collisions(
    topics: list[TopicSpec],
    services: list[ServiceSpec],
    actions: list[ActionSpec],
    by_id: dict[str, RobotSpec],
    shared_ae: str,
) -> None:
    seen: dict[str, str] = {}

    def chk(robot_id: str, branch: str, rel: str, who: str, views: tuple[str, ...] = ("",)) -> None:
        ae = robot_ae(by_id[robot_id], shared_ae)
        for view in views:
            # 공유 AE에서도 robot CNT가 최상위 격리 경계다. rel_path에는 더 이상
            # robot 세그먼트를 중복 저장하지 않는다.
            robot = sanitize_segment(robot_id)
            key = f"{ae}/robots/{robot}/{branch}/{rel}{view}"
            if key in seen:
                raise ResolveError(
                    f"oneM2M path collision: '{key}' claimed by both "
                    f"'{seen[key]}' and '{who}'. Disambiguate with a distinct "
                    f"'path'/'alias', include '{{robot}}' in the path, or enable "
                    f"ae_per_robot (§4.5)."
                )
            seen[key] = who

    for t in topics:
        who = f"{t.interface} (robot={t.robot_id})"
        # "/qos" 뷰는 qos FCNT 자리(QoS_FCNT_설계서 §4.4) — 사용자 경로의 선점을 조기 검출
        views = (
            ("", "/last", "/hist", "/state", "/qos") if t.representation == "both" else ("", "/qos")
        )
        if t.direction in ("observe", "both"):
            chk(t.robot_id, "topics/observe", t.rel_path, who, views)
        if t.direction in ("command", "both"):
            chk(t.robot_id, "topics/command", t.rel_path, who, ("", "/qos"))
    for s in services:
        chk(s.robot_id, "services", s.rel_path, f"{s.interface} (robot={s.robot_id})", ("", "/qos"))
    for a in actions:
        chk(a.robot_id, "actions", a.rel_path, f"{a.interface} (robot={a.robot_id})", ("", "/qos"))


def _resolve_qos(value, profiles, missing):
    try:
        return _qos_profile(value, profiles, missing)
    except QoSConfigError as exc:
        raise ResolveError(str(exc)) from exc


def _action_qos(value, profiles, interface):
    try:
        return _qos_channels(value, profiles, interface)
    except QoSConfigError as exc:
        raise ResolveError(str(exc)) from exc


# Interface rule schema. Partial defaults must not inject per-interface defaults.
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
    "max_age_ms": {"type": "integer", "min": 0},  # 수신 신선도 게이트
    "liveliness_lease_ms": {"type": "integer", "min": 1},  # 로봇 측 IPE 사망 감지
}

ACCESS_SCHEMA: dict[str, Any] = {
    "enabled": {"type": "boolean"},
    "confirm": {
        "type": "string",
        "allowed": ["auto", "required", "on_first_use"],
    },
}

SOURCE_TS_SCHEMA: dict[str, Any] = {
    "field": {"type": "string", "empty": False},  # 메시지 내 점 표기 경로
    "format": {"type": "string", "empty": False},  # 레지스트리 이름; 어댑터가 별칭 추가 가능
}

_QOS_FIELD = {"type": ["string", "dict"], "schema": QOS_INLINE_SCHEMA}

TOPIC_ITEM_SCHEMA: dict[str, Any] = {
    "name": {"type": "string", "empty": False},
    "match": {"type": "string", "empty": False},
    "type": {"type": "string"},  # 선택적 타입 고정; 로드 프로브 대상
    "direction": {"type": "string", "allowed": ["observe", "command", "both"]},
    "representation": {
        "type": "string",
        "allowed": ["historical", "latest", "both", "sampled"],
    },
    "qos": _QOS_FIELD,
    "sample": {"type": "dict", "schema": SAMPLE_SCHEMA},
    "filter": {"type": "dict", "schema": FILTER_SCHEMA},
    "selected_fields": {"type": "list", "schema": {"type": "string"}},
    "stale_after_ms": {"type": "integer", "min": 0},  # 신선도 워치독
    "source_ts": {"type": "dict", "schema": SOURCE_TS_SCHEMA},
    "role": {"type": "string"},
    "group": {"type": "string"},
    "alias": {"type": "string"},
    "alias_template": {"type": "string"},
    "path": {"type": "string"},  # {capture} 포함 가능
    "command": {"type": "dict", "schema": COMMAND_SAFETY_SCHEMA},
    "access": {"type": "dict", "schema": ACCESS_SCHEMA},
    "robot": {"type": "string"},  # 명시적 robot 오버라이드
    # FCNT 매핑 선언 — representation latest/both에서만 (loader 교차검증)
    "flexcontainer": {
        "type": "dict",
        "schema": {
            "type": {"type": "string", "required": True, "empty": False},
            "cnd": {"type": "string", "required": True, "empty": False},
            "field_map": {
                "type": "dict",
                "required": True,
                "minlength": 1,
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
    "qos": _QOS_FIELD,  # rclpy Client의 qos_profile
    "request_fields": {"type": "list", "schema": {"type": "string"}},
    "response_fields": {"type": "list", "schema": {"type": "string"}, "nullable": True},
    "request_template": {"type": "dict"},
    "alias": {"type": "string"},
    "path": {"type": "string"},
    "access": {"type": "dict", "schema": ACCESS_SCHEMA},
    "robot": {"type": "string"},
}

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

_NOT_IN_DEFAULTS = (
    "name",
    "match",
    "robot",
    "path",
    "alias",
    "alias_template",
    "direction",
    "type",
)


def _defaults_fields(item_schema: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in item_schema.items() if k not in _NOT_IN_DEFAULTS}


DEFAULTS_SCHEMA: dict[str, Any] = {
    "topic_observe": {"type": "dict", "schema": _defaults_fields(TOPIC_ITEM_SCHEMA)},
    "topic_command": {"type": "dict", "schema": _defaults_fields(TOPIC_ITEM_SCHEMA)},
    "service": {"type": "dict", "schema": _defaults_fields(SERVICE_ITEM_SCHEMA)},
    "action": {"type": "dict", "schema": _defaults_fields(ACTION_ITEM_SCHEMA)},
}
