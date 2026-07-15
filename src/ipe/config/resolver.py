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
from typing import Any

from ipe.core.common import deep_merge as _deep_merge
from ipe.config.identity import (
    apply_captures,
    interface_segments,
    match_pattern,
    pattern_specificity,
    resolve_robot,
    robot_ae,
    robot_root,
    sanitize_segment,
    unresolved_captures,
)
from ipe.config.rules import (
    command_qos_resolve_message,
    command_qos_violation,
    qos_ref_resolve_message,
    termination_resolve_message,
    termination_violation,
    undefined_qos_ref,
)
from ipe.config.spec import (
    ACTION_QOS_CHANNELS,
    ActionSpec,
    CommandSafety,
    CSESpec,
    MqttSpec,
    QosFcntSpec,
    QoSSpec,
    ResolvedConfig,
    RobotSpec,
    SampleSpec,
    ServiceSpec,
    SourceTsSpec,
    TopicSpec,
)

log = logging.getLogger("ipe.config.resolver")

Discovered = dict[str, list[tuple[str, list[str]]]]


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
_DEFAULT_STALE_AFTER_MS = 5000

# 내장 deny: 숨김 인터페이스('_'로 시작하는 세그먼트 — */_action/* 내부 포함)와
# 노드별 파라미터 서비스 6종. 모드와 무관하게 주입되며 명시적 `name` 항목은
# 여전히 이긴다.
_PARAM_SERVICE_SUFFIXES = frozenset({
    "get_parameters", "set_parameters", "list_parameters",
    "describe_parameters", "get_parameter_types", "set_parameters_atomically",
})


def _builtin_denied(kind: str, interface: str) -> bool:
    segs = [s for s in interface.split("/") if s]
    if any(s.startswith("_") for s in segs):
        return True
    return kind == "services" and bool(segs) and segs[-1] in _PARAM_SERVICE_SUFFIXES


# ---------------------------------------------------------------------------
# QoS
# ---------------------------------------------------------------------------

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
        raise ResolveError(qos_ref_resolve_message(*bad))
    if isinstance(value, str):
        return profiles[value]
    if isinstance(value, dict):
        base_name = value.get("profile")
        base = profiles[base_name] if base_name else QoSSpec()
        return base.merged(value)
    raise ResolveError(f"qos must be a profile name or a map, got {type(value).__name__}")


def _action_qos(value: Any, profiles: dict[str, QoSSpec], interface: str) -> dict[str, QoSSpec]:
    """액션 클라이언트 채널별 QoS. 지정하지 않은 채널은 부재로 남긴다
    (= rclpy 채널 기본값)."""
    if not value:
        return {}
    if not isinstance(value, dict):
        raise ResolveError(f"action '{interface}': qos must be a channel map (§8.5)")
    out: dict[str, QoSSpec] = {}
    for ch, v in value.items():
        if ch not in ACTION_QOS_CHANNELS:
            raise ResolveError(
                f"action '{interface}': unknown qos channel '{ch}' "
                f"(allowed: {', '.join(ACTION_QOS_CHANNELS)})"
            )
        out[ch] = _resolve_qos(v, profiles, QoSSpec())
    return out


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
        if _builtin_denied(kind, name) or _denied(name, deny):
            log.warning(
                "explicit %s entry '%s' matches a deny pattern — explicit names are "
                "never denied (§5.2); remove the entry to stop bridging it", kind, name,
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
        log.info("dynamic robot '%s' registered from {robot} capture (interface '%s')",
                 cap, interface)
        return dyn
    return resolve_robot(interface, robots)


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
    root = robot_root(robot)
    if merged.get("path"):
        templ = _substituted(merged["path"], captures, interface, "path")
        segs = [sanitize_segment(s, sanitize) for s in templ.strip("/").split("/") if s]
        # {robot} 캡처도 robot 자신의 세그먼트도 없는 path에는 robot 루트를
        # 앞에 붙인다 (멀티 robot 충돌 가드)
        if root and "{robot}" not in merged["path"] and root not in segs:
            segs = [root, *segs]
    else:
        alias = merged.get("alias") or merged.get("alias_template")
        if alias:
            alias = _substituted(alias, captures, interface, "alias")
        segs = interface_segments(robot, interface, style, sanitize, alias)
        if root:
            segs = [root, *segs]
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
    cse = CSESpec(
        endpoint=cse_c.get("endpoint", ""),
        cse_base=cse_c["cse_base"],
        ae_name=cse_c["ae_name"],
        protocol=protocol,
        cse_id=cse_c.get("cse_id", ""),
        origin=cse_c.get("origin", "CAdmin"),
        rvi=cse_c.get("rvi", "3"),
        poa=cse_c.get("poa", ""),
        mqtt=_mqtt_spec(cse_c) if protocol == "mqtt" else None,
    )
    robots = []
    for r in config.get("robots", [{"id": "default"}]):
        ns = r.get("namespace", "")
        if ns and not ns.startswith("/"):   # raw dict가 직접 들어오는 경우 방어
            ns = "/" + ns
        robots.append(RobotSpec(id=r["id"], namespace=ns,
                                ae_per_robot=r.get("ae_per_robot", False),
                                ae_name=r.get("ae_name")))
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
    ctx = _Ctx(robots, by_id, strict, profiles, naming, defaults, default_confirm,
               mode, allow, deny, refresh)
    topics = _resolve_topics(bridge.get("topics", []), discovered, ctx)
    services = _resolve_services(bridge.get("services", []), discovered, ctx)
    actions = _resolve_actions(bridge.get("actions", []), discovered, ctx)

    _check_collisions(topics, services, actions, by_id, cse.ae_name)

    qf = config.get("qos_fcnt", {})
    qos_fcnt = QosFcntSpec(
        enabled=bool(qf.get("enabled", True)),
        type=qf.get("type", "ros:tqos"),
        cnd=qf.get("cnd", "kr.ac.sejong.seslab.ros2.moduleclass.topicQos"),
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

    __slots__ = ("robots", "by_id", "strict", "profiles", "naming", "defaults",
                 "default_confirm", "mode", "allow", "deny", "refresh")

    def __init__(self, robots, by_id, strict, profiles, naming, defaults,
                 default_confirm, mode, allow, deny, refresh):
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
    """세 종류 resolver의 공통 전처리: 후보 수집 → 규칙 병합 → defaults
    deep-merge(B3: 필드 단위 — defaults는 필드별로 진다) → robot/경로 해석.

    (interface, types, merged, caps, src, robot, rel, leaf)를 낸다.
    dblock은 dict 또는 (병합 전 규칙 필드) -> dict callable — 토픽은 direction에
    따라 defaults 블록이 갈린다. check는 defaults 병합 직후·robot 해석 전에
    실행하는 검사 훅 — 기존 오류 발생 순서 보존용(토픽 representation 검사는
    robot 해석보다 먼저 걸려야 한다).
    """
    cand = _candidates(kind, rules, discovered, ctx.mode, ctx.allow, ctx.deny)
    for interface, types in cand.items():
        hit = _merged_for(interface, rules, ctx.mode)
        if hit is None:
            continue
        merged, caps, src = hit
        block = dblock(merged) if callable(dblock) else dblock
        merged = _deep_merge(block, merged)
        if check is not None:
            check(interface, merged)
        robot = _robot_for(merged, interface, caps, ctx.robots, ctx.by_id, ctx.strict)
        rel, leaf = _rel_path(robot, interface, merged, ctx.naming, caps)
        yield interface, types, merged, caps, src, robot, rel, leaf


def _resolve_topics(rules, discovered, ctx: _Ctx) -> list[TopicSpec]:
    def dblock(fields: dict[str, Any]) -> dict[str, Any]:
        # direction은 defaults 블록에 못 들어가므로(schema._NOT_IN_DEFAULTS)
        # 병합 전후가 같다 — 블록 선택은 병합 전 값으로 한다
        return ctx.defaults.get(
            "topic_command" if fields.get("direction", "observe") == "command"
            else "topic_observe", {})

    def check(interface: str, merged: dict[str, Any]) -> None:
        rep = merged.get("representation", _DISCOVERY_REPRESENTATION)
        if rep == "sampled" and not merged.get("sample"):
            raise ResolveError(
                f"topic '{interface}': merged representation is 'sampled' but no "
                f"'sample' block survives the merge (§18.4 ①)"
            )

    out: list[TopicSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
            "topics", rules, discovered, ctx, dblock, check):
        direction = merged.get("direction", "observe")
        representation = merged.get("representation", _DISCOVERY_REPRESENTATION)

        # fail-safe QoS(B9): observe는 sensor_data가 기본(설정 프로파일이 있으면
        # 그것, 없으면 내장); command는 신뢰성 있는 베이스가 기본.
        if direction == "command":
            missing = QoSSpec()
        else:
            missing = ctx.profiles.get("sensor_data", _BUILTIN_SENSOR_DATA)
        qos = _resolve_qos(merged.get("qos"), ctx.profiles, missing)

        if direction == "command":
            # 병합 후 최종값으로 가드 재검사 (규칙 조합으로 새로 생길 수 있음);
            # 술어는 rules.command_qos_violation(loader와 공유)
            violation = command_qos_violation(qos.liveliness, qos.deadline_ms)
            if violation:
                raise ResolveError(command_qos_resolve_message(violation, interface))
        elif qos.lifespan_ms is not None:
            log.warning(
                "observe topic '%s': lifespan_ms on a subscription expires samples by "
                "SOURCE timestamp — clock-skewed robots may silently drop everything; "
                "prefer stale_after_ms (§8.4)", interface,
            )

        stale = merged.get("stale_after_ms")
        if stale is None and direction in ("observe", "both"):
            # 기본값: deadline이 있으면 그 2배, 없으면 5초
            stale = qos.deadline_ms * 2 if qos.deadline_ms else _DEFAULT_STALE_AFTER_MS

        mtype, source_rule = _type_and_source(merged, types, src)
        enabled, confirm = _access(merged, ctx.default_confirm)
        out.append(TopicSpec(
            robot_id=robot.id,
            interface=interface,
            msg_type=mtype,
            direction=direction,
            representation=representation,
            qos=qos,
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
        ))
    return out


def _resolve_services(rules, discovered, ctx: _Ctx) -> list[ServiceSpec]:
    out: list[ServiceSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
            "services", rules, discovered, ctx, ctx.defaults.get("service", {})):
        timeout = merged.get("timeout_ms", 5000)
        if termination_violation(timeout, ctx.refresh):
            raise ResolveError(termination_resolve_message("service", interface))
        qv = merged.get("qos")
        sqos = _resolve_qos(qv, ctx.profiles, QoSSpec()) if qv is not None else None
        mtype, source_rule = _type_and_source(merged, types, src)
        enabled, confirm = _access(merged, ctx.default_confirm)
        out.append(ServiceSpec(
            robot_id=robot.id, interface=interface,
            srv_type=mtype,
            qos=sqos,
            timeout_ms=timeout,
            request_fields=merged.get("request_fields"),
            response_fields=merged.get("response_fields"),
            request_template=merged.get("request_template", {}),
            leaf=leaf, rel_path=rel,
            access_enabled=enabled, confirm=confirm,
            source_rule=source_rule,
        ))
    return out


def _resolve_actions(rules, discovered, ctx: _Ctx) -> list[ActionSpec]:
    out: list[ActionSpec] = []
    for interface, types, merged, _caps, src, robot, rel, leaf in _iter_candidates(
            "actions", rules, discovered, ctx, ctx.defaults.get("action", {})):
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
        out.append(ActionSpec(
            robot_id=robot.id, interface=interface,
            action_type=mtype,
            qos=_action_qos(merged.get("qos"), ctx.profiles, interface),
            feedback=feedback,
            feedback_sample=fsample,
            goal_fields=merged.get("goal_fields"),
            feedback_fields=merged.get("feedback_fields"),
            result_fields=merged.get("result_fields"),
            goal_template=merged.get("goal_template", {}),
            timeout_ms=timeout,
            leaf=leaf, rel_path=rel,
            access_enabled=enabled, confirm=confirm,
            source_rule=source_rule,
        ))
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

    def chk(robot_id: str, branch: str, rel: str, who: str,
            views: tuple[str, ...] = ("",)) -> None:
        ae = robot_ae(by_id[robot_id], shared_ae)
        for view in views:
            key = f"{ae}/{branch}/{rel}{view}"
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
        views = (("", "/latest", "/history", "/qos")
                 if t.representation == "both" else ("", "/qos"))
        if t.direction in ("observe", "both"):
            chk(t.robot_id, "ros2Data", t.rel_path, who, views)
        if t.direction in ("command", "both"):
            chk(t.robot_id, "ros2Command", t.rel_path, who, ("", "/qos"))
    for s in services:
        chk(s.robot_id, "services", s.rel_path, f"{s.interface} (robot={s.robot_id})")
    for a in actions:
        chk(a.robot_id, "actions", a.rel_path, f"{a.interface} (robot={a.robot_id})")
