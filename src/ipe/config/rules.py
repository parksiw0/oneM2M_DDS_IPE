"""§8.5/§18.4 의미 규칙의 술어·안내 문구 정본 — loader와 resolver가 공유.

같은 규칙을 두 계층이 두 번 검사하는 것은 의도다(resolver는 규칙 조합·defaults
병합 후의 최종값으로 재검사). 여기에는 판정 술어와 문구만 두고, loader는
ConfigError로, resolver는 ResolveError로 감싸기만 한다.

불변식: 문구는 테스트가 단언하므로 바이트 단위로 보존해야 한다 — 계층별로
좌표 표기(설정 경로 vs 인터페이스)가 달라 렌더러를 따로 둔다.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

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
# §18.4 ⑧ — 종료 불변식: timeout_ms 0(무제한)은 디스커버리 폴링이 있어야 허용
# ---------------------------------------------------------------------------


def termination_violation(timeout_ms: Any, refresh_sec: Any) -> bool:
    return timeout_ms == 0 and not (refresh_sec and refresh_sec > 0)


def termination_load_message(where: str, label: str) -> str:
    return (
        f"{where} '{label}': timeout_ms 0 (unbounded) requires "
        f"discovery.refresh_sec > 0 — at least one termination mechanism "
        f"must exist (§18.4 ⑧)."
    )


def termination_resolve_message(kind: str, interface: str) -> str:
    return (
        f"{kind} '{interface}': timeout_ms 0 requires discovery.refresh_sec "
        f"> 0 — termination invariant (§18.4 ⑧)"
    )


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
            return "base", base
    return None


def qos_ref_load_message(kind: str, name: str, where: str) -> str:
    what = "qos profile" if kind == "profile" else "qos.profile"
    return f"{where} references undefined {what} '{name}'."


def qos_ref_resolve_message(kind: str, name: str) -> str:
    if kind == "profile":
        return f"qos references undefined profile '{name}'"
    return f"qos.profile references undefined profile '{name}'"
