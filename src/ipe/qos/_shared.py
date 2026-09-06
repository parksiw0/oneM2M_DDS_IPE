"""DDS duration and ordered-policy primitives without ROS imports."""

from __future__ import annotations

from typing import Any

INT64_MAX = 2**63 - 1


def is_infinite(duration: Any) -> bool:
    """*duration*이 rmw 무한 센티널(또는 None=미설정)이면 True.

    rclpy Duration, 원시 int 나노초, None을 받는다. 엄격한 센티널 검사라서
    0 ns(rmw "default" 센티널)는 여기서 무한이 아니다 — ``duration_ms``가
    둘 다 None(미설정)으로 접는다.
    """
    if duration is None:
        return True
    ns = duration.nanoseconds if hasattr(duration, "nanoseconds") else int(duration)
    return ns >= INT64_MAX


def duration_ms(duration: Any) -> int | None:
    """duration -> 정수 밀리초; 미설정/무한이면 None.

    rmw 센티널 둘 다 None으로 접는다: INT64_MAX(RMW_DURATION_INFINITE)와
    0(RMW_*_DEFAULT, 기본 생성 QoSProfile이 갖는 값). 실제 유한 QoS
    duration은 0일 수 없다.
    """
    if duration is None:
        return None
    ns = duration.nanoseconds if hasattr(duration, "nanoseconds") else int(duration)
    if ns == 0 or ns >= INT64_MAX:
        return None
    return ns // 1_000_000


def _policy_name(value: Any) -> str:
    """enum 멤버 또는 문자열 -> 정준 대문자 정책 이름."""
    if isinstance(value, str):
        return value.upper()
    name = getattr(value, "name", None)
    if name is not None:
        return str(name)
    raise TypeError(f"unsupported QoS policy value: {value!r}")


def _max_or_none(values: list[int | None]) -> int | None:
    """제공 duration들의 max; 미설정/무한(None)이 하나라도 있으면 None이 지배."""
    if any(v is None for v in values):
        return None
    finite = [v for v in values if v is not None]
    return max(finite) if finite else None


def _min_finite(values: list[int | None]) -> int | None:
    """유한한 요청 duration들의 min; Infinite/미설정은 아예 제외."""
    finite = [v for v in values if v is not None]
    return min(finite) if finite else None


def _dedupe(events: list[str]) -> list[str]:
    """순서 보존 중복 제거(이벤트는 어휘이지 카운트가 아니다)."""
    return list(dict.fromkeys(events))


def choose_kind(peers, field, configured, strengths, *, weakest, explicit=False):
    pool = [
        (strengths[name], name)
        for peer in peers
        if (name := _policy_name(getattr(peer, field))) in strengths
    ]
    if explicit and configured in strengths:
        pool.append((strengths[configured], configured))
    if not pool:
        return configured
    chosen = (min if weakest else max)(pool)[1]
    if not weakest and strengths.get(configured, -1) >= strengths[chosen]:
        return configured
    return chosen


def requested_duration(offered, field, configured, explicit):
    return (
        configured if explicit else _max_or_none([duration_ms(getattr(p, field)) for p in offered])
    )


def offered_duration(requested, field, configured):
    needed = _min_finite([duration_ms(getattr(p, field)) for p in requested])
    return (
        needed if needed is not None and (configured is None or configured > needed) else configured
    )


def guard_duration(offered, field, configured):
    values = [duration_ms(getattr(p, field)) for p in offered]
    strict = configured is not None and any(v is None or v > configured for v in values)
    return (_max_or_none(values) if strict else configured), strict
