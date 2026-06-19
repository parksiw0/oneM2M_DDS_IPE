"""ROS2 메시지 <-> IR 변환 + 선언적 source-ts 포맷 레지스트리 (DESIGN §3.3, §3.4)."""

from __future__ import annotations

from typing import Any, Callable

from ipe.ir import TopicIR


def parse_message(msg: Any) -> dict[str, Any]:
    """임의 ROS2 메시지 -> 정준 JSON-safe dict. 타입별 코드 없음."""
    from rosidl_runtime_py import message_to_ordereddict

    from ipe.core.transcode import to_canonical

    return to_canonical(message_to_ordereddict(msg), type(msg))


def make_topic_ir(
    robot_id: str,
    interface_name: str,
    message_type: str,
    payload: dict[str, Any],
    source_ts: float | None,
    ingest_ts: float,
    seq: int,
    metadata: dict[str, Any] | None = None,
) -> TopicIR:
    return TopicIR(
        interface_type="topic",
        robot_id=robot_id,
        interface_name=interface_name,
        message_type=message_type,
        source_ts=source_ts,
        ingest_ts=ingest_ts,
        seq=seq,
        payload=payload,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# source-ts 포맷 레지스트리
#
# 선언적 추출: `source_ts: {field, format}`이 등록된 변환기(원시 필드 값 ->
# epoch 초 | None)를 지정한다. 펌웨어 고유 토큰은 어댑터가 등록하는
# 별칭이다 — 코어는 절대 하드코딩하지 않는다.
# ---------------------------------------------------------------------------

TsFormatFn = Callable[[Any], "float | None"]


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ros_time_dict(value: Any) -> float | None:
    """{sec, nanosec} dict → epoch 초. 그 외 형태는 None — 폴백은 호출자 몫."""
    if isinstance(value, dict) and "sec" in value:
        return value.get("sec", 0) + value.get("nanosec", 0) / 1e9
    return None


def _fmt_ros_time(value: Any) -> float | None:
    # 폴백 차이는 의도: _fmt는 문자열 숫자도 허용(_as_float),
    # _coerce_ros_time은 int/float만 받는다.
    ts = _ros_time_dict(value)
    return ts if ts is not None else _as_float(value)


def _fmt_epoch_seconds(value: Any) -> float | None:
    return _as_float(value)


def _fmt_milliseconds(value: Any) -> float | None:
    f = _as_float(value)
    return None if f is None else f / 1e3


def _fmt_microseconds(value: Any) -> float | None:
    f = _as_float(value)
    return None if f is None else f / 1e6


def _fmt_nanoseconds(value: Any) -> float | None:
    f = _as_float(value)
    return None if f is None else f / 1e9


FORMAT_REGISTRY: dict[str, TsFormatFn] = {
    "ros_time": _fmt_ros_time,
    "epoch_seconds": _fmt_epoch_seconds,
    "milliseconds": _fmt_milliseconds,
    "microseconds": _fmt_microseconds,
    "nanoseconds": _fmt_nanoseconds,
}


def register_ts_format(name: str, fn: TsFormatFn) -> None:
    """source-ts 포맷 변환기 등록/덮어쓰기 (어댑터 확장점)."""
    FORMAT_REGISTRY[name] = fn


# 펌웨어 별칭 등록 예시 (PX4는 epoch 마이크로초를 발행한다).
register_ts_format("px4_microseconds", _fmt_microseconds)


def extract_source_ts(payload: dict[str, Any], field: str | None = None, fmt: str | None = None) -> float | None:
    """메시지 페이로드에서 source 타임스탬프(epoch 초)를 추출한다.

    field가 있으면 ``fmt``가 가리키는 레지스트리 변환기를 적용한다(미등록
    이름은 ValueError — 설정 오류는 시끄럽게 드러나야 한다). field가 없으면
    표준 `header.stamp` / `stamp` {sec, nanosec} 관례를 탐색한다. 쓸 만한
    스탬프가 없으면 None을 반환한다(어댑터는 ingest_ts에 의존).
    """
    if field is not None:
        value = _get_nested(payload, field)
        if value is None:
            return None
        if fmt is None:
            return _coerce_ros_time(value)
        fn = FORMAT_REGISTRY.get(fmt)
        if fn is None:
            raise ValueError(f"unknown source_ts format {fmt!r}; registered: {sorted(FORMAT_REGISTRY)}")
        return fn(value)

    stamp = _get_nested(payload, "header.stamp")
    if stamp is None:
        stamp = payload.get("stamp")
    return _coerce_ros_time(stamp)


def _coerce_ros_time(value: Any) -> float | None:
    ts = _ros_time_dict(value)
    if ts is not None:
        return ts
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _get_nested(d: dict[str, Any], path: str) -> Any:
    current: Any = d
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
        if current is None:
            return None
    return current
