"""rosidl dict 형태 <-> IPE 정준(canonical) JSON 트랜스코더 (DESIGN §3.3).

SLOT_TYPES 인트로스펙션만으로 양방향 변환한다(타입별 코드 없음). 읽기: byte 배열 ->
base64, 스칼라 byte -> int, NaN/Inf는 치환 없이 통과(normalize 소관). 쓰기: 미지 필드·
NaN/Inf·범위/길이 위반 모두 거부, int64_as_string이면 2^53 초과 int64 값은 문자열로 오간다.
"""

from __future__ import annotations

import base64
import binascii
import importlib
from collections.abc import Callable, Mapping
from typing import Any

try:
    from rosidl_parser.definition import (
        AbstractGenericString,
        AbstractNestedType,
        Array,
        BasicType,
        BoundedSequence,
        BoundedString,
        BoundedWString,
        NamespacedType,
    )

    _HAVE_ROSIDL = True
except ImportError:  # ROS 없이도(CI) 임포트는 가능해야 함; 변환 호출 시점에 명시적으로 실패
    _HAVE_ROSIDL = False

__all__ = [
    "TranscodeError",
    "to_canonical",
    "from_canonical",
    "make_input_example",
    "parse_message",
    "extract_source_ts",
    "register_ts_format",
]

_JSON_SAFE_INT_MAX = 2**53

_OCTET_TYPENAMES = frozenset({"octet"})
_BYTE_ARRAY_TYPENAMES = frozenset({"uint8", "octet"})  # 이 타입들의 시퀀스 -> base64
_FLOAT_TYPENAMES = frozenset({"float", "double", "long double"})
_FLOAT32_MAX = 3.402823466e38

_INT_RANGES: dict[str, tuple[int, int]] = {
    "int8": (-(2**7), 2**7 - 1),
    "int16": (-(2**15), 2**15 - 1),
    "int32": (-(2**31), 2**31 - 1),
    "int64": (-(2**63), 2**63 - 1),
    "uint8": (0, 2**8 - 1),
    "uint16": (0, 2**16 - 1),
    "uint32": (0, 2**32 - 1),
    "uint64": (0, 2**64 - 1),
    "octet": (0, 255),
    "char": (0, 255),
    "wchar": (0, 2**16 - 1),
    # C 스타일 IDL 별칭(.idl 기반 타입에서 rosidl이 이 이름들을 낼 수 있음)
    "short": (-(2**15), 2**15 - 1),
    "long": (-(2**31), 2**31 - 1),
    "long long": (-(2**63), 2**63 - 1),
    "unsigned short": (0, 2**16 - 1),
    "unsigned long": (0, 2**32 - 1),
    "unsigned long long": (0, 2**64 - 1),
}

_INT64_TYPENAMES = frozenset({"int64", "uint64", "long long", "unsigned long long"})

# rosidl은 필드 없는 구조체에 이 합성 멤버를 채워 넣는다. 0필드 타입의 정준
# 인코딩은 {}이므로 이 멤버는 경계를 절대 넘지 않는다.
_EMPTY_STRUCT_MEMBER = "structure_needs_at_least_one_member"


class TranscodeError(ValueError):
    """정준 <-> rosidl 변환 실패. 문제가 된 필드 경로를 담는다."""

    def __init__(self, path: str, reason: str):
        self.path = path or "<root>"
        self.reason = reason
        super().__init__(f"{self.path}: {reason}")


# ---------------------------------------------------------------------------
# 인트로스펙션 헬퍼
# ---------------------------------------------------------------------------


def _require_rosidl() -> None:
    if not _HAVE_ROSIDL:
        raise TranscodeError(
            "", "rosidl_parser is required for transcoding (ROS environment not sourced)"
        )


def _iter_fields(msg_class: Any) -> list[tuple[str, Any]]:
    names = list(msg_class.get_fields_and_field_types().keys())
    return list(zip(names, msg_class.SLOT_TYPES, strict=False))


def _resolve_namespaced(nt: NamespacedType) -> Any:
    module = importlib.import_module(".".join(nt.namespaces))
    return getattr(module, nt.name)


def _join(path: str, name: str) -> str:
    return f"{path}.{name}" if path else name


def _is_byte_sequence(slot: AbstractNestedType) -> bool:
    vt = slot.value_type
    return isinstance(vt, BasicType) and vt.typename in _BYTE_ARRAY_TYPENAMES


# ---------------------------------------------------------------------------
# 읽기 경로: rosidl dict 형태 -> 정준 JSON-safe dict
# ---------------------------------------------------------------------------


def to_canonical(
    payload: Mapping[str, Any], msg_class: type, int64_as_string: bool = False
) -> dict[str, Any]:
    """``message_to_ordereddict`` 출력(또는 원시 속성 dict) -> 정준 dict.

    NaN/Inf float는 여기서 치환하지 않고 그대로 통과시킨다(치환은 normalize 소관).
    """
    _require_rosidl()
    return _read_struct(payload, msg_class, "", int64_as_string)


def _read_struct(payload: Any, msg_class: type, path: str, i64s: bool) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise TranscodeError(
            path, f"expected mapping for {msg_class.__name__}, got {type(payload).__name__}"
        )
    out: dict[str, Any] = {}
    for name, slot in _iter_fields(msg_class):
        if name == _EMPTY_STRUCT_MEMBER or name not in payload:
            continue
        out[name] = _read_value(payload[name], slot, _join(path, name), i64s)
    return out


def _read_value(value: Any, slot: Any, path: str, i64s: bool) -> Any:
    if isinstance(slot, BasicType):
        return _read_basic(value, slot.typename, path, i64s)
    if isinstance(slot, AbstractGenericString):
        if isinstance(value, str):
            return value
        raise TranscodeError(path, f"expected string, got {type(value).__name__}")
    if isinstance(slot, NamespacedType):
        return _read_struct(value, _resolve_namespaced(slot), path, i64s)
    if isinstance(slot, AbstractNestedType):
        return _read_sequence(value, slot, path, i64s)
    raise TranscodeError(path, f"unsupported slot type {type(slot).__name__}")


def _read_basic(value: Any, typename: str, path: str, i64s: bool) -> Any:
    try:
        if typename == "boolean":
            return bool(value)
        if typename in _OCTET_TYPENAMES:
            return _octet_to_int(value, path)
        if typename in _FLOAT_TYPENAMES:
            return float(value)  # numpy 스칼라 포함; NaN/Inf 보존
        if typename in _INT_RANGES:
            if isinstance(value, str):  # char/wchar는 1글자 str로 올 수 있음
                if len(value) != 1:
                    raise TranscodeError(
                        path, f"expected single char, got {len(value)}-char string"
                    )
                return ord(value)
            v = int(value)
            if i64s and typename in _INT64_TYPENAMES and abs(v) > _JSON_SAFE_INT_MAX:
                return str(v)
            return v
    except TranscodeError:
        raise
    except (TypeError, ValueError) as exc:
        raise TranscodeError(
            path, f"cannot read {typename} from {type(value).__name__}: {exc}"
        ) from exc
    return value


def _octet_to_int(value: Any, path: str) -> int:
    if isinstance(value, bytes | bytearray):
        if len(value) != 1:
            raise TranscodeError(path, f"scalar byte must be 1 byte, got {len(value)}")
        return value[0]
    if isinstance(value, str):  # message_to_ordereddict는 bytes를 chr() 연결 문자열로 렌더링
        if len(value) != 1:
            raise TranscodeError(path, f"scalar byte must be 1 char, got {len(value)}")
        return _checked_ord(value, path)
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    raise TranscodeError(path, f"cannot read scalar byte from {type(value).__name__}")


def _checked_ord(ch: str, path: str) -> int:
    code = ord(ch)
    if code > 255:
        raise TranscodeError(path, f"byte char out of range: {code}")
    return code


def _read_sequence(value: Any, slot: AbstractNestedType, path: str, i64s: bool) -> Any:
    if _is_byte_sequence(slot):
        return base64.b64encode(_container_to_bytes(value, path)).decode("ascii")
    if isinstance(value, str | bytes | bytearray | Mapping):
        raise TranscodeError(path, f"expected sequence, got {type(value).__name__}")
    try:
        items = list(value)
    except TypeError as exc:
        raise TranscodeError(path, f"expected sequence, got {type(value).__name__}") from exc
    return [
        _read_value(item, slot.value_type, f"{path}[{i}]", i64s) for i, item in enumerate(items)
    ]


def _container_to_bytes(value: Any, path: str) -> bytes:
    """uint8[]/byte[] 컨테이너(bytes/str/array.array/numpy/list)를 bytes로 정규화."""
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    if isinstance(value, str):  # bytes 값을 rosidl이 chr() 연결로 렌더링한 형태
        return bytes(_checked_ord(c, f"{path}[{i}]") for i, c in enumerate(value))
    if hasattr(value, "tobytes"):  # array.array('B')와 numpy uint8 배열
        return bytes(value.tobytes())
    if isinstance(value, list | tuple):
        out = bytearray()
        for i, item in enumerate(value):
            elem_path = f"{path}[{i}]"
            if isinstance(item, bytes | bytearray):
                if len(item) != 1:
                    raise TranscodeError(elem_path, f"byte element must be 1 byte, got {len(item)}")
                out += item
            elif isinstance(item, str):
                if len(item) != 1:
                    raise TranscodeError(elem_path, f"byte element must be 1 char, got {len(item)}")
                out.append(_checked_ord(item, elem_path))
            elif isinstance(item, int) and not isinstance(item, bool):
                if not 0 <= item <= 255:
                    raise TranscodeError(elem_path, f"byte value out of range: {item}")
                out.append(item)
            else:
                raise TranscodeError(elem_path, f"cannot read byte from {type(item).__name__}")
        return bytes(out)
    raise TranscodeError(path, f"cannot read byte array from {type(value).__name__}")


# ---------------------------------------------------------------------------
# 쓰기 경로: 정준 dict -> set_message_fields가 받는 dict
# ---------------------------------------------------------------------------


def from_canonical(
    canonical: Mapping[str, Any], msg_class: type, int64_as_string: bool = False
) -> dict[str, Any]:
    """정준 dict를 ``set_message_fields(msg_class(), ...)``용으로 검증·강제 변환.

    엄격 검증: 미지 필드, NaN/Inf, 타입 불일치, 범위 초과, 고정/제한 길이 위반은
    모두 문제 필드 경로를 담은 :class:`TranscodeError`. 누락 필드는 허용
    (rosidl 기본값 적용).
    """
    _require_rosidl()
    return _write_struct(canonical, msg_class, "", int64_as_string)


def _write_struct(value: Any, msg_class: type, path: str, i64s: bool) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TranscodeError(
            path, f"expected object for {msg_class.__name__}, got {type(value).__name__}"
        )
    slots = dict(_iter_fields(msg_class))
    out: dict[str, Any] = {}
    for key, item in value.items():
        if key == _EMPTY_STRUCT_MEMBER:
            continue
        if key not in slots:
            raise TranscodeError(
                _join(path, str(key)),
                f"unknown field for {msg_class.__name__} (known: {sorted(slots)})",
            )
        out[key] = _write_value(item, slots[key], _join(path, key), i64s)
    return out


def _write_value(value: Any, slot: Any, path: str, i64s: bool) -> Any:
    if isinstance(slot, BasicType):
        return _write_basic(value, slot.typename, path, i64s)
    if isinstance(slot, AbstractGenericString):
        return _write_string(value, slot, path)
    if isinstance(slot, NamespacedType):
        return _write_struct(value, _resolve_namespaced(slot), path, i64s)
    if isinstance(slot, AbstractNestedType):
        return _write_sequence(value, slot, path, i64s)
    raise TranscodeError(path, f"unsupported slot type {type(slot).__name__}")


def _write_basic(value: Any, typename: str, path: str, i64s: bool) -> Any:
    if typename == "boolean":
        if not isinstance(value, bool):
            raise TranscodeError(path, f"expected bool, got {type(value).__name__}")
        return value
    if typename in _FLOAT_TYPENAMES:
        return _write_float(value, typename, path)
    if typename in _OCTET_TYPENAMES:
        return bytes(
            [_write_int(value, typename, path, i64s)]
        )  # rclpy의 스칼라 byte는 1바이트 bytes
    if typename in _INT_RANGES:
        return _write_int(value, typename, path, i64s)
    return value


def _write_float(value: Any, typename: str, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TranscodeError(path, f"expected number, got {type(value).__name__}")
    v = float(value)
    if v != v or v in (float("inf"), float("-inf")):
        raise TranscodeError(path, "NaN/Inf not allowed in command input")
    if typename == "float" and abs(v) > _FLOAT32_MAX:
        raise TranscodeError(path, f"out of range for float32: {v!r}")
    return v


def _write_int(value: Any, typename: str, path: str, i64s: bool) -> int:
    if isinstance(value, bool):
        raise TranscodeError(path, "expected int, got bool")
    if isinstance(value, str):
        if not (i64s and typename in _INT64_TYPENAMES):
            raise TranscodeError(
                path,
                "expected int, got str (int64_as_string only applies to int64/uint64)",
            )
        try:
            v = int(value, 10)
        except ValueError as exc:
            raise TranscodeError(path, f"cannot parse int64 string: {value!r}") from exc
    elif isinstance(value, float):
        if not value.is_integer():
            raise TranscodeError(path, f"expected integer, got non-integral float {value!r}")
        v = int(value)
    elif isinstance(value, int):
        v = value
    else:
        raise TranscodeError(path, f"expected int, got {type(value).__name__}")
    lo, hi = _INT_RANGES[typename]
    if not lo <= v <= hi:
        raise TranscodeError(path, f"out of range for {typename} [{lo}, {hi}]: {v}")
    return v


def _write_string(value: Any, slot: AbstractGenericString, path: str) -> str:
    if not isinstance(value, str):
        raise TranscodeError(path, f"expected string, got {type(value).__name__}")
    if isinstance(slot, BoundedString | BoundedWString) and len(value) > slot.maximum_size:
        raise TranscodeError(path, f"string length {len(value)} exceeds bound {slot.maximum_size}")
    return value


def _check_length(n: int, slot: AbstractNestedType, path: str) -> None:
    if isinstance(slot, Array) and n != slot.size:
        raise TranscodeError(path, f"fixed array requires exactly {slot.size} elements, got {n}")
    if isinstance(slot, BoundedSequence) and n > slot.maximum_size:
        raise TranscodeError(path, f"sequence length {n} exceeds bound {slot.maximum_size}")


def _write_sequence(value: Any, slot: AbstractNestedType, path: str, i64s: bool) -> Any:
    if _is_byte_sequence(slot):
        if not isinstance(value, str):
            raise TranscodeError(
                path, f"expected base64 string for byte array, got {type(value).__name__}"
            )
        try:
            raw = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise TranscodeError(path, f"invalid base64: {exc}") from exc
        _check_length(len(raw), slot, path)
        if slot.value_type.typename in _OCTET_TYPENAMES:
            return [bytes([b]) for b in raw]  # rclpy의 byte[]는 1바이트 bytes의 시퀀스
        return raw  # rclpy의 uint8[]은 bytes를 그대로 받음
    if isinstance(value, str | bytes | bytearray | Mapping) or not isinstance(value, list | tuple):
        raise TranscodeError(path, f"expected array, got {type(value).__name__}")
    _check_length(len(value), slot, path)
    return [
        _write_value(item, slot.value_type, f"{path}[{i}]", i64s) for i, item in enumerate(value)
    ]


# ---------------------------------------------------------------------------
# 정준 입력 예시(config CNT input_example)
# ---------------------------------------------------------------------------


def make_input_example(msg_class: type) -> dict[str, Any]:
    """기본 생성 인스턴스로 만든 해당 타입의 정준 dict 예시."""
    from rosidl_runtime_py import message_to_ordereddict

    return to_canonical(message_to_ordereddict(msg_class()), msg_class)


def parse_message(msg: Any) -> dict[str, Any]:
    """임의 ROS2 메시지 -> 정준 JSON-safe dict. 타입별 코드 없음."""
    from rosidl_runtime_py import message_to_ordereddict

    return to_canonical(message_to_ordereddict(msg), type(msg))


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
        return float(value.get("sec", 0) + value.get("nanosec", 0) / 1e9)
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


def extract_source_ts(
    payload: dict[str, Any], field: str | None = None, fmt: str | None = None
) -> float | None:
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
            raise ValueError(
                f"unknown source_ts format {fmt!r}; registered: {sorted(FORMAT_REGISTRY)}"
            )
        return fn(value)

    stamp = _get_nested(payload, "header.stamp")
    if stamp is None:
        stamp = payload.get("stamp")
    return _coerce_ros_time(stamp)


def _coerce_ros_time(value: Any) -> float | None:
    ts = _ros_time_dict(value)
    if ts is not None:
        return ts
    if isinstance(value, int | float):
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
