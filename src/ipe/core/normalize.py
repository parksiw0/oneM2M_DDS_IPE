"""관측 정규화 (DESIGN §7.3).

v2 TopicIR -> payload 빌더 입력 dict. selected_fields의 점 표기 경로를 중첩 구조
그대로 투영하고, 비유한 float는 원소 위치를 보존한 채 None으로 치환한다(tinyIoT는
NaN/Inf 리터럴을 거부하지만 null은 받는다). bytes는 트랜스코더가 이미 처리했다.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from ipe.core.common import get_path, set_path as _set_path
from ipe.ir import TopicIR


def ct_to_epoch(ct: str | None, now: float | None = None) -> float | None:
    """CSE ct("yyyymmddThhmmss[,SSS]") → epoch. 해석 불가면 None —
    명령 신선도 게이트는 None을 expired로 본다(fail-safe).

    함정: oneM2M 표준 ts는 UTC지만 tinyIoT는 로컬타임으로 찍는다(실측 KST).
    UTC로만 읽으면 age가 -9h가 되어 만료 검출이 무력화된다. UTC/로컬 두 해석 중
    '현재에 가장 가까운 과거'를 채택해 양쪽 CSE에서 안전하게 동작시킨다."""
    if not ct:
        return None
    import time as _time
    from datetime import datetime, timezone
    base, _, millis = ct.partition(",")
    frac = int(millis) / 1000.0 if millis.isdigit() else 0.0
    try:
        naive = datetime.strptime(base, "%Y%m%dT%H%M%S")
    except ValueError:
        return None
    utc = naive.replace(tzinfo=timezone.utc).timestamp() + frac
    local = naive.timestamp() + frac          # 시스템 로컬 tz 해석
    ref = _time.time() if now is None else now
    # 미래(±2s 허용 초과) 해석은 배제하고, 과거 해석 중 현재에 가까운 쪽
    past = [e for e in (utc, local) if ref - e >= -2.0]
    if not past:
        return None                           # 양쪽 다 미래 — 신선도 증명 불가
    return max(past)


def epoch_to_onem2m_ts(epoch: float) -> str:
    """epoch 초 -> oneM2M 타임스탬프(UTC, ms 정밀도)."""
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    millis = dt.microsecond // 1000
    return f"{dt.strftime('%Y%m%dT%H%M%S')},{millis:03d}"


def sanitize_value(v: Any) -> Any:
    """비유한 float를 재귀적으로 None으로 치환하되 위치를 보존한다.

    리스트/튜플은 길이와 원소 순서를 유지하고(제거 없음 — LaserScan ranges처럼
    index=각도 의미가 살아야 한다), 튜플은 JSON 직렬화를 위해 리스트가 된다.
    """
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, (list, tuple)):
        return [sanitize_value(x) for x in v]
    if isinstance(v, dict):
        return {k: sanitize_value(val) for k, val in v.items()}
    return v




def project_fields(
    payload: dict[str, Any], fields: list[str] | None
) -> dict[str, Any]:
    """``selected_fields``(점 표기 중첩 경로)를 투영하고 sanitize한다.

    선택이 없으면 전체 payload를 sanitize. 없는 경로는 조용히 생략한다 —
    부분 메시지가 파이프라인을 죽이면 안 된다.
    """
    if not fields:
        return {k: sanitize_value(v) for k, v in payload.items()}
    out: dict[str, Any] = {}
    for f in fields:
        found, value = get_path(payload, f)
        if found:
            _set_path(out, f, sanitize_value(value))
    return out


def normalize_ir(ir: TopicIR, selected_fields: list[str] | None = None) -> dict[str, Any]:
    """v2 TopicIR -> payload 빌더가 소비하는 형태."""
    return {
        "topic": ir["interface_name"],
        "robot": ir["robot_id"],
        "seq": ir["seq"],
        "source_ts": ir.get("source_ts"),
        "ingest_ts": ir["ingest_ts"],
        "fields": project_fields(ir["payload"], selected_fields),
    }
