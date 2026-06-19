"""계층 무의존 공유 유틸 — 중복 구현 통합의 정본.

deep_merge(resolver·app), 점 표기 get/set(normalize·command·transform),
TokenBucket(command 레이트리밋·app 예산), MinIntervalGate(샘플링·피드백·
이벤트 코얼레스), as_numbers(delta·anomaly 평탄화), project_fields(투영).
"""

from __future__ import annotations

import math
import time
from typing import Any


def deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    """필드 단위 재귀 병합 — over가 이긴다. 입력은 변경하지 않는다."""
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def get_path(d: dict[str, Any], path: str) -> tuple[bool, Any]:
    """점 표기 중첩 조회 — (존재 여부, 값). None 값과 부재를 구분한다."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def set_path(d: dict[str, Any], path: str, value: Any) -> None:
    """점 표기 중첩 설정 — 중간 노드가 dict가 아니면 dict로 치환한다."""
    parts = path.split(".")
    cur = d
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = cur[part] = {}
        cur = nxt
    cur[parts[-1]] = value


def project_top_level(d: dict[str, Any], fields: list[str], strict: bool = False) -> dict[str, Any]:
    """최상위 필드 투영(명령/goal 입력 계약용). strict면 투영 밖 필드에 ValueError.
    중첩 점 표기 관측 투영은 normalize.project_fields가 담당한다."""
    out = {f: d[f] for f in fields if f in d}
    if strict:
        extras = set(d) - set(fields)
        if extras:
            raise ValueError(f"fields outside projection: {sorted(extras)}")
    return out


def as_numbers(v: Any) -> list[float] | None:
    """스칼라/수치 배열 → float 리스트. bool·비수치·NaN/Inf 포함이면 None(판정 불가)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else [f]
    if isinstance(v, (list, tuple)):
        out: list[float] = []
        for x in v:
            if isinstance(x, bool) or not isinstance(x, (int, float)):
                return None
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            out.append(f)
        return out
    return None


class TokenBucket:
    """단조 시계 토큰버킷 — 명령 레이트리밋·전역 write 예산 공용."""

    def __init__(self, rate_hz: float, burst: float | None = None) -> None:
        self.rate = rate_hz
        self.capacity = max(burst if burst is not None else rate_hz, 1.0)
        self.tokens = self.capacity
        self.last = time.monotonic()

    def allow(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class MinIntervalGate:
    """키별 최소 간격 게이트 — 첫 호출은 통과. 시계는 호출자가 now로 공급
    (샘플링=ingest_ts, 코얼레스=monotonic — §0.2.9)."""

    def __init__(self) -> None:
        self._last: dict[Any, float] = {}

    def allow(self, key: Any, interval: float, now: float) -> bool:
        last = self._last.get(key)
        if last is None or now - last >= interval:
            self._last[key] = now
            return True
        return False
