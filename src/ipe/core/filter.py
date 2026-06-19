from __future__ import annotations

import statistics
from typing import Any

from ipe.core.common import as_numbers as _as_numbers


def _max_abs_delta(old: list[float], new: list[float]) -> float:
    if len(old) != len(new):
        return float("inf")
    return max((abs(a - b) for a, b in zip(old, new)), default=0.0)


class DeltaFilter:
    """key는 호출자가 합성한 robot 스코프 키(policy._state_key) —
    토픽명 단독이 아니다. 교차 로봇 격리는 이 키 합성에 의존한다."""

    def __init__(self) -> None:
        self._last: dict[str, dict[str, list[float]]] = {}
        self._last_send: dict[str, float] = {}

    def allow(
        self,
        key: str,
        fields: dict[str, Any],
        monitored: list[str] | None,
        min_change: float,
        max_interval: float | None = None,
        now: float | None = None,
    ) -> bool:
        names = monitored if monitored else list(fields.keys())
        current: dict[str, list[float]] = {}
        for name in names:
            if name not in fields:
                continue
            nums = _as_numbers(fields[name])
            if nums is not None:
                current[name] = nums

        if not current:
            return True

        last = self._last.get(key)
        if last is None:
            self._last[key] = current
            if now is not None:
                self._last_send[key] = now
            return True

        changed = False
        for name, nums in current.items():
            prev = last.get(name)
            if prev is None or _max_abs_delta(prev, nums) >= min_change:
                changed = True
                break

        forced = False
        if not changed and max_interval is not None and now is not None:
            sent = self._last_send.get(key)
            if sent is None or (now - sent) >= max_interval:
                forced = True

        if changed or forced:
            self._last[key] = {**last, **current}
            if now is not None:
                self._last_send[key] = now
            return True
        return False


def _agg_vectors(vectors: list[list[float]], fn: Any) -> Any:
    cols = zip(*vectors)
    res = [fn(list(c)) for c in cols]
    return res[0] if len(res) == 1 else res


_AGG_FN = {
    "mean": lambda c: sum(c) / len(c),
    "min": min,
    "max": max,
    "std": statistics.pstdev,
}


class WindowAggregator:
    """key 규약은 DeltaFilter와 동일(robot 스코프 합성 키)."""

    def __init__(self) -> None:
        self._buf: dict[str, dict[str, Any]] = {}

    def _new(self, ts: float) -> dict[str, Any]:
        return {"vecs": {}, "n": 0, "start": ts, "last": {}}

    def add(
        self,
        key: str,
        fields: dict[str, Any],
        mode: str,
        size: float,
        aggregations: list[str],
        monitored: list[str] | None,
        ts: float,
    ) -> dict[str, Any] | None:
        st = self._buf.get(key)
        if st is None:
            st = self._buf[key] = self._new(ts)

        names = monitored if monitored else list(fields.keys())
        st["last"] = {k: fields[k] for k in names if k in fields}
        for name in names:
            if name not in fields:
                continue
            nums = _as_numbers(fields[name])
            if nums is not None:
                st["vecs"].setdefault(name, []).append(nums)
        st["n"] += 1

        if mode == "count":
            closed = st["n"] >= size
        else:
            closed = (ts - st["start"]) >= size
        if not closed:
            return None

        out = self._flush(st, aggregations)
        self._buf[key] = self._new(ts)
        return out

    def _flush(self, st: dict[str, Any], aggregations: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {"window_n": st["n"]}
        for name, vectors in st["vecs"].items():
            for agg in aggregations:
                if agg == "last":
                    out[f"{name}_last"] = st["last"].get(name)
                elif agg in _AGG_FN:
                    out[f"{name}_{agg}"] = _agg_vectors(vectors, _AGG_FN[agg])
        for agg in aggregations:
            if agg == "last":
                for name, val in st["last"].items():
                    if name not in st["vecs"]:
                        out[f"{name}_last"] = val
        return out
