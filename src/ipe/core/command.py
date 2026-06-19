"""명령 디스패치 매니저 (DESIGN §10).

executor 스레드에서 돈다. 어드미션 dedup은 리스너에서 이미 끝났고, 여기서는
신선도·접근·레이트리밋·클램프 게이트만 맡는다. 조용한 드롭은 없다 — 모든
명령은 publish 아니면 commandStatus 이벤트로 끝난다.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ipe.core.common import TokenBucket, get_path, set_path
from ipe.config.spec import TopicSpec


@dataclass
class CommandOutcome:
    published: bool
    status: str           # published | rejected | expired | rateLimited | accessDenied | error
    detail: str = ""
    clamped: dict[str, Any] = field(default_factory=dict)








class CommandDispatchManager:
    """publish_fn(spec, payload) -> bool — 어댑터가 executor 스레드에서 소유하는 publish."""

    def __init__(self, publish_fn: Callable[[TopicSpec, dict[str, Any]], bool]) -> None:
        self.publish_fn = publish_fn
        self._buckets: dict[tuple[str, str], TokenBucket] = {}

    def dispatch(
        self,
        spec: TopicSpec,
        payload: dict[str, Any],
        cin_ct_epoch: float | None,
        ingest_monotonic: float,
    ) -> CommandOutcome:
        if not spec.access_enabled:
            return CommandOutcome(False, "accessDenied", "access.enabled is false")
        if spec.confirm == "required":
            return CommandOutcome(False, "rejected", "pending confirmation (confirm: required)")

        safety = spec.command
        max_age_ms = safety.max_age_ms if safety else 5000

        # 신선도 게이트 1: CIN 생성 나이(벽시계 — CSE 타임스탬프 도메인).
        # ct를 해석 못 하면 fail-safe로 expired — 신선도를 증명 못 하는 명령은 실행하지 않는다.
        if max_age_ms > 0:
            if cin_ct_epoch is None:
                return CommandOutcome(False, "expired", "cin ct unparseable (fail-safe)")
            age_ms = (time.time() - cin_ct_epoch) * 1000.0
            if age_ms > max_age_ms:
                return CommandOutcome(False, "expired", f"cin age {age_ms:.0f}ms > {max_age_ms}ms")
        # 신선도 게이트 2: 큐 체류 시간 — 게이트 1과 달리 단조 시계 기준.
        if max_age_ms > 0:
            queued_ms = (time.monotonic() - ingest_monotonic) * 1000.0
            if queued_ms > max_age_ms:
                return CommandOutcome(False, "expired", f"queued {queued_ms:.0f}ms > {max_age_ms}ms")

        if safety and safety.rate_limit_hz:
            key = (spec.robot_id, spec.interface)
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = self._buckets[key] = TokenBucket(safety.rate_limit_hz, burst=1.0)
            if not bucket.allow():
                return CommandOutcome(False, "rateLimited",
                                      f"rate limit {safety.rate_limit_hz}Hz exceeded")

        clamped: dict[str, Any] = {}
        if safety and safety.clamp:
            for path, (lo, hi) in safety.clamp.items():
                found, v = get_path(payload, path)
                if found and isinstance(v, (int, float)) and not isinstance(v, bool):
                    cv = min(max(float(v), lo), hi)
                    if cv != v:
                        set_path(payload, path, cv)
                        clamped[path] = cv

        try:
            ok = self.publish_fn(spec, payload)
        except Exception as e:  # 인코딩/타입 실패도 이벤트로 드러낸다 — 조용히 삼키지 않음
            return CommandOutcome(False, "rejected", f"encode/publish failed: {e}", clamped)
        if not ok:
            return CommandOutcome(False, "error", "publisher unavailable", clamped)
        return CommandOutcome(True, "published", "", clamped)
