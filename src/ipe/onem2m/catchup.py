"""캐치업 스윕 (DESIGN §14.5).

tinyIoT 알림은 fire-and-forget이라 놓친 NOTIFY는 구독 경로로 복구되지 않는다.
입력 리프 CNT를 다시 읽어 마지막 완료 구간부터 일반 수락 경로로 재주입한다.
NOTIFY 수신 위치와 복구 위치를 분리하고, 경계 시각의 중복은 admission이 거른다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

# admission_fn(path_key, cin_ri, con, ct) -> 'ok'|'duplicate'|'overflow'|'invalid'|'denied'
AdmissionFn = Callable[[str, str, dict[str, Any] | None, str | None], str]


class CatchUpSweeper:
    def __init__(self, state: Any, ops: Any, admission_fn: AdmissionFn) -> None:
        self.state = state
        self.ops = ops
        self.admission_fn = admission_fn
        # path_key -> 입력 리프 CNT의 절대 CSE 경로
        self.input_cnts: dict[str, str] = {}

    def register(self, path_key: str, cnt_path: str) -> None:
        self._initialize_marker(path_key)
        self.input_cnts[path_key] = cnt_path

    def replace(self, input_cnts: dict[str, str]) -> None:
        """활성 binding generation의 입력 CNT 집합으로 원자적 참조를 교체한다."""
        for path_key in input_cnts:
            self._initialize_marker(path_key)
        self.input_cnts = dict(input_cnts)

    def _initialize_marker(self, path_key: str) -> None:
        """Migrate an existing cursor once, before accepting new notifications."""
        marker_key = f"catchup_cin_ct:{path_key}"
        missing = object()
        if self.state.get_kv(marker_key, missing) is missing:
            self.state.set_kv(marker_key, self.state.get_kv(f"last_cin_ct:{path_key}"))

    def mark_processed(self, path_key: str, ct: str | None) -> None:
        """Record the latest accepted NOTIFY timestamp without advancing recovery."""
        if ct:
            prev = self.state.get_kv(f"last_cin_ct:{path_key}")
            if prev is None or ct > prev:
                self.state.set_kv(f"last_cin_ct:{path_key}", ct)

    def sweep(self, reason: str) -> dict[str, int]:
        """{path_key: 재주입 수} 반환. 절대 던지지 않는다(CNT별 격리)."""
        injected: dict[str, int] = {}
        for path_key, cnt_path in self.input_cnts.items():
            try:
                injected[path_key] = self._sweep_one(path_key, cnt_path)
            except Exception as e:
                log.warning("catch-up sweep failed for %s (%s): %s", path_key, reason, e)
        total = sum(injected.values())
        if total:
            log.info("catch-up sweep (%s): re-injected %d CIN(s)", reason, total)
        return injected

    def _sweep_one(self, path_key: str, cnt_path: str) -> int:
        # NOTIFY arrival order is not a contiguous history watermark. Only a
        # completed scan prefix can advance recovery past older unseen CINs.
        marker_key = f"catchup_cin_ct:{path_key}"
        marker = self.state.get_kv(marker_key)
        completed = marker
        cins = self.ops.list_child_cins(cnt_path)
        # tinyIoT ct 형식(yyyymmddThhmmss)은 사전순 정렬 == 시간순
        cins.sort(key=lambda c: (c.get("ct") or "", c.get("ri") or ""))
        count = 0
        for cin in cins:
            ct = cin.get("ct")
            # ct has second precision: replay the boundary second and let
            # admission dedup distinguish different CINs with the same ct.
            if marker is not None and ct is not None and ct < marker:
                continue
            verdict = self.admission_fn(path_key, cin.get("ri", ""),
                                        cin.get("con"), ct)
            if verdict in ("overflow", "denied"):
                break
            self.mark_processed(path_key, ct)
            if ct and (completed is None or ct > completed):
                completed = ct
            if verdict == "ok":
                count += 1
        if completed is not None and completed != marker:
            self.state.set_kv(marker_key, completed)
        return count
