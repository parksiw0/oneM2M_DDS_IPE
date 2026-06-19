"""캐치업 스윕 (DESIGN §14.5).

tinyIoT 알림은 fire-and-forget이라 놓친 NOTIFY는 구독 경로로 복구되지 않는다.
입력 리프 CNT를 다시 읽어 저장된 마커보다 새로운 CIN을 일반 수락 경로로
재주입하며, 중복 제거와 max_age 게이트가 재주입을 안전하게 만든다.
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
        self.input_cnts[path_key] = cnt_path

    def mark_processed(self, path_key: str, ct: str | None) -> None:
        """마커 전진 — 리스너도 수락된 NOTIFY마다 호출하므로 정상 운영 중에는
        스윕 윈도가 작게 유지된다."""
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
        marker = self.state.get_kv(f"last_cin_ct:{path_key}")
        cins = self.ops.list_child_cins(cnt_path)
        # tinyIoT ct 형식(yyyymmddThhmmss)은 사전순 정렬 == 시간순
        cins.sort(key=lambda c: (c.get("ct") or "", c.get("ri") or ""))
        count = 0
        for cin in cins:
            ct = cin.get("ct")
            if marker is not None and ct is not None and ct <= marker:
                continue
            verdict = self.admission_fn(path_key, cin.get("ri", ""),
                                        cin.get("con"), ct)
            # 방문한 모든 CIN에서 마커를 전진 — 중복/만료 포함 —
            # 다음 스윕이 같은 구간을 재생하지 않게 한다
            self.mark_processed(path_key, ct)
            if verdict == "ok":
                count += 1
            elif verdict == "overflow":
                break   # 큐 포화 — 다음 스윕이 마커부터 재개
        return count
