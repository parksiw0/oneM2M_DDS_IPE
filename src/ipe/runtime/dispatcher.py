"""인바운드 NOTIFY 라우팅 (DESIGN v3 §14.3).

라우팅 권위는 IPE가 SUB 생성 시 부여한 nu 경로 키
(``<branch>/<robot>/<iface>/<leaf>``, 서버 접두사 제거)이고 ``sur``는
보조 검증용일 뿐이다. 미등록 키는 None을 반환해, 호출자가 nack 대신
알림을 보류했다가 라우트 갱신 후 재조회하게 한다.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from ipe.onem2m.notification import Notification

# kind -> CIN con 안의 상관 키. cancel은 goalId로 상관시키되 dedup은
# (robot_id, goalId, event_id) 기준이다 — 그래서 모든 InboundEvent에
# event_id가 실린다.
CORRELATION_FIELDS: dict[str, str] = {
    "command": "commandId",
    "service": "requestId",
    "action_goal": "goalId",
    "cancel": "goalId",
    "decision": "proposalId",
}


@dataclass
class InboundEvent:
    kind: str
    robot_id: str
    interface: str
    correlation_id: str | None
    event_id: str | None             # CIN ri — processedEventId
    payload: dict[str, Any] | None   # CIN con
    ct: str | None                   # CIN ct — catch-up 마커 입력
    ingest_monotonic: float | None = None   # 수락 시각(단조) — dispatch 신선도 게이트 입력
    dedup_corr: str | None = None            # admission이 실제 사용한 멱등 키(드레인과 일치 필수)
    spec: Any = None                          # _bind_* 내부 이벤트의 운반체


@dataclass
class Route:
    kind: str
    robot_id: str
    interface: str
    meta: dict[str, Any] = field(default_factory=dict)


class RouteTable:
    """path_key -> Route 레지스트리.

    executor 스레드가 변경하고 리스너 스레드가 읽으므로 내부 락을 둔다.
    """

    def __init__(self) -> None:
        self._routes: dict[str, Route] = {}
        # sur(구독 참조) -> path_key. MQTT NOTIFY는 nu URL 경로가 없어 path_key를
        # 못 싣는다 — m2m:sgn.sur(구조 경로)로 라우팅한다(HTTP는 미사용).
        self._by_sur: dict[str, str] = {}
        self._lock = threading.Lock()

    def add(self, path_key: str, kind: str, robot_id: str, interface: str,
            meta: dict[str, Any] | None = None) -> None:
        if kind not in CORRELATION_FIELDS:
            raise ValueError(f"unknown route kind: {kind!r}")
        with self._lock:
            self._routes[path_key] = Route(kind, robot_id, interface, dict(meta or {}))

    @staticmethod
    def _norm_sur(sur: str) -> str:
        # tinyIoT sur는 선행 슬래시 없는 구조 경로지만 방어적으로 정규화한다
        return sur.lstrip("/")

    def add_alias(self, sur: str | None, path_key: str) -> None:
        """sur -> path_key 별칭 등록(MQTT 라우팅). sur가 비면 무시."""
        if not sur:
            return
        with self._lock:
            self._by_sur[self._norm_sur(sur)] = path_key

    def resolve_sur(self, sur: str | None) -> str | None:
        """sur로 path_key를 찾는다. 없으면 None."""
        if not sur:
            return None
        with self._lock:
            return self._by_sur.get(self._norm_sur(sur))

    def remove(self, path_key: str) -> None:
        with self._lock:
            self._routes.pop(path_key, None)

    def get(self, path_key: str) -> Route | None:
        with self._lock:
            return self._routes.get(path_key)

    def __contains__(self, path_key: str) -> bool:
        with self._lock:
            return path_key in self._routes

    def __len__(self) -> int:
        with self._lock:
            return len(self._routes)

    def route(self, path_key: str, notif: Notification) -> InboundEvent | None:
        with self._lock:
            r = self._routes.get(path_key)
        if r is None:
            return None
        corr = None
        if notif.con is not None:
            corr = notif.con.get(CORRELATION_FIELDS[r.kind])
        return InboundEvent(
            kind=r.kind,
            robot_id=r.robot_id,
            interface=r.interface,
            correlation_id=corr,
            event_id=notif.cin_ri,
            payload=notif.con,
            ct=notif.cin_ct,
        )
