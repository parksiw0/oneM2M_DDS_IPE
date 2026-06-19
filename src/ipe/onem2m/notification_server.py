"""HTTP 알림 리스너 (DESIGN §14.2/§14.4).

수신은 스레드 풀에서 돌지만 수락 호출(on_notify)은 전역 락 하나 안에서
실행돼 'seq 순서 == 큐 순서'가 성립한다. tinyIoT NOTIFY는 fire-and-forget
이라 파싱 불가/비-sgn 입력은 200/2000으로 닫고, 큐 넘침과 수락 장애만
500 + X-M2M-RSC 5207로 답해 CSE 쪽 로그에 남게 한다.
"""

from __future__ import annotations

import json
import threading
import logging
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from ipe.onem2m.notification import Notification, parse_notification

log = logging.getLogger(__name__)

# on_notify 반환 계약 (수락 상태 기계)
ACK_RESULTS = frozenset({"ok", "duplicate", "invalid", "denied"})


class NotificationServer:
    """oneM2M NOTIFY 엔드포인트 + /healthz 도달성 프로브.

    ``on_notify(path_key, notification)``은 ``'ok' | 'duplicate' |
    'invalid' | 'denied'``(→ 200/RSC 2000) 또는 ``'overflow'``
    (→ 500/RSC 5207)를 반환해야 한다. 전역 락 안에서 불리므로 수락과
    큐 적재만 해야 한다 — ROS2 연산이나 블로킹 I/O를 하면 CSE의 CREATE
    응답 경로가 멎는다.

    ``path_key``는 요청 경로에서 ``prefix``를 뗀 것. 예:
    POST /notify/robots/tb/cmd_vel/publishRequest →
    ``robots/tb/cmd_vel/publishRequest``.
    """

    def __init__(
        self,
        host: str,
        port: int,
        on_notify: Callable[[str, Notification], str],
        prefix: str = "/notify/",
        diag_fn: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._on_notify = on_notify
        self._prefix = prefix
        self._diag_fn = diag_fn
        self._httpd = ThreadingHTTPServer((host, port), self._make_handler())
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        """실제 바인딩된 포트(port=0 바인딩 해석)."""
        return self._httpd.server_address[1]

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        on_notify = self._on_notify
        prefix = self._prefix
        diag_fn = self._diag_fn

        class Handler(BaseHTTPRequestHandler):
            def _respond(self, status: int, rsc: str | None = None) -> None:
                self.send_response(status)
                if rsc is not None:
                    self.send_header("X-M2M-RSC", rsc)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0]
                if path == "/healthz":
                    self._respond(200)
                elif path == "/diag" and diag_fn is not None:
                    try:
                        payload = json.dumps(diag_fn(), default=str).encode()
                    except Exception:
                        log.exception("diag_fn failed")
                        self._respond(500)
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                else:
                    self._respond(404)

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0) or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw) if raw else {}
                except (ValueError, TypeError):
                    log.warning("unparseable NOTIFY body on %s dropped", self.path)
                    self._respond(200, "2000")
                    return
                notif = parse_notification(body)
                if notif is None:
                    log.warning("non-sgn POST on %s dropped", self.path)
                    self._respond(200, "2000")
                    return
                if notif.vrq:
                    # vrq는 ack 후 종료 — 라우팅·큐 적재 없음
                    self._respond(200, "2000")
                    return
                path = self.path.split("?", 1)[0]
                if path.startswith(prefix):
                    path_key = path[len(prefix):]
                else:
                    path_key = path.lstrip("/")
                try:
                    # 직렬화는 앱의 admission 락이 담당한다(catch-up 경로 포함 단일 권위)
                    result = on_notify(path_key, notif)
                except Exception:
                    log.exception("admission failed for %s", path_key)
                    self._respond(500, "5207")
                    return
                if result in ACK_RESULTS:
                    self._respond(200, "2000")
                elif result == "overflow":
                    self._respond(500, "5207")
                else:
                    log.error("unknown admission result %r for %s", result, path_key)
                    self._respond(500, "5207")

            def log_message(self, *args: Any) -> None:
                pass

        return Handler

    def start(self) -> None:
        self._thread.start()
        log.info("Notification server started on port %d", self.port)

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
