"""oneM2M MQTT 알림 리스너 (DESIGN §14.2/§14.4).

HTTP NotificationServer의 MQTT 짝. tinyIoT는 AE의 mqtt POA로 NOTIFY를
``/oneM2M/req/<cse_id>/<poa_path>`` 토픽(4세그, /json 없음)에 발행하며, 페이로드는
**순수 ``{"m2m:sgn": {...}}``** (op/pc 래핑 없음)다. 그래서 parse_notification을
그대로 재사용한다.

MQTT POA는 단일 엔드포인트라 nu URL 경로에 라우팅 키를 못 싣는다 — 그래서
``m2m:sgn.sur``(구조 경로)로 path_key를 찾는다(route_resolver). 수락(on_notify)은
앱의 admission 락 안에서 실행되므로 HTTP 경로와 동일하게 'seq 순서 == 큐 순서'가
성립한다. tinyIoT의 NOTIFY는 fire-and-forget(ack를 소비하지 않음)이라 ack 발행은
스펙 준수용 best-effort이고, 실제 손실 보정은 catch-up 스위퍼가 담당한다.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ipe.onem2m.notification import Notification, parse_notification
from ipe.onem2m.notification_server import ACK_RESULTS

if TYPE_CHECKING:
    from ipe.config.spec import MqttSpec

log = logging.getLogger(__name__)

OnNotify = Callable[[str, Notification], str]
RouteResolver = Callable[[str | None], "str | None"]


class MQTTNotificationListener:
    """oneM2M NOTIFY를 MQTT로 수신한다. start/stop/port 계약은 HTTP 리스너와 동일
    (port는 MQTT에 없으므로 None)."""

    def __init__(
        self,
        mqtt_cfg: MqttSpec,
        cse_id: str,
        poa_path: str,
        on_notify: OnNotify,
        route_resolver: RouteResolver,
        *,
        paho_client: Any = None,
    ) -> None:
        self.cfg = mqtt_cfg
        self.cse_id = cse_id
        self.poa_path = poa_path           # POA URI의 경로부 = tinyIoT가 쓰는 토픽 세그먼트
        self._on_notify = on_notify
        self._resolve = route_resolver
        self.prefix = getattr(mqtt_cfg, "topic_prefix", "") or ""
        self.qos = int(getattr(mqtt_cfg, "qos", 1))
        self._connect_timeout = float(getattr(mqtt_cfg, "connect_timeout_ms", 10000)) / 1000.0
        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._paho = paho_client if paho_client is not None else self._make_paho()
        self._paho.on_connect = self._on_connect
        self._paho.on_message = self._on_message
        self._paho.on_subscribe = self._on_subscribe
        self._paho.on_disconnect = self._on_disconnect

    @property
    def port(self) -> int | None:
        return None   # MQTT에는 바인딩 포트가 없다 (HTTP 리스너 계약 충족)

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    # -- paho 생성 -------------------------------------------------------
    def _make_paho(self) -> Any:
        import paho.mqtt.client as mqtt
        from paho.mqtt.enums import CallbackAPIVersion

        cid = f"{getattr(self.cfg, 'client_id', 'ros2-ipe')}-notify-{uuid.uuid4().hex[:6]}"
        # 알림은 연결이 끊긴 동안에도 큐잉되도록 영속 세션 선호(보강은 catch-up).
        c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=cid, clean_session=False)
        user = getattr(self.cfg, "username", None)
        if user:
            c.username_pw_set(user, getattr(self.cfg, "password", None))
        if getattr(self.cfg, "tls", False):
            c.tls_set(
                ca_certs=getattr(self.cfg, "tls_ca", None) or None,
                certfile=getattr(self.cfg, "tls_cert", None) or None,
                keyfile=getattr(self.cfg, "tls_key", None) or None,
            )
            if getattr(self.cfg, "tls_insecure", False):
                c.tls_insecure_set(True)
        c.reconnect_delay_set(min_delay=1, max_delay=30)
        return c

    # -- 토픽 ------------------------------------------------------------
    def _notify_topic(self) -> str:
        return f"{self.prefix}/oneM2M/req/{self.cse_id}/{self.poa_path}"

    def _resp_topic(self) -> str:
        return f"{self.prefix}/oneM2M/resp/{self.cse_id}/{self.poa_path}/json"

    # -- 생명주기 -------------------------------------------------------
    def start(self) -> None:
        """연결 + 구독. CONNACK와 SUBACK까지 블록한다 — SUB 생성(따라서 CSE의 vrq
        검증)보다 알림 구독이 먼저 활성화돼 있어야 vrq를 놓치지 않는다."""
        from ipe.onem2m.client import TransportError
        try:
            self._paho.connect(self.cfg.host, int(self.cfg.port),
                               keepalive=int(getattr(self.cfg, "keepalive", 60)))
            self._paho.loop_start()
        except Exception as e:
            raise TransportError(f"MQTT listener connect failed: {e}") from e
        if not self._connected.wait(self._connect_timeout):
            raise TransportError("MQTT listener: no CONNACK within timeout")
        if not self._subscribed.wait(self._connect_timeout):
            raise TransportError("MQTT listener: no SUBACK for notify topic")
        log.info("MQTT notification listener subscribed %s", self._notify_topic())

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            self._paho.disconnect()
        with contextlib.suppress(Exception):
            self._paho.loop_stop()

    # -- paho 콜백 -------------------------------------------------------
    def _on_connect(self, client: Any, userdata: Any, flags: Any,
                    reason_code: Any, properties: Any = None) -> None:
        if getattr(reason_code, "is_failure", False):
            log.error("MQTT listener connect refused: %s", reason_code)
            return
        self._subscribed.clear()
        # 정확 토픽 + '/#' 변형(서버가 꼬리 세그먼트를 붙이는 경우 대비) 둘 다 구독
        client.subscribe([(self._notify_topic(), self.qos),
                          (self._notify_topic() + "/#", self.qos)])
        self._connected.set()

    def _on_subscribe(self, client: Any, userdata: Any, mid: Any,
                      reason_code_list: Any = None, properties: Any = None) -> None:
        self._subscribed.set()

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any = None,
                       reason_code: Any = None, properties: Any = None) -> None:
        self._connected.clear()
        log.warning("MQTT listener disconnected (%s) — auto-reconnect pending", reason_code)

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        try:
            body = json.loads(message.payload.decode("utf-8", "replace"))
        except (ValueError, AttributeError):
            log.warning("unparseable NOTIFY on %s dropped", getattr(message, "topic", "?"))
            self._ack(2000)   # HTTP와 동일: 못 읽어도 ack로 닫는다
            return
        notif = parse_notification(body)
        if notif is None:
            log.warning("non-sgn MQTT NOTIFY dropped")
            self._ack(2000)
            return
        if notif.vrq:
            self._ack(2000)   # 검증 요청 — 라우팅/큐 적재 없이 ack
            return
        # sur -> path_key. 못 찾으면 sur를 그대로 넘겨 _admit이 unknownRoute로 표면화
        path_key = self._resolve(notif.sur) or (notif.sur or "")
        try:
            result = self._on_notify(path_key, notif)   # 앱 admission 락 안에서 실행
        except Exception:
            log.exception("admission failed for sur=%s", notif.sur)
            self._ack(5207)
            return
        self._ack(2000 if result in ACK_RESULTS else 5207)

    # -- ack (best-effort; tinyIoT는 소비하지 않음) ----------------------
    def _ack(self, rsc: int) -> None:
        ack = {"rsc": rsc, "rqi": "notify", "to": f"/{self.cse_id}", "rvi": "3"}
        with contextlib.suppress(Exception):
            self._paho.publish(self._resp_topic(), json.dumps(ack), qos=self.qos, retain=False)
