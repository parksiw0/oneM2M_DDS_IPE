"""oneM2M MQTT 바인딩 클라이언트 (tinyIoT 전제, DESIGN §15).

paho 백그라운드 스레드 위에 **동기 요청/응답 facade**를 얹는다: 각 메서드는
oneM2M 요청 primitive를 req 토픽에 발행하고, ``rqi``로 매칭되는 응답을 resp
토픽에서 기다려 OneM2MResponse로 매핑한다. 겉 계약은 OneM2MHTTPClient와 동일한
단일-시도 — 재시도/백오프/스풀은 classify()/backoff_delays()를 쓰는 워커 몫이다.

tinyIoT MQTT 실측 규칙(2026-06, ENABLE_MQTT 빌드 확인):
- 요청 토픽  /oneM2M/req/<idify(origin)>/<cse_id>/json  (페이로드=전체 primitive)
- 응답 토픽  /oneM2M/resp/<idify(origin)>/<cse_id>/json (rqi로 상관, rsc는 본문 필드)
- to = CSE-상대 경로(선행 '/' 없음, CSE_BASE_NAME으로 시작) — HTTP URL 경로에서 '/'만 뗀 것
- rvi 필수; op 1/2/3/4 = Create/Retrieve/Update/Delete; 직렬화는 소문자 'json'만
- cse_id(토픽 receiver=CSE_BASE_RI)는 to 경로의 cse_base(CSE_BASE_NAME)와 다르다
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
import uuid
from typing import TYPE_CHECKING, Any

from ipe.onem2m.client import (
    DEFAULT_MAX_PAYLOAD,
    OneM2MResponse,
    OversizeError,
    TransportError,
    http_status_from_rsc,
    idify,
)

if TYPE_CHECKING:
    from ipe.config.spec import MqttSpec

log = logging.getLogger(__name__)

OP_CREATE, OP_RETRIEVE, OP_UPDATE, OP_DELETE = 1, 2, 3, 4


class _Waiter:
    """rqi 하나에 대한 응답 대기 슬롯."""

    __slots__ = ("ev", "resp", "err")

    def __init__(self) -> None:
        self.ev = threading.Event()
        self.resp: dict[str, Any] | None = None
        self.err: BaseException | None = None


class OneM2MMQTTClient:
    """단일 시도 MQTT 바인딩. OneM2MClient Protocol 구현.

    ``origin``은 프로퍼티 — AE 등록 후 aei로 재할당하면 resp 구독 토픽을 자동
    재구독한다(요청 토픽의 originator 세그먼트가 origin에서 유도되므로).
    paho 클라이언트는 테스트 주입을 위해 ``paho_client``로 받을 수 있다.
    """

    def __init__(
        self,
        mqtt_cfg: MqttSpec,
        cse_id: str,
        cse_base: str,
        *,
        origin: str,
        rvi: str = "3",
        max_payload: int = DEFAULT_MAX_PAYLOAD,
        paho_client: Any = None,
    ) -> None:
        self.cfg = mqtt_cfg
        self.cse_id = cse_id            # 토픽 receiver 세그먼트 (CSE_BASE_RI)
        self.cse_base = cse_base        # to 경로 루트 (CSE_BASE_NAME) — 참고용
        self._origin = origin
        self.rvi = rvi
        self.max_payload = getattr(mqtt_cfg, "max_payload", None) or max_payload
        self.endpoint = f"mqtt://{mqtt_cfg.host}:{mqtt_cfg.port}"
        self.prefix = getattr(mqtt_cfg, "topic_prefix", "") or ""
        self.qos = int(getattr(mqtt_cfg, "qos", 1))
        self.timeout = float(getattr(mqtt_cfg, "response_timeout_ms", 5000)) / 1000.0
        self._connect_timeout = float(getattr(mqtt_cfg, "connect_timeout_ms", 10000)) / 1000.0

        self._pending: dict[str, _Waiter] = {}
        self._lock = threading.Lock()
        self._connected = threading.Event()
        self._subscribed: set[str] = set()   # 이미 구독한 resp 토픽들

        self._paho = paho_client if paho_client is not None else self._make_paho()
        self._paho.on_connect = self._on_connect
        self._paho.on_message = self._on_message
        self._paho.on_disconnect = self._on_disconnect

    # -- paho 생성/연결 --------------------------------------------------
    def _make_paho(self) -> Any:
        import paho.mqtt.client as mqtt
        from paho.mqtt.enums import CallbackAPIVersion

        cid = f"{getattr(self.cfg, 'client_id', 'ros2-ipe')}-{uuid.uuid4().hex[:6]}"
        c = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id=cid,
            clean_session=bool(getattr(self.cfg, "clean_session", True)),
        )
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

    def start(self) -> None:
        """브로커 연결 + 백그라운드 루프 시작. CONNACK까지 블록한다.

        연결 실패/타임아웃은 TransportError로 올린다(부트스트랩에서 시끄럽게
        실패하도록).
        """
        try:
            self._paho.connect(self.cfg.host, int(self.cfg.port),
                               keepalive=int(getattr(self.cfg, "keepalive", 60)))
            self._paho.loop_start()
        except Exception as e:  # 소켓/해석 오류 등
            raise TransportError(f"MQTT connect failed: {e}", recoverable=True) from e
        if not self._connected.wait(self._connect_timeout):
            raise TransportError(
                f"MQTT broker did not acknowledge connection within "
                f"{self._connect_timeout:.0f}s ({self.endpoint})",
                recoverable=True,
            )
        log.info("MQTT connected: %s (cse_id=%s)", self.endpoint, self.cse_id)

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            self._paho.disconnect()
        with contextlib.suppress(Exception):
            self._paho.loop_stop()
        self._fail_pending(TransportError("client stopped", recoverable=True))

    # -- origin (가변; resp 구독 토픽이 따라간다) ------------------------
    @property
    def origin(self) -> str:
        return self._origin

    @origin.setter
    def origin(self, value: str) -> None:
        self._origin = value
        if self._connected.is_set():
            self._subscribe_resp()   # 새 origin의 resp 토픽 구독

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    # -- 토픽 --------------------------------------------------------------
    def _req_topic(self) -> str:
        return f"{self.prefix}/oneM2M/req/{idify(self._origin)}/{self.cse_id}/json"

    def _resp_topic(self) -> str:
        # rqi로 상관하므로 와일드카드 tail로 직렬화 세그먼트 변형까지 흡수
        return f"{self.prefix}/oneM2M/resp/{idify(self._origin)}/{self.cse_id}/#"

    def _subscribe_resp(self) -> None:
        topic = self._resp_topic()
        if topic in self._subscribed:
            return
        self._paho.subscribe(topic, qos=self.qos)
        self._subscribed.add(topic)
        log.debug("MQTT subscribed resp topic %s", topic)

    # -- paho 콜백 (백그라운드 스레드) -----------------------------------
    def _on_connect(self, client: Any, userdata: Any, flags: Any,
                    reason_code: Any, properties: Any = None) -> None:
        if getattr(reason_code, "is_failure", False):
            log.error("MQTT connect refused: %s", reason_code)
            return
        self._subscribed.clear()      # 재연결 시 재구독 보장
        self._subscribe_resp()
        self._connected.set()

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any = None,
                       reason_code: Any = None, properties: Any = None) -> None:
        self._connected.clear()
        # 인플라이트 대기자를 복구가능 오류로 깨운다(타임아웃까지 멈춰 있지 않게)
        self._fail_pending(TransportError("MQTT disconnected", recoverable=True))
        log.warning("MQTT disconnected (%s) — auto-reconnect pending", reason_code)

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        try:
            prim = json.loads(message.payload.decode("utf-8", "replace"))
        except (ValueError, AttributeError):
            return
        rqi = prim.get("rqi") if isinstance(prim, dict) else None
        if rqi is None:
            return
        with self._lock:
            w = self._pending.get(rqi)
        if w is None:
            return   # 늦게 온/모르는 응답 — no-op
        w.resp = prim
        w.ev.set()

    def _fail_pending(self, err: BaseException) -> None:
        with self._lock:
            waiters = list(self._pending.values())
        for w in waiters:
            if w.resp is None:
                w.err = err
                w.ev.set()

    # -- 요청/응답 --------------------------------------------------------
    def _request(
        self, op: int, path: str, *,
        ty: int | None = None, pc: dict[str, Any] | None = None,
        rcn: int | None = None, fc: dict[str, Any] | None = None,
    ) -> OneM2MResponse:
        # 오버사이즈 가드: HTTP와 동일하게 본문(pc)을 기준으로 (tinyIoT MAX_PAYLOAD_SIZE)
        if pc is not None:
            size = len(json.dumps(pc, ensure_ascii=False).encode("utf-8"))
            if size > self.max_payload:
                raise OversizeError(size, self.max_payload)

        rqi = uuid.uuid4().hex[:16]
        prim: dict[str, Any] = {
            "op": op, "to": path.lstrip("/"), "fr": self._origin,
            "rqi": rqi, "rvi": self.rvi,
        }
        if ty is not None:
            prim["ty"] = ty
        if pc is not None:
            prim["pc"] = pc
        if rcn is not None:
            prim["rcn"] = rcn
        if fc is not None:
            prim["fc"] = fc

        payload = json.dumps(prim, ensure_ascii=False)
        w = _Waiter()
        with self._lock:
            self._pending[rqi] = w          # 발행 전 등록(이른 응답 경쟁 방지)
        try:
            info = self._paho.publish(self._req_topic(), payload, qos=self.qos, retain=False)
            rc = getattr(info, "rc", 0)
            if rc != 0:
                raise TransportError(f"MQTT publish failed rc={rc}", recoverable=True)
            if not w.ev.wait(self.timeout):
                raise TransportError(f"MQTT response timeout (rqi={rqi}, op={op})",
                                     recoverable=True)
            if w.err is not None:
                raise w.err
            return self._to_response(w.resp or {})
        finally:
            with self._lock:
                self._pending.pop(rqi, None)

    @staticmethod
    def _to_response(prim: dict[str, Any]) -> OneM2MResponse:
        rsc = prim.get("rsc")
        rsc_int = int(rsc) if rsc is not None else None
        status = http_status_from_rsc(rsc_int) if rsc_int is not None else 200
        headers = {"X-M2M-RSC": str(rsc_int)} if rsc_int is not None else {}
        body = prim.get("pc")
        return OneM2MResponse(status=status, body=body, headers=headers, rsc=rsc_int)

    # -- OneM2MClient 계약 -----------------------------------------------
    def get(self, path: str, params: dict[str, Any] | None = None) -> OneM2MResponse:
        rcn = fc = None
        if params:
            rcn = params.get("rcn")
            lim = params.get("lim")
            if lim is not None:
                # 일반 RETRIEVE: lim은 fc에, fu는 넣지 않는다(fu=1이면 tinyIoT가
                # DISCOVERY로 바꿔 m2m:cnt/m2m:cin 파싱이 깨진다)
                fc = {"lim": lim}
        return self._request(OP_RETRIEVE, path, rcn=rcn, fc=fc)

    def create(self, path: str, ty: int, body: dict[str, Any]) -> OneM2MResponse:
        return self._request(OP_CREATE, path, ty=ty, pc=body)

    def update(self, path: str, body: dict[str, Any]) -> OneM2MResponse:
        return self._request(OP_UPDATE, path, pc=body)

    def delete(self, path: str) -> OneM2MResponse:
        return self._request(OP_DELETE, path)
