"""OneM2MMQTTClient 단위 테스트 — 가짜 paho로 브로커 없이 검증.

확인 대상: 요청 primitive 모양(op/ty/pc/to 슬래시 제거/fr/rqi/rvi), 토픽 형식,
rsc->status 매핑, 오버사이즈 사전 거부, 응답 타임아웃/단절 시 TransportError,
origin 변경 시 resp 재구독, 그리고 수정하지 않은 ResourceOps가 MQTT 클라이언트로도
동일하게 동작하는지(동기 계약 보존).
"""
from __future__ import annotations

import json
import threading
import time
import types

import pytest

from ipe.onem2m.client import OversizeError, TransportError
from ipe.onem2m.mqtt_client import OneM2MMQTTClient


class _RC:
    is_failure = False

    def __init__(self, v: int = 0) -> None:
        self.value = v

    def __eq__(self, o: object) -> bool:
        return self.value == o


class _Info:
    def __init__(self, rc: int = 0) -> None:
        self.rc = rc


class _Msg:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


def _default_responder(prim: dict) -> dict:
    rsc = {1: 2001, 2: 2000, 3: 2004, 4: 2002}[prim["op"]]
    return {"rsc": rsc, "rqi": prim["rqi"], "to": prim.get("to"),
            "fr": prim.get("fr"), "rvi": "3", "pc": prim.get("pc")}


class FakePaho:
    """동기 가짜 paho: publish 시 즉시 on_message로 응답을 되돌린다."""

    def __init__(self, responder=None, auto_respond: bool = True) -> None:
        self.on_connect = self.on_message = self.on_disconnect = None
        self.published: list[tuple[str, dict]] = []
        self.subscriptions: list[tuple[str, int]] = []
        self.responder = responder or _default_responder
        self.auto_respond = auto_respond

    def connect(self, host, port, keepalive=60):
        if self.on_connect:
            self.on_connect(self, None, {}, _RC(0), None)

    def loop_start(self): pass
    def loop_stop(self): pass

    def disconnect(self):
        if self.on_disconnect:
            self.on_disconnect(self, None, None, _RC(0), None)

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))

    def username_pw_set(self, *a, **k): pass
    def tls_set(self, *a, **k): pass
    def tls_insecure_set(self, *a, **k): pass
    def reconnect_delay_set(self, *a, **k): pass

    def publish(self, topic, payload, qos=0, retain=False):
        prim = json.loads(payload)
        self.published.append((topic, prim))
        if self.auto_respond and self.on_message:
            resp = self.responder(prim)
            if resp is not None:
                self.on_message(self, None,
                                _Msg("/oneM2M/resp/x/x/json", json.dumps(resp).encode()))
        return _Info(0)


def _cfg(**over):
    d = dict(host="127.0.0.1", port=1883, client_id="t", keepalive=60, qos=1,
             clean_session=True, topic_prefix="", response_timeout_ms=500,
             connect_timeout_ms=2000, tls=False, username=None, password=None,
             max_payload=65536)
    d.update(over)
    return types.SimpleNamespace(**d)


def _client(fake, *, origin="Cipe", **cfg_over):
    c = OneM2MMQTTClient(_cfg(**cfg_over), "tinyiot", "TinyIoT",
                         origin=origin, paho_client=fake)
    c.start()
    return c


# --- 요청 primitive 모양 ----------------------------------------------------

def test_create_primitive_and_status_mapping():
    fake = FakePaho()
    c = _client(fake)
    r = c.create("/TinyIoT/ae", 2, {"m2m:ae": {"rn": "ae"}})
    topic, prim = fake.published[-1]
    assert topic == "/oneM2M/req/Cipe/tinyiot/json"
    assert prim["op"] == 1 and prim["ty"] == 2
    assert prim["to"] == "TinyIoT/ae"           # 선행 슬래시 제거
    assert prim["fr"] == "Cipe" and prim["rvi"] == "3" and prim["rqi"]
    assert prim["pc"] == {"m2m:ae": {"rn": "ae"}}
    assert r.status == 201 and r.rsc == 2001 and r.ok


def test_retrieve_params_map_to_fc_without_fu():
    fake = FakePaho()
    c = _client(fake)
    c.get("/TinyIoT/ae/cnt", params={"rcn": 4, "lim": 10})
    _, prim = fake.published[-1]
    assert prim["op"] == 2 and prim["rcn"] == 4
    assert prim["fc"] == {"lim": 10}
    assert "fu" not in prim and "fu" not in prim.get("fc", {})  # 일반 RETRIEVE


def test_update_has_no_ty_delete_has_no_pc():
    fake = FakePaho()
    c = _client(fake)
    c.update("/x", {"m2m:cnt": {"lbl": ["a"]}})
    _, p = fake.published[-1]
    assert p["op"] == 3 and "ty" not in p and p["pc"]
    c.delete("/x/y")
    _, p = fake.published[-1]
    assert p["op"] == 4 and "pc" not in p and p["to"] == "x/y"


def test_response_status_from_rsc_duplicate():
    # 4105(rn 중복) -> status 409 로 합성되어 ResourceOps의 _is_duplicate가 본다
    fake = FakePaho(responder=lambda prim: {"rsc": 4105, "rqi": prim["rqi"], "pc": None})
    c = _client(fake)
    r = c.create("/x", 4, {"m2m:cin": {"con": "v"}})
    assert r.status == 409 and r.rsc == 4105 and not r.ok


# --- 오버사이즈 / 타임아웃 / 단절 ------------------------------------------

def test_oversize_rejected_before_publish():
    fake = FakePaho()
    c = _client(fake, max_payload=50)
    with pytest.raises(OversizeError):
        c.create("/x", 4, {"m2m:cin": {"con": "x" * 200}})
    assert fake.published == []   # 발행 자체를 안 한다


def test_timeout_raises_recoverable():
    fake = FakePaho(auto_respond=False)
    c = _client(fake, response_timeout_ms=120)
    with pytest.raises(TransportError) as ei:
        c.get("/x")
    assert ei.value.recoverable is True


def test_disconnect_wakes_blocked_waiter():
    fake = FakePaho(auto_respond=False)
    c = _client(fake, response_timeout_ms=5000)
    out: dict = {}

    def call():
        try:
            c.get("/x")
        except BaseException as e:  # noqa: BLE001
            out["err"] = e

    t = threading.Thread(target=call)
    t.start()
    time.sleep(0.15)              # 대기자 등록 + 블록될 시간
    c._on_disconnect(fake, None)  # 브로커 단절 시뮬
    t.join(2)
    assert isinstance(out.get("err"), TransportError) and out["err"].recoverable


# --- origin 가변 + 재구독 ---------------------------------------------------

def test_origin_change_resubscribes_and_retargets():
    fake = FakePaho()
    c = OneM2MMQTTClient(_cfg(), "tinyiot", "TinyIoT", origin="CAdmin", paho_client=fake)
    c.start()
    c.origin = "Cipe"   # AE 등록 후 aei 반영
    assert any(t == "/oneM2M/resp/Cipe/tinyiot/#" for t, _ in fake.subscriptions)
    c.create("/x", 2, {"m2m:ae": {}})
    topic, _ = fake.published[-1]
    assert topic == "/oneM2M/req/Cipe/tinyiot/json"


# --- 동기 계약 보존: 수정 안 한 ResourceOps가 MQTT로도 동작 -----------------

def test_resourceops_ensure_ae_over_mqtt():
    from ipe.onem2m.resource_ops import ResourceOps

    def responder(prim):
        # AE create -> 응답 pc에 m2m:ae.aei (실측과 동일 모양)
        return {"rsc": 2001, "rqi": prim["rqi"],
                "pc": {"m2m:ae": {"rn": "ros2-ipe", "aei": "Cipe"}}}

    c = _client(FakePaho(responder=responder))
    ops = ResourceOps(c)
    path, aei = ops.ensure_ae("/TinyIoT", "ros2-ipe")
    assert path == "/TinyIoT/ros2-ipe" and aei == "Cipe"


def test_resourceops_create_cin_idempotent_duplicate():
    from ipe.onem2m.resource_ops import ResourceOps

    # 결정적 rn 재전송이 4105로 오면 duplicate 성공으로 풀려야 한다
    c = _client(FakePaho(responder=lambda p: {"rsc": 4105, "rqi": p["rqi"], "pc": None}))
    ops = ResourceOps(c)
    res = ops.create_cin("/TinyIoT/ae/cnt", {"v": 1}, rn="det-rn")
    assert res.duplicate and res.ok and not res.created
