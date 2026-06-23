"""MQTTNotificationListener + RouteTable sur 별칭 단위 테스트 (브로커 없이)."""
from __future__ import annotations

import json
import types

from ipe.onem2m.mqtt_listener import MQTTNotificationListener
from ipe.runtime.dispatcher import RouteTable


class _RC:
    is_failure = False


class _Info:
    rc = 0


class _Msg:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class FakePaho:
    def __init__(self) -> None:
        self.on_connect = self.on_message = self.on_subscribe = self.on_disconnect = None
        self.subscriptions: list = []
        self.published: list[tuple[str, dict]] = []

    def connect(self, host, port, keepalive=60):
        if self.on_connect:
            self.on_connect(self, None, {}, _RC(), None)

    def loop_start(self): pass
    def loop_stop(self): pass
    def disconnect(self): pass

    def subscribe(self, topics, qos=0):
        self.subscriptions.append(topics)
        if self.on_subscribe:
            self.on_subscribe(self, None, 1, [_RC()], None)

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload)))
        return _Info()

    def username_pw_set(self, *a, **k): pass
    def tls_set(self, *a, **k): pass
    def reconnect_delay_set(self, *a, **k): pass


def _cfg(**over):
    d = dict(host="127.0.0.1", port=1883, client_id="t", keepalive=60, qos=1,
             topic_prefix="", connect_timeout_ms=2000, tls=False,
             username=None, password=None)
    d.update(over)
    return types.SimpleNamespace(**d)


def _listener(fake, on_notify, resolver, poa_path="Cipe"):
    lis = MQTTNotificationListener(_cfg(), "tinyiot", poa_path, on_notify, resolver,
                                   paho_client=fake)
    lis.start()
    return lis


def _sgn_data(sur, ri="r1", ct="20260623T000000", con='{"commandId":"c1"}'):
    return {"m2m:sgn": {"nev": {"net": 3, "rep": {"m2m:cin": {"con": con, "ri": ri, "ct": ct}}},
                        "sur": sur}}


def _feed(lis, fake, obj):
    lis._on_message(fake, None, _Msg("/oneM2M/req/tinyiot/Cipe", json.dumps(obj).encode()))


# --- RouteTable sur 별칭 ----------------------------------------------------

def test_routetable_sur_alias_normalizes_leading_slash():
    rt = RouteTable()
    rt.add("command/r1/iface", "command", "r1", "iface")
    rt.add_alias("TinyIoT/ae/cnt/ipeSub", "command/r1/iface")
    assert rt.resolve_sur("TinyIoT/ae/cnt/ipeSub") == "command/r1/iface"
    assert rt.resolve_sur("/TinyIoT/ae/cnt/ipeSub") == "command/r1/iface"  # 선행 슬래시 무관
    assert rt.resolve_sur("nope") is None
    assert rt.resolve_sur(None) is None


# --- 리스너: 구독 / vrq / 라우팅 / ack -------------------------------------

def test_start_subscribes_notify_topic():
    fake = FakePaho()
    _listener(fake, lambda *_: "ok", lambda s: None)
    flat = [t for sub in fake.subscriptions for (t, _q) in sub]
    assert "/oneM2M/req/tinyiot/Cipe" in flat


def test_vrq_acked_without_routing():
    fake = FakePaho()
    calls = []
    lis = _listener(fake, lambda pk, n: calls.append(pk) or "ok", lambda s: None)
    _feed(lis, fake, {"m2m:sgn": {"vrq": True, "sur": "x", "cr": "y"}})
    assert calls == []                       # 라우팅 안 함
    assert fake.published[-1][1]["rsc"] == 2000


def test_data_notify_routes_by_sur_and_acks_2000():
    fake = FakePaho()
    seen = {}

    def on_notify(path_key, notif):
        seen["pk"] = path_key
        seen["ri"] = notif.cin_ri
        return "ok"

    rt = RouteTable()
    rt.add("command/r1/iface", "command", "r1", "iface")
    rt.add_alias("TinyIoT/ae/cnt/ipeSub", "command/r1/iface")
    lis = _listener(fake, on_notify, rt.resolve_sur)
    _feed(lis, fake, _sgn_data("TinyIoT/ae/cnt/ipeSub"))
    assert seen["pk"] == "command/r1/iface" and seen["ri"] == "r1"
    assert fake.published[-1][1]["rsc"] == 2000


def test_unresolved_sur_passes_through_for_unknownroute():
    fake = FakePaho()
    seen = {}

    # resolver가 None이면 sur를 그대로 path_key로 넘겨 _admit이 unknownRoute 처리
    def on_notify(pk, n):
        seen["pk"] = pk
        return "invalid"

    lis = _listener(fake, on_notify, lambda s: None)
    _feed(lis, fake, _sgn_data("TinyIoT/zzz/ipeSub"))
    assert seen["pk"] == "TinyIoT/zzz/ipeSub"
    assert fake.published[-1][1]["rsc"] == 2000   # invalid도 ACK_RESULTS -> 2000


def test_overflow_result_acks_5207():
    fake = FakePaho()
    lis = _listener(fake, lambda pk, n: "overflow", lambda s: "command/r1/iface")
    _feed(lis, fake, _sgn_data("TinyIoT/ae/cnt/ipeSub"))
    assert fake.published[-1][1]["rsc"] == 5207


def test_non_sgn_dropped_with_ack():
    fake = FakePaho()
    calls = []
    lis = _listener(fake, lambda pk, n: calls.append(pk) or "ok", lambda s: None)
    _feed(lis, fake, {"not": "a sgn"})
    assert calls == []
    assert fake.published[-1][1]["rsc"] == 2000
