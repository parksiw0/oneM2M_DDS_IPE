"""FCNT 레코드 합성 — 총함수 불변식, INF 인코딩, lbl 동치, peers/dvAxs."""
from __future__ import annotations

from types import SimpleNamespace

from ipe.config.spec import QoSSpec
from ipe.core.qos import (
    divergent_axes,
    endpoint_to_peer,
    spec_to_fcnt_attrs,
    spec_to_metadata,
)

CF_KEYS = ("cfRlb", "cfDrb", "cfHst", "cfDpt", "cfDdl", "cfLsp", "cfLiv", "cfLse")
AP_KEYS = tuple("ap" + k[2:] for k in CF_KEYS)


def _rec(**kw):
    base = dict(direction="observe", interface="/tb3/odom", robot_id="tb3",
                configured=QoSSpec(profile="px4_default"))
    base.update(kw)
    return spec_to_fcnt_attrs(**base)


def test_total_function_invariant():
    # CREATE 레코드: identity + cf* 8키 상존, ap*/관측 속성 부재
    rec = _rec()
    assert all(k in rec for k in CF_KEYS)
    assert not any(k in rec for k in AP_KEYS)
    assert (rec["dir"], rec["iface"], rec["robot"]) == ("observe", "/tb3/odom", "tb3")
    assert rec["pfRef"] == "px4_default" and rec["sver"] == "1"
    # 게시 레코드: 한번 쓰는 속성은 빈 값이라도 실린다(T4 유실 방지)
    rec = _rec(applied=QoSSpec(), events=[], peers=[])
    assert all(k in rec for k in AP_KEYS)
    assert rec["evts"] == [] and rec["peers"] == [] and rec["pcnt"] == 0
    assert rec["dvAxs"] == []


def test_inf_and_decimal_encoding():
    cfg = QoSSpec(deadline_ms=100, lifespan_ms=None,
                  liveliness_lease_duration_ms=1000, depth=5)
    rec = _rec(configured=cfg)
    assert rec["cfDdl"] == "100" and rec["cfLsp"] == "INF" and rec["cfLse"] == "1000"
    assert rec["cfDpt"] == 5      # depth만 정수(§4.2.3)


def test_values_match_lbl_scheme():
    # 부록 D: lbl 값과 FCNT ap* 값은 동일 소스·동일 규약
    ap = QoSSpec(reliability="BEST_EFFORT", deadline_ms=None, depth=7)
    rec = _rec(applied=ap)
    md = spec_to_metadata(ap)
    assert rec["apRlb"] == md["reliability"]
    assert rec["apDdl"] == md["deadline_ms"] == "INF"
    assert str(rec["apDpt"]) == md["depth"]


def test_smode_and_meta_optionals():
    rec = _rec(smode="demote", msg_type="nav_msgs/msg/Odometry")
    assert rec["smode"] == "demote" and rec["rtype"] == "nav_msgs/msg/Odometry"
    rec = _rec(configured=QoSSpec())          # 프리셋 유래가 아니면 pfRef 부재
    assert "pfRef" not in rec and "smode" not in rec


def _info(node="talker", ns="/", rlb="RELIABLE", ddl=None, lsp=0, lease=None):
    qos = SimpleNamespace(reliability=rlb, durability="VOLATILE",
                          liveliness="AUTOMATIC", deadline=ddl,
                          liveliness_lease_duration=lease, lifespan=lsp)
    return SimpleNamespace(qos_profile=qos, node_name=node, node_namespace=ns)


def test_endpoint_to_peer():
    p = endpoint_to_peer(_info(ddl=100_000_000), "pub")   # 100ms(ns)
    assert p["ep"] == "pub" and p["node"] == "/talker"
    assert p["rlb"] == "RELIABLE" and p["ddl"] == "100"
    assert p["lsp"] == "INF" and p["lse"] == "INF"        # 0/None 센티널 접기
    assert "hist" not in p and "dpth" not in p            # 인트로스펙션 허위값 배제
    # UNKNOWN은 관측 사실로 통과(§4.2.4)
    assert endpoint_to_peer(_info(rlb="UNKNOWN"), "sub")["rlb"] == "UNKNOWN"


def test_peers_cap_and_divergence():
    peers = [endpoint_to_peer(_info(rlb="RELIABLE"), "pub"),
             endpoint_to_peer(_info(rlb="BEST_EFFORT", ddl=100_000_000), "pub")]
    assert divergent_axes(peers) == ["reliability", "deadline"]
    rec = _rec(applied=QoSSpec(), peers=peers[:1], peer_count=9)
    assert rec["pcnt"] == 9 and len(rec["peers"]) == 1    # 캡 초과분은 pcnt로 인지
