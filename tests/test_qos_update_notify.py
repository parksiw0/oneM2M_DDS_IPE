"""qos_update 인바운드 — 특화 rep 파싱/멱등 키, 에코 가드, 검증·예측·원복."""
from __future__ import annotations

import queue
import types

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.config.spec import QoSSpec, TopicSpec
from ipe.onem2m.notification import parse_notification
from ipe.runtime.app import IPEApp
from ipe.runtime.dispatcher import InboundEvent, RouteTable

_BASE = {
    "cse": {"endpoint": "http://localhost:3000", "cse_base": "T", "ae_name": "ipe"},
    "qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}},
}


def _sgn(rep_key="ros:tqos", st=5, **attrs):
    return {"m2m:sgn": {"sur": "T/ipe/ros2Data/r/x/qos/ipeSub",
                        "nev": {"net": 1, "rep": {rep_key: {"st": st, **attrs}}}}}


def test_parse_specialisation_rep():
    n = parse_notification(_sgn(cfRlb="RELIABLE"))
    assert n.fcnt_key == "ros:tqos" and n.st == 5
    assert n.fcnt["cfRlb"] == "RELIABLE" and n.con is None
    # 표준 m2m:* rep는 특화로 취급하지 않는다
    assert parse_notification(_sgn(rep_key="m2m:cnt")).fcnt is None


def test_route_qos_update_idempotency_key():
    rt = RouteTable()
    rt.add("qos_update/r/observe/x", "qos_update", "r", "/x",
           meta={"direction": "observe"})
    ev = rt.route("qos_update/r/observe/x", parse_notification(_sgn()))
    assert ev.kind == "qos_update" and ev.event_id.endswith(":st=5")
    assert ev.meta["direction"] == "observe" and ev.payload["st"] == 5
    # 특화 rep가 아니면 소비하지 않는다(None → 보류/무시 경로)
    cin_notif = parse_notification({"m2m:sgn": {"nev": {"net": 3, "rep": {
        "m2m:cin": {"ri": "c1", "con": "{}"}}}}})
    assert rt.route("qos_update/r/observe/x", cin_notif) is None


class FakeAdapter:
    def __init__(self):
        self.verdict: tuple[bool, list[str]] = (True, [])
        self.rebound: list = []
        self.states = [{"robot_id": "r", "interface": "/x", "direction": "observe",
                        "configured": QoSSpec(), "applied": QoSSpec(),
                        "peers": [], "events": []}]

    def qos_states(self):
        return [dict(s) for s in self.states]

    def check_candidate(self, key, candidate, direction):
        return self.verdict

    def rebind_interface(self, key, new_qos):
        self.rebound.append((key, new_qos))
        self.states[0]["configured"] = new_qos
        self.states[0]["applied"] = new_qos
        return True


def _app(tmp_path):
    cfg = dict(_BASE)
    cfg["storage"] = {"state_db": str(tmp_path / "s.db")}
    cfg["qos_fcnt"] = {"allow_update": True, "publish_min_interval_ms": 0}
    app = IPEApp(resolve(validate_config(cfg)), types.SimpleNamespace())
    app.adapter = FakeAdapter()
    app.path_map[("r", "/x", "qosObserve")] = "/T/ipe/ros2Data/r/x/qos"
    app.status_paths["qosStatus"] = "/T/ipe/status/qosStatus"
    spec = TopicSpec(robot_id="r", interface="/x", msg_type="std_msgs/msg/String",
                     direction="observe", representation="historical",
                     qos=QoSSpec())
    app.specs_by_key[("observe", "r", "/x")] = spec
    return app


def _ev(st=5, direction="observe", **attrs):
    return InboundEvent(kind="qos_update", robot_id="r", interface="/x",
                        correlation_id=None, event_id=f"sur:st={st}",
                        payload={"st": st, **attrs}, ct=None,
                        meta={"direction": direction})


def _drain(app):
    ops = []
    while True:
        try:
            ops.append(app.outbound.get_nowait())
        except queue.Empty:
            return ops


def _events(ops):
    import json
    return [json.loads(o.content["con"]) if isinstance(o.content.get("con"), str)
            else o.content for o in ops if o.kind == "create_cin"]


def test_echo_guard_ignores_own_publish(tmp_path):
    app = _app(tmp_path)
    try:
        app._dispatch_qos_update(_ev(cfRlb="RELIABLE", cfDpt=10), "sur:st=5")
        assert app.adapter.rebound == []
        assert [o for o in _drain(app) if o.kind == "update_fcnt"] == []
    finally:
        app.state.close()


def test_domain_violation_rejected_and_restored(tmp_path):
    app = _app(tmp_path)
    try:
        app._dispatch_qos_update(_ev(cfRlb="BOGUS"), "sur:st=5")
        ops = _drain(app)
        assert app.adapter.rebound == []
        assert any(e.get("event") == "qosUpdateRejected" for e in _events(ops))
        # 원복: 직전 정본 레코드 재게시
        assert len([o for o in ops if o.kind == "update_fcnt"]) == 1
    finally:
        app.state.close()


def test_command_rules_reused(tmp_path):
    app = _app(tmp_path)
    try:
        spec = TopicSpec(robot_id="r", interface="/x",
                         msg_type="std_msgs/msg/String", direction="command",
                         representation="historical", qos=QoSSpec())
        app.specs_by_key[("command", "r", "/x")] = spec
        app.adapter.states[0]["direction"] = "command"
        app.path_map[("r", "/x", "qosCommand")] = "/T/ipe/ros2Command/r/x/qos"
        app._dispatch_qos_update(_ev(direction="command", cfDdl="100"), "sur:st=5")
        assert app.adapter.rebound == []
        assert any(e.get("event") == "qosUpdateRejected"
                   and "8.5" in e.get("reason", "")
                   for e in _events(_drain(app)))
    finally:
        app.state.close()


def test_predicted_error_rejected(tmp_path):
    app = _app(tmp_path)
    try:
        app.adapter.verdict = (False, ["reliability ERROR"])
        app._dispatch_qos_update(_ev(cfRlb="BEST_EFFORT"), "sur:st=5")
        assert app.adapter.rebound == []
        assert any(e.get("event") == "qosUpdateRejected" and
                   "predicted" in e.get("reason", "")
                   for e in _events(_drain(app)))
    finally:
        app.state.close()


def test_accept_rebinds_and_converges(tmp_path):
    app = _app(tmp_path)
    try:
        app._dispatch_qos_update(_ev(cfRlb="BEST_EFFORT", cfDdl="INF"), "sur:st=5")
        assert len(app.adapter.rebound) == 1
        key, cand = app.adapter.rebound[0]
        assert key == ("r", "/x") and cand.reliability == "BEST_EFFORT"
        ops = _drain(app)
        evs = _events(ops)
        assert any(e.get("event") == "qosConfigUpdated" for e in evs)
        fcnt = [o for o in ops if o.kind == "update_fcnt"]
        assert len(fcnt) == 1
        assert fcnt[0].content["ros:tqos"]["cfRlb"] == "BEST_EFFORT"
    finally:
        app.state.close()
