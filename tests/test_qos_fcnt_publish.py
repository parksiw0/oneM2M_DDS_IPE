"""_publish_qos_state — 총함수 게시, 캐시/최소 간격, lbl 병행, lbl-only 폴백."""
from __future__ import annotations

import queue
import types

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.config.spec import QoSSpec
from ipe.runtime.app import IPEApp

_BASE = {
    "cse": {"endpoint": "http://localhost:3000", "cse_base": "T", "ae_name": "ipe"},
    "qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}},
}


class FakeAdapter:
    def __init__(self, states):
        self.states = states

    def qos_states(self):
        return [dict(s) for s in self.states]


def _app(tmp_path, qf=None, states=None):
    cfg = dict(_BASE)
    cfg["storage"] = {"state_db": str(tmp_path / "s.db")}
    cfg["qos_fcnt"] = {"publish_min_interval_ms": 0, **(qf or {})}
    app = IPEApp(resolve(validate_config(cfg)), types.SimpleNamespace())
    app.adapter = FakeAdapter(states if states is not None else [_state()])
    app.path_map[("r", "/x", "qosObserve")] = "/T/ipe/ros2Data/r/x/qos"
    app.path_map[("r", "/x", "history")] = "/T/ipe/ros2Data/r/x"
    app.specs_by_key[("observe", "r", "/x")] = types.SimpleNamespace(
        msg_type="std_msgs/msg/String")
    return app


def _state(**kw):
    st = {"robot_id": "r", "interface": "/x", "direction": "observe",
          "configured": QoSSpec(profile="sensor_data"), "applied": QoSSpec(),
          "peers": [], "events": ["noPublisherFallback"]}
    st.update(kw)
    return st


def _drain(app):
    ops = []
    while True:
        try:
            ops.append(app.outbound.get_nowait())
        except queue.Empty:
            return ops


def test_total_record_and_lbl_parallel(tmp_path):
    app = _app(tmp_path)
    try:
        app._publish_qos_state()
        ops = _drain(app)
        fcnt = [o for o in ops if o.kind == "update_fcnt"]
        lbl = [o for o in ops if o.kind == "update_lbl"]
        assert len(fcnt) == 1 and fcnt[0].path == "/T/ipe/ros2Data/r/x/qos"
        rec = fcnt[0].content["ros:tqos"]
        assert rec["pfRef"] == "sensor_data" and rec["evts"] == ["noPublisherFallback"]
        assert all(k in rec for k in ("cfRlb", "apRlb", "peers", "pcnt", "dvAxs"))
        assert len(lbl) == 1 and lbl[0].path == "/T/ipe/ros2Data/r/x"
        labels = lbl[0].content["labels"]
        assert any(x.startswith("qos:reliability=") for x in labels)
        assert "ipe:qosResource=/T/ipe/ros2Data/r/x/qos" in labels
    finally:
        app.state.close()


def test_cache_suppresses_identical_record(tmp_path):
    app = _app(tmp_path)
    try:
        app._publish_qos_state()
        _drain(app)
        app._publish_qos_state()
        assert [o for o in _drain(app) if o.kind == "update_fcnt"] == []
        app.adapter.states[0]["applied"] = QoSSpec(reliability="BEST_EFFORT")
        app._publish_qos_state()
        assert len([o for o in _drain(app) if o.kind == "update_fcnt"]) == 1
    finally:
        app.state.close()


def test_min_interval_gates_but_keeps_cache_stale(tmp_path):
    app = _app(tmp_path, qf={"publish_min_interval_ms": 60_000})
    try:
        app._publish_qos_state()                       # 첫 게시(간격 기록)
        assert len([o for o in _drain(app) if o.kind == "update_fcnt"]) == 1
        app.adapter.states[0]["applied"] = QoSSpec(reliability="BEST_EFFORT")
        app._publish_qos_state()                       # 변화 있어도 간격이 막음
        assert [o for o in _drain(app) if o.kind == "update_fcnt"] == []
    finally:
        app.state.close()


def test_lbl_only_fallback_key(tmp_path):
    app = _app(tmp_path)
    try:
        del app.path_map[("r", "/x", "qosObserve")]    # FCNT 생성 실패 상황
        app._publish_qos_state()
        ops = _drain(app)
        assert [o for o in ops if o.kind == "update_fcnt"] == []
        lbl = [o for o in ops if o.kind == "update_lbl"]
        assert len(lbl) == 1
        assert not any(x.startswith("ipe:qosResource=")
                       for x in lbl[0].content["labels"])
    finally:
        app.state.close()


def test_disabled_publishes_lbl_only(tmp_path):
    app = _app(tmp_path, qf={"enabled": False})
    try:
        app._publish_qos_state()
        ops = _drain(app)
        assert [o for o in ops if o.kind == "update_fcnt"] == []
        assert len([o for o in ops if o.kind == "update_lbl"]) == 1
    finally:
        app.state.close()
