"""qos FCNT 프로비저닝 — 배치 규칙, 실패 폴백, allow_update SUB(net=1)."""
from __future__ import annotations

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.onem2m.resource_ops import SubStatus
from ipe.runtime.provisioning import Provisioner


class FakeOps:
    def __init__(self, fail_fcnt: bool = False):
        self.fail_fcnt = fail_fcnt
        self.fcnt_calls: list[tuple] = []
        self.sub_calls: list[dict] = []

    def ensure_cnt(self, parent, name, mni=None, lbl=None):
        return f"{parent.rstrip('/')}/{name}"

    def ensure_fcnt(self, parent, name, cnd, fcnt_type, initial_attrs=None):
        self.fcnt_calls.append((parent, name, cnd, fcnt_type, initial_attrs))
        if self.fail_fcnt:
            raise RuntimeError("RSC 4125 unknown cnd")
        return f"{parent.rstrip('/')}/{name}"

    def ensure_sub(self, parent, name, nu, net=None, nct=1):
        self.sub_calls.append({"parent": parent, "nu": nu, "net": net, "nct": nct})
        return SubStatus(path=f"{parent}/{name}", ok=True, created=True,
                         verified=True, ri="sub1")


def _prov(extra: dict, fail_fcnt: bool = False):
    cfg = {
        "cse": {"endpoint": "http://localhost:3000", "cse_base": "T", "ae_name": "ipe"},
        "qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}},
        "discovery": {"mode": "config-only"},
        "bridge": {"topics": [
            {"name": "/x", "type": "std_msgs/msg/String",
             "representation": "historical", "qos": "sensor_data"},
            {"name": "/cmd", "type": "std_msgs/msg/String", "direction": "command",
             "access": {"enabled": True}},
        ]},
        **extra,
    }
    rc = resolve(validate_config(cfg))
    ops = FakeOps(fail_fcnt=fail_fcnt)
    return Provisioner(rc, ops, state=None, poa_base="http://127.0.0.1:5050"), ops


def test_qos_fcnt_placement_and_attrs():
    prov, ops = _prov({})
    res = prov.provision_all()
    assert not res.errors and not res.qos_fcnt_failed
    by_dir = {c[4]["dir"]: c for c in ops.fcnt_calls}
    obs, cmd = by_dir["observe"], by_dir["command"]
    # 배치 단일 규칙(§4.4): 인터페이스 노드 아래 rn="qos"
    assert obs[1] == cmd[1] == "qos"
    assert "/ros2Data/" in obs[0] and "/ros2Command/" in cmd[0]
    assert obs[2] == "kr.ac.sejong.seslab.ros2.moduleclass.topicQos"
    assert obs[3] == "ros:tqos"
    attrs = obs[4]
    assert attrs["iface"] == "/x" and attrs["smode"] == "reject"
    assert all(k in attrs for k in ("cfRlb", "cfDrb", "cfHst", "cfDpt",
                                    "cfDdl", "cfLsp", "cfLiv", "cfLse"))
    assert "Iwked-Technology:ROS2" in attrs["lbl"]
    assert "smode" not in cmd[4]                     # smode는 observe 전용
    # path_map 뷰 키
    keys = {k[2] for k in res.path_map}
    assert {"qosObserve", "qosCommand"} <= keys


def test_failure_is_lbl_only_not_fatal():
    prov, ops = _prov({}, fail_fcnt=True)
    res = prov.provision_all()
    assert len(res.qos_fcnt_failed) == 2             # observe + command
    assert not res.errors                            # 치명 아님 — 계속 진행
    assert not any(k[2].startswith("qos") for k in res.path_map)


def test_allow_update_gates_sub():
    prov, ops = _prov({})
    prov.provision_all()
    assert not any("/qos" in c["parent"] for c in ops.sub_calls)
    prov, ops = _prov({"qos_fcnt": {"allow_update": True}})
    res = prov.provision_all()
    qos_subs = [c for c in ops.sub_calls if c["parent"].endswith("/qos")]
    assert len(qos_subs) == 2 and all(c["net"] == [1] for c in qos_subs)
    route_keys = [k for k in res.routes if k.startswith("qos_update/")]
    assert len(route_keys) == 2
    assert all(res.routes[k]["direction"] in ("observe", "command")
               for k in route_keys)


def test_disabled_skips_fcnt():
    prov, ops = _prov({"qos_fcnt": {"enabled": False}})
    prov.provision_all()
    assert ops.fcnt_calls == []
