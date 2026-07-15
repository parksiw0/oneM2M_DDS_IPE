"""qos_fcnt 설정 블록 — 기본값, 오버라이드, pfRef 배관, /qos 경로 선점 검출."""
from __future__ import annotations

import pytest

from ipe.config.loader import ConfigError, validate_config
from ipe.config.resolver import ResolveError, resolve

_BASE = {
    "cse": {"endpoint": "http://localhost:3000", "cse_base": "T", "ae_name": "ipe"},
    "qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}},
}


def _rc(extra: dict, discovered=None):
    return resolve(validate_config({**_BASE, **extra}), discovered=discovered)


def test_defaults():
    rc = _rc({})
    qf = rc.qos_fcnt
    assert qf.enabled is True
    assert qf.type == "ros:tqos"
    assert qf.cnd == "kr.ac.sejong.seslab.ros2.moduleclass.topicQos"
    assert qf.lbl_compat is True
    assert qf.allow_update is False          # 외부 쓰기 경로는 옵트인(결정 #6)
    assert qf.publish_min_interval_ms == 5000
    assert qf.peers_max == 8


def test_overrides():
    rc = _rc({"qos_fcnt": {"enabled": False, "type": "x:q", "cnd": "org.example.q",
                           "lbl_compat": False, "allow_update": True,
                           "publish_min_interval_ms": 0, "peers_max": 2}})
    qf = rc.qos_fcnt
    assert (qf.enabled, qf.type, qf.cnd) == (False, "x:q", "org.example.q")
    assert (qf.lbl_compat, qf.allow_update) == (False, True)
    assert (qf.publish_min_interval_ms, qf.peers_max) == (0, 2)


def test_pfref_from_profile_and_inline():
    rc = _rc({"discovery": {"mode": "config-only"},
              "bridge": {"topics": [
                  {"name": "/a", "type": "std_msgs/msg/String", "qos": "sensor_data"},
                  {"name": "/b", "type": "std_msgs/msg/String",
                   "qos": {"profile": "sensor_data", "depth": 3}},
                  {"name": "/c", "type": "std_msgs/msg/String",
                   "qos": {"reliability": "RELIABLE"}},
              ]}})
    by = {t.interface: t for t in rc.topics}
    assert by["/a"].qos.profile == "sensor_data"
    assert by["/b"].qos.profile == "sensor_data" and by["/b"].qos.depth == 3
    assert by["/c"].qos.profile is None


def test_qos_view_collision_detected():
    with pytest.raises(ResolveError, match="collision"):
        _rc({"discovery": {"mode": "config-only"},
             "bridge": {"topics": [
                 {"name": "/a", "type": "std_msgs/msg/String", "path": "a"},
                 {"name": "/b", "type": "std_msgs/msg/String", "path": "a/qos"},
             ]}})


def test_section_85_still_enforced():
    # qos_fcnt 블록 추가가 기존 command 금지 QoS 검증을 건드리지 않는다
    with pytest.raises((ConfigError, ResolveError)):
        _rc({"qos_fcnt": {"allow_update": True},
             "discovery": {"mode": "config-only"},
             "bridge": {"topics": [
                 {"name": "/cmd", "type": "std_msgs/msg/String",
                  "direction": "command", "access": {"enabled": True},
                  "qos": {"deadline_ms": 100}},
             ]}})
