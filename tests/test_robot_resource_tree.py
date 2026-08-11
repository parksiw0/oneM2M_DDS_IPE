"""Robot CNT가 oneM2M 리소스 트리의 격리 경계인지 검증한다."""

from __future__ import annotations

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.onem2m.resource_ops import SubStatus
from ipe.runtime.provisioning import Provisioner


class FakeOps:
    def __init__(self) -> None:
        self.cnts: list[str] = []
        self.subs: list[str] = []

    def ensure_cnt(self, parent, name, mni=None, lbl=None):
        path = f"{parent.rstrip('/')}/{name}"
        self.cnts.append(path)
        return path

    def ensure_fcnt(self, parent, name, cnd, fcnt_type, initial_attrs=None):
        return f"{parent.rstrip('/')}/{name}"

    def ensure_sub(self, parent, name, nu, net=None, nct=1):
        self.subs.append(parent)
        return SubStatus(path=f"{parent}/{name}", ok=True, created=True,
                         verified=True, ri=f"sub-{len(self.subs)}")


def _resolved():
    raw = {
        "cse": {"endpoint": "http://localhost:3000", "cse_base": "TinyIoT",
                "ae_name": "ros2-ipe"},
        "robots": [{"id": "tb3", "namespace": "/tb3"}],
        "qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}},
        "discovery": {"mode": "config-only"},
        "bridge": {
            "topics": [
                {"name": "/tb3/scan", "type": "sensor_msgs/msg/LaserScan"},
                {"name": "/tb3/cmd_vel", "type": "geometry_msgs/msg/Twist",
                 "direction": "command", "access": {"enabled": True}},
            ],
            "services": [
                {"name": "/tb3/reset", "type": "std_srvs/srv/Trigger"},
            ],
            "actions": [
                {"name": "/tb3/navigate", "type": "nav2_msgs/action/NavigateToPose"},
            ],
        },
    }
    return resolve(validate_config(raw))


def test_robot_boundary_wraps_all_ros_interfaces():
    rc = _resolved()
    ops = FakeOps()
    result = Provisioner(rc, ops, state=None,
                         poa_base="http://127.0.0.1:5050").provision_all()

    assert result.ok and not result.errors
    root = "/TinyIoT/ros2-ipe/robots/tb3"
    assert {f"{root}/{name}" for name in
            ("ros2Data", "ros2Command", "services", "actions")} <= set(ops.cnts)
    assert "/TinyIoT/ros2-ipe/status" in ops.cnts
    assert "/TinyIoT/ros2-ipe/config" in ops.cnts

    # robot namespace는 robot CNT와 인터페이스 경로에 중복되지 않는다.
    assert f"{root}/ros2Data/scan" in ops.cnts
    assert f"{root}/ros2Data/tb3/scan" not in ops.cnts


def test_command_cin_and_sub_share_the_topic_cnt():
    rc = _resolved()
    ops = FakeOps()
    Provisioner(rc, ops, state=None,
                poa_base="http://127.0.0.1:5050").provision_all()

    command = "/TinyIoT/ros2-ipe/robots/tb3/ros2Command/cmd_vel"
    assert command in ops.subs
    assert f"{command}/publishRequest" not in ops.cnts
    assert f"{command}/publishStatus" not in ops.cnts


def test_service_tree_contains_only_request_and_response_leaves():
    rc = _resolved()
    ops = FakeOps()
    Provisioner(rc, ops, state=None,
                poa_base="http://127.0.0.1:5050").provision_all()

    service = "/TinyIoT/ros2-ipe/robots/tb3/services/reset"
    assert f"{service}/request" in ops.cnts
    assert f"{service}/response" in ops.cnts
    assert f"{service}/invocationStatus" not in ops.cnts
