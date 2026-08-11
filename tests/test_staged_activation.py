from __future__ import annotations

from types import SimpleNamespace

from ipe.runtime.app import IPEApp
from ipe.runtime.dispatcher import RouteTable
from ipe.runtime.provisioning import ProvisionResult


class _CatchUp:
    def __init__(self):
        self.inputs = None

    def replace(self, inputs):
        self.inputs = dict(inputs)


def test_routes_remain_staged_until_provision_result_is_absorbed():
    harness = SimpleNamespace(
        path_map={},
        status_paths={},
        routes=RouteTable(),
        catchup=_CatchUp(),
        protocol="http",
        _qos_lbl_only=set(),
    )
    result = ProvisionResult(
        ok=True,
        path_map={("tb3", "/cmd_vel", "command"): "/T/ipe/cmd"},
        routes={
            "command/tb3/cmd_vel": {
                "kind": "command",
                "robot_id": "tb3",
                "interface": "/cmd_vel",
                "input_cnt_path": "/T/ipe/cmd",
                "sub_ri": "sub-1",
            },
        },
    )

    assert "command/tb3/cmd_vel" not in harness.routes

    IPEApp._absorb_provision(harness, result)

    assert "command/tb3/cmd_vel" in harness.routes
    assert harness.catchup.inputs == {"command/tb3/cmd_vel": "/T/ipe/cmd"}
    assert harness.path_map == {("tb3", "/cmd_vel", "command"): "/T/ipe/cmd"}


def test_initial_endpoint_failure_rolls_back_the_generation():
    topic = SimpleNamespace(
        robot_id="tb3",
        interface="/both",
        direction="both",
        access_enabled=True,
        msg_type="std_msgs/msg/String",
    )

    class Adapter:
        def __init__(self):
            self.shutdown_called = False

        def bind_observe(self, _spec):
            return True

        def bind_command(self, _spec):
            return False

        def shutdown(self):
            self.shutdown_called = True

    adapter = Adapter()
    harness = SimpleNamespace(
        rc=SimpleNamespace(topics=[topic], services=[], actions=[]),
        adapter=adapter,
        specs_by_key={},
    )

    assert IPEApp._bind_all(harness) is False
    assert adapter.shutdown_called is True
    assert harness.specs_by_key == {}
