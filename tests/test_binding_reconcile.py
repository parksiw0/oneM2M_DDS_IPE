"""S10 graph 추가·제거 대칭성과 generation 활성화 순서."""

from __future__ import annotations

import queue
from types import SimpleNamespace

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.config.runtime_config import discovery_runtime_config
from ipe.runtime.app_dispatch import DispatchMixin
from ipe.runtime.app_workers import WorkersMixin
from ipe.runtime.dispatcher import Route, RouteTable
from ipe.runtime.lifecycle import Lifecycle
from ipe.runtime.plan import PendingBindingPlan
from ipe.runtime.provisioning import Provisioner as ResourceProvisioner
from ipe.runtime.provisioning import ProvisionResult


def _args():
    return SimpleNamespace(cse_endpoint=None, cse_base=None, ae_name=None,
                           instance_id=None, robot_id=None, robot_namespace=None,
                           refresh_sec=None, domain_id=None)


def _snap(topic: str):
    return {"topics": [(topic, ["std_msgs/msg/String"])],
            "services": [], "actions": [],
            "topic_directions": {topic: "observe"}}


def test_reconcile_stages_addition_and_symmetric_removal():
    raw = discovery_runtime_config(_args(), {})
    raw["discovery"]["vanish_grace_polls"] = 1
    raw = validate_config(raw)
    active = resolve(raw, discovered=_snap("/old"))

    class Provisioner:
        def __init__(self, rc):
            self.rc = rc

        def provision_all(self):
            return ProvisionResult(ok=True)

    class Inbound:
        def __init__(self):
            self.events = []

        def put_control(self, event):
            self.events.append(event)
            return True

    class Harness(WorkersMixin):
        def __init__(self):
            self.rc = active
            self.provisioner = Provisioner(active)
            self._plan_misses = {}
            self._resource_removal_pending = {}
            self.lifecycle = SimpleNamespace(
                snapshot=SimpleNamespace(generation=7),
            )
            self.inbound = Inbound()
            self.guard = SimpleNamespace(trigger=lambda: None)

        def _churn_track(self, _snap):
            pass

        def _defer_unloadable_types(self, _rc):
            pass

    harness = Harness()
    harness._reconcile_discovery(_snap("/new"))

    pending = harness.inbound.events[0].spec
    assert isinstance(pending, PendingBindingPlan)
    assert [item[0][2] for item in pending.additions] == ["/new"]
    assert [item[0][2] for item in pending.removals] == ["/old"]
    assert pending.base_generation == 7
    assert harness.rc is active  # executor 활성화 전에는 active generation 유지


def test_activation_is_make_before_break():
    events = []

    class State:
        def set_kv(self, *_args):
            pass

    class Harness(DispatchMixin):
        def __init__(self):
            self.lifecycle = Lifecycle(State())
            self.rc = SimpleNamespace()
            self.provisioner = SimpleNamespace(rc=self.rc)
            self._prov_jobs = queue.Queue()

        def _bind_plan_spec(self, key, spec):
            events.append(("bind", key))
            return True

        def _absorb_provision(self, _result):
            events.append(("activate", None))

        def _unbind_plan_spec(self, key, spec):
            events.append(("unbind", key))

        def _terminate_inflight(self, *_args):
            pass

        def _publish_contract_for(self, *_args):
            pass

        def _publish_qos_state(self, *_args, **_kwargs):
            pass

        def emit_event(self, *_args):
            pass

    old = SimpleNamespace(robot_id="robot", interface="/old", direction="observe")
    new = SimpleNamespace(robot_id="robot", interface="/new", direction="observe")
    pending = PendingBindingPlan(
        rc=SimpleNamespace(), provision=ProvisionResult(ok=True),
        additions=[(("topic", "robot", "/new"), new)],
        removals=[(("topic", "robot", "/old"), old)],
    )
    Harness()._activate_plan(pending)

    assert [name for name, _key in events] == ["bind", "activate", "unbind"]


def test_route_generation_replacement_drops_stale_aliases():
    routes = RouteTable()
    routes.add("old", "command", "r", "/old")
    routes.add_alias("old/sub", "old")

    routes.replace({"new": Route("service", "r", "/new")}, {"new/sub": "new"})

    assert "old" not in routes and "new" in routes
    assert routes.resolve_sur("old/sub") is None
    assert routes.resolve_sur("/new/sub") == "new"


def test_superseded_plan_never_binds_or_removes_endpoints():
    events = []

    class Harness(DispatchMixin):
        def __init__(self):
            self.lifecycle = SimpleNamespace(
                snapshot=SimpleNamespace(generation=3),
            )

        def emit_event(self, category, severity, payload):
            events.append((category, severity, payload))

        def _bind_plan_spec(self, *_args):
            raise AssertionError("superseded plan must not bind")

        def _unbind_plan_spec(self, *_args):
            raise AssertionError("superseded plan must not unbind")

    pending = PendingBindingPlan(
        rc=SimpleNamespace(),
        provision=ProvisionResult(ok=True),
        additions=[],
        removals=[],
        base_generation=2,
    )

    Harness()._activate_plan(pending)

    assert events[0][2] == {
        "event": "bindingPlanSuperseded",
        "baseGeneration": 2,
        "activeGeneration": 3,
    }


def test_failed_resource_removal_is_retried_on_the_next_reconcile():
    key = ("topic", "tb3", "/scan")
    spec = SimpleNamespace(direction="observe")

    class Provisioner:
        def __init__(self):
            self.calls = 0

        def remove_interface(self, _kind, _spec):
            self.calls += 1
            return [] if self.calls == 1 else ["/T/ipe/robots/tb3/ros2Data/scan"]

    harness = SimpleNamespace(
        provisioner=Provisioner(),
        _resource_removal_pending={},
    )

    WorkersMixin._remove_interfaces(harness, [(key, spec)])
    assert harness._resource_removal_pending == {key: spec}

    WorkersMixin._remove_interfaces(
        harness,
        list(harness._resource_removal_pending.items()),
    )
    assert harness._resource_removal_pending == {}


def test_bidirectional_topic_removes_both_resource_branches():
    deleted = []

    class Ops:
        def delete_resource(self, path):
            deleted.append(path)
            return SimpleNamespace(ok=True, status=200, rsc=2002)

    rc = SimpleNamespace(
        cse=SimpleNamespace(cse_base="TinyIoT", ae_name="ros2-ipe"),
        naming={"sanitize": "_"},
    )
    provisioner = ResourceProvisioner(
        rc,
        Ops(),
        state=None,
        poa_base="http://127.0.0.1:5050",
    )
    spec = SimpleNamespace(robot_id="tb3", rel_path="shared", direction="both")

    removed = provisioner.remove_interface("topic", spec)

    assert removed == [
        "/TinyIoT/ros2-ipe/robots/tb3/ros2Data/shared",
        "/TinyIoT/ros2-ipe/robots/tb3/ros2Command/shared",
    ]
    assert deleted == removed
