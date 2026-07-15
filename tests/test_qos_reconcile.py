"""QoS 조정 엔진 — 최약/최강, 센티널 접기, 가드, refresh 무플랩 회귀(P1)."""
from __future__ import annotations

from types import SimpleNamespace

from ipe.adapter.ros2 import GenericROS2Adapter, _ObserveState
from ipe.config.spec import QoSSpec, TopicSpec
from ipe.core import qos as qosmod

INT64_MAX = 2**63 - 1


def _prof(rlb="RELIABLE", drb="VOLATILE", liv="AUTOMATIC",
          ddl=None, lease=None, lsp=None):
    return SimpleNamespace(reliability=rlb, durability=drb, liveliness=liv,
                           deadline=ddl, liveliness_lease_duration=lease,
                           lifespan=lsp)


def test_sentinel_folding():
    assert qosmod.duration_ms(None) is None
    assert qosmod.duration_ms(0) is None                  # rmw default
    assert qosmod.duration_ms(INT64_MAX) is None          # RMW_DURATION_INFINITE
    assert qosmod.duration_ms(100_000_000) == 100


def test_reconcile_observe_weakest():
    cfg = QoSSpec(reliability="RELIABLE", durability="TRANSIENT_LOCAL")
    spec, events = qosmod.reconcile_observe(
        [_prof(rlb="BEST_EFFORT"), _prof(rlb="RELIABLE")], cfg, has_explicit=True)
    assert spec.reliability == "BEST_EFFORT"
    assert spec.durability == "VOLATILE" and "latchedDowngraded" in events
    spec, events = qosmod.reconcile_observe([], cfg, has_explicit=True)
    assert spec == cfg and events == ["noPublisherFallback"]


def test_reconcile_observe_duration_inf_dominates():
    spec, _ = qosmod.reconcile_observe(
        [_prof(ddl=100_000_000), _prof(ddl=None)], QoSSpec(), has_explicit=True)
    assert spec.deadline_ms is None
    spec, _ = qosmod.reconcile_observe(
        [_prof(ddl=100_000_000), _prof(ddl=200_000_000)], QoSSpec(),
        has_explicit=True)
    assert spec.deadline_ms == 200


def test_reconcile_command_strongest():
    cfg = QoSSpec(reliability="BEST_EFFORT", depth=10)
    spec, events = qosmod.reconcile_command(
        [_prof(rlb="RELIABLE", ddl=100_000_000)], cfg)
    assert spec.reliability == "RELIABLE" and spec.deadline_ms == 100
    assert "qosUpgraded" in events
    spec, events = qosmod.reconcile_command([], cfg)
    assert spec == cfg and events == ["noSubscriberFallback"]


def test_strictness_guard_modes():
    spec = QoSSpec(deadline_ms=50)
    offered = [_prof(ddl=None)]
    kept, ev = qosmod.strictness_guard(spec, offered, "reject")
    assert kept == spec and ev == ["strictDeadline"]
    demoted, ev = qosmod.strictness_guard(spec, offered, "demote")
    assert demoted.deadline_ms is None and ev == ["strictDeadline"]


class FakeNode:
    def __init__(self):
        self.pub_infos: list = []
        self.destroyed = 0

    def get_publishers_info_by_topic(self, _iface):
        return self.pub_infos

    def destroy_subscription(self, _sub):
        self.destroyed += 1


def _adapter_with_state(mode="demote"):
    node = FakeNode()
    node.pub_infos = [SimpleNamespace(qos_profile=_prof(rlb="BEST_EFFORT"),
                                      node_name="talker", node_namespace="/")]
    ad = GenericROS2Adapter(node, on_topic_ir=lambda ir: None,
                            on_event=lambda *a: None, qos_strictness=mode)
    cfg = QoSSpec(deadline_ms=50)
    spec = TopicSpec(robot_id="r", interface="/x", msg_type="std_msgs/msg/String",
                     direction="observe", representation="historical", qos=cfg)
    offered = [i.qos_profile for i in node.pub_infos]
    resolved, events = qosmod.reconcile_observe(offered, cfg, has_explicit=True)
    guarded, strict = qosmod.strictness_guard(resolved, offered, mode)
    key = ("r", "/x")
    ad.observes[key] = _ObserveState(
        spec=spec, subscription=object(), applied_qos=guarded,
        offered_peers=[qosmod.endpoint_to_peer(i, "pub") for i in node.pub_infos],
        events=list(dict.fromkeys([*events, *strict])))
    return ad, node, key


def test_refresh_no_flap_when_offered_unchanged():
    # P1 회귀: 가드 적용 후 값끼리 비교 — offered 불변이면 rebind 없음
    ad, node, key = _adapter_with_state()
    assert ad._refresh_observe(key) is False
    assert node.destroyed == 0


def test_refresh_rebinds_on_offered_change():
    ad, node, key = _adapter_with_state()
    node.pub_infos = [SimpleNamespace(qos_profile=_prof(rlb="RELIABLE"),
                                      node_name="talker", node_namespace="/")]
    assert ad._refresh_observe(key) is True
    assert node.destroyed == 1        # 구독 재생성 시도(rebind 경로 진입)
