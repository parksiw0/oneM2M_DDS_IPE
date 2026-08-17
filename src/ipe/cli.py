from __future__ import annotations

import argparse
import ipaddress
import logging
import os
import sys
from pathlib import Path

from ipe.config.loader import ConfigError, load_config, validate_config
from ipe.config.resolver import ResolveError, resolve
from ipe.config.runtime_config import discovery_runtime_config
from ipe.config.spec import QoSSpec, ResolvedConfig

log = logging.getLogger(__name__)

CYCLONE_RMW = "rmw_cyclonedds_cpp"


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="ipe", description="Generic ROS2 <-> oneM2M IPE")
    p.add_argument(
        "--config", "-c", type=Path,
        help="Optional deployment YAML. Without it, interfaces come from the ROS 2 graph.",
    )
    p.add_argument("--cse-endpoint", help="oneM2M HTTP binding endpoint")
    p.add_argument("--cse-base", help="CSEBase resource name")
    p.add_argument(
        "--cse-timezone",
        help="CSE timestamp timezone (IANA name such as Asia/Seoul, or local)",
    )
    p.add_argument("--ae-name", help="IPE AE resource name")
    p.add_argument("--instance-id", help="IPE instance identifier")
    p.add_argument("--robot-id", help="Robot CNT name for an un-namespaced ROS graph")
    p.add_argument("--robot-namespace", help="ROS namespace owned by --robot-id")
    p.add_argument("--domain-id", type=int, help="ROS_DOMAIN_ID")
    p.add_argument("--ros-peer", help="Cyclone DDS unicast discovery peer IP")
    p.add_argument("--refresh-sec", type=float, help="ROS graph reconcile interval")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--explain", action="store_true",
                   help="Resolve config (no ROS2/CSE) and print the bridge plan per interface")
    p.add_argument("--dry-run", action="store_true", help="Alias of --explain")
    p.add_argument("--discover", action="store_true", help="Print discovered ROS2 graph and exit")
    p.add_argument("--bootstrap-only", action="store_true", help="Create CSE resources and exit")
    p.add_argument("--reset", action="store_true", help="Delete existing AE(s) before bootstrap")
    return p.parse_args(argv)


def _qos_str(q: QoSSpec) -> str:
    parts = [q.reliability[:3], q.durability[:3], f"{q.history}/{q.depth}"]
    if q.deadline_ms is not None:
        parts.append(f"dl={q.deadline_ms}ms")
    if q.lifespan_ms is not None:
        parts.append(f"ls={q.lifespan_ms}ms")
    if q.liveliness != "AUTOMATIC":
        parts.append(f"liv={q.liveliness}")
    return " ".join(parts)


def explain(rc: ResolvedConfig, log: logging.Logger) -> None:
    log.info("=== Bridge plan: %s -> AE %s @ %s (RVI %s) ===",
             rc.instance_id, rc.cse.ae_name, rc.cse.endpoint, rc.cse.rvi)
    log.info("robots: %s", ", ".join(f"{r.id}(ns='{r.namespace}')" for r in rc.robots.values()))
    log.info("discovery: mode=%s", rc.discovery.get("mode"))

    if rc.topics:
        log.info("--- topics (%d) ---", len(rc.topics))
    for t in sorted(rc.topics, key=lambda x: (x.robot_id, x.interface)):
        branch = {"observe": "ros2Data", "command": "ros2Command", "both": "ros2Data+ros2Command"}[t.direction]
        log.info("  [%s] %-32s %-9s -> %s/%s | %s | rep=%s | qos:%s | rule:%s%s",
                 t.robot_id, t.interface, t.direction, branch, t.rel_path,
                 t.msg_type or "(type@discovery)", t.representation, _qos_str(t.qos),
                 t.source_rule, "  ACCESS:ON" if t.access_enabled else "")
    if rc.services:
        log.info("--- services (%d) ---", len(rc.services))
    for s in sorted(rc.services, key=lambda x: (x.robot_id, x.interface)):
        log.info("  [%s] %-32s -> services/%s | %s | timeout=%dms | rule:%s",
                 s.robot_id, s.interface, s.rel_path, s.srv_type or "(type@discovery)",
                 s.timeout_ms, s.source_rule)
    if rc.actions:
        log.info("--- actions (%d) ---", len(rc.actions))
    for a in sorted(rc.actions, key=lambda x: (x.robot_id, x.interface)):
        log.info("  [%s] %-32s -> actions/%s | %s | feedback=%s | rule:%s",
                 a.robot_id, a.interface, a.rel_path, a.action_type or "(type@discovery)",
                 a.feedback, a.source_rule)

    pending = [s.interface for grp in (rc.topics, rc.services, rc.actions) for s in grp
               if s.source_rule.startswith("AMBIGUOUS_TYPE")]
    if pending:
        log.warning("ambiguous types (refuse to bind until disambiguated): %s", pending)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)
    log = logging.getLogger("ipe")

    try:
        config = (load_config(args.config) if args.config is not None
                  else validate_config(discovery_runtime_config(args)))
    except ConfigError as e:
        log.error("Configuration error: %s", e)
        return 1

    # 설정 시점 해석: 명시적 이름은 지금, 패턴은 디스커버리 때 확장된다.
    # ROS2 없이 --explain을 돌리기엔 이것으로 충분하다.
    try:
        rc = resolve(config, discovered=None)
    except ResolveError as e:
        log.error("Resolution error: %s", e)
        return 1

    try:
        _configure_ros_environment(args, rc)
    except ConfigError as e:
        log.error("Configuration error: %s", e)
        return 1
    args.configless = args.config is None

    if args.explain or args.dry_run:
        if args.configless:
            return _discover(log, rc, explain_plan=True)
        explain(rc, log)
        return 0

    if args.discover:
        return _discover(log, rc)

    if args.reset:
        _reset_ae(rc, log)   # 삭제 후 정상 부트스트랩으로 계속

    # 런타임 import 실패는 조용한 no-op 대신 시끄럽게 실패시킨다.
    from ipe.runtime.app import run
    return run(rc, args)


def _configure_ros_environment(args: argparse.Namespace, rc: ResolvedConfig) -> None:
    """rclpy.init() 전에 DDS Domain과 선택적 Cyclone DDS peer를 적용한다.

    ``--ros-peer``는 Cyclone DDS 설정이다. RMW가 아직 선택되지 않았다면
    Cyclone을 선택하고, 사용자가 다른 RMW를 명시했다면 설정이 적용되지
    않는다는 경고를 남긴다. 이렇게 해야 Fast DDS 실행에서 peer IP가 실제로
    사용된 것처럼 보이는 조용한 오설정을 피할 수 있다.
    """
    domain_id = args.domain_id
    if domain_id is None:
        domain_id = int(rc.discovery.get("domain_id", os.environ.get("ROS_DOMAIN_ID", 0)))
    os.environ["ROS_DOMAIN_ID"] = str(domain_id)

    peer = args.ros_peer or os.environ.get("IPE_ROS_PEER")
    if not peer:
        return
    try:
        ipaddress.ip_address(peer)
    except ValueError as e:
        raise ConfigError(f"--ros-peer must be an IP address: {peer!r}") from e

    rmw = os.environ.get("RMW_IMPLEMENTATION", "").strip()
    if not rmw:
        rmw = CYCLONE_RMW
        os.environ["RMW_IMPLEMENTATION"] = rmw
    if rmw != CYCLONE_RMW:
        log.warning(
            "--ros-peer %s was not applied: RMW_IMPLEMENTATION=%s; "
            "use %s or configure discovery for the selected RMW",
            peer, rmw, CYCLONE_RMW,
        )
        return
    os.environ["CYCLONEDDS_URI"] = (
        "<CycloneDDS><Domain><Discovery><Peers>"
        f'<Peer address="{peer}"/>'
        "</Peers></Discovery></Domain></CycloneDDS>"
    )


def _discover(log: logging.Logger, rc: ResolvedConfig, *, explain_plan: bool = False) -> int:
    """수렴한 graph 또는 graph에서 계산한 DesiredBindingPlan을 출력한다."""
    import rclpy
    from rclpy.node import Node

    from ipe.adapter.ros2 import GenericROS2Adapter
    from ipe.runtime.discovery import GraphNotReady, await_graph_convergence

    rclpy.init()
    try:
        try:
            node = Node("ipe_discover", enable_rosout=False,
                        start_parameter_services=False)
        except TypeError:
            node = Node("ipe_discover")
        adapter = GenericROS2Adapter(node, lambda _ir: None, lambda *_: None)
        disc = rc.discovery
        state = await_graph_convergence(
            adapter.snapshot,
            timeout_sec=float(disc.get("graph_settle_timeout_sec", 10)),
            stable_polls=int(disc.get("graph_stable_polls", 2)),
            poll_sec=float(disc.get("graph_poll_sec", 0.5)),
        )
        snap = state.snapshot
        if explain_plan:
            explain(resolve(rc.raw, discovered=snap), log)
            return 0
        for kind in ("topics", "services", "actions"):
            log.info("--- %s (%d) ---", kind, len(snap.get(kind, [])))
            for name, types in sorted(snap.get(kind, [])):
                direction = snap.get("topic_directions", {}).get(name)
                suffix = f" [{direction}]" if direction else ""
                log.info("  %-40s %s%s", name, ",".join(types), suffix)
    except GraphNotReady as e:
        log.error("Discovery not ready: %s", e)
        return 2
    finally:
        if "node" in locals():
            node.destroy_node()
        rclpy.shutdown()
    return 0


def _reset_ae(rc, log: logging.Logger) -> None:
    from ipe.onem2m.client import TransportError, make_onem2m_client

    client = make_onem2m_client(rc, rc.cse.origin)
    client.start()   # MQTT는 브로커 연결 (HTTP는 no-op)
    try:
        path = f"/{rc.cse.cse_base}/{rc.cse.ae_name}"
        r = client.delete(path)
        if r.status in (200, 204):
            log.info("DELETED AE %s (reset)", path)
        elif r.status == 404:
            log.info("AE %s already absent (reset noop)", path)
        else:
            log.warning("AE reset DELETE %s -> status %d rsc=%s", path, r.status, r.rsc)
    except TransportError as e:
        log.warning("AE reset failed to reach CSE: %s", e)
    finally:
        client.stop()


if __name__ == "__main__":
    sys.exit(main())
