from __future__ import annotations

import ipaddress
import logging
import os
import sys
import time
from types import SimpleNamespace

import config as settings
from ipe.config.loader import ConfigError, load_config
from ipe.config.resolver import ResolveError, resolve
from ipe.config.spec import QoSSpec, ResolvedConfig
from ipe.rmw_selection import CYCLONE_DDS_RMW, RMWSelectionError, cyclone_peer_uri, select_rmw

log = logging.getLogger(__name__)

CYCLONE_RMW = CYCLONE_DDS_RMW


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


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
    target = rc.cse.endpoint
    if rc.cse.protocol == "mqtt" and rc.cse.mqtt is not None:
        target = f"mqtt://{rc.cse.mqtt.host}:{rc.cse.mqtt.port}"
    log.info("=== Bridge plan: %s -> AE %s @ %s (RVI %s) ===",
             rc.instance_id, rc.cse.ae_name, target, rc.cse.rvi)
    log.info("robots: %s", ", ".join(f"{r.id}(ns='{r.namespace}')" for r in rc.robots.values()))
    log.info("discovery: mode=%s", rc.discovery.get("mode"))

    if rc.topics:
        log.info("--- topics (%d) ---", len(rc.topics))
    for t in sorted(rc.topics, key=lambda x: (x.robot_id, x.interface)):
        branch = {
            "observe": "topics/observe",
            "command": "topics/command",
            "both": "topics/observe+topics/command",
        }[t.direction]
        qos_text = _qos_str(t.qos_for("observe" if t.direction != "command" else "command"))
        if t.direction == "both":
            qos_text = f"observe={qos_text}; command={_qos_str(t.qos_for('command'))}"
        log.info("  [%s] %-32s %-9s -> %s/%s | %s | rep=%s | qos:%s | rule:%s%s",
                 t.robot_id, t.interface, t.direction, branch, t.rel_path,
                 t.msg_type or "(type@discovery)", t.representation, qos_text,
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
    if (argv if argv is not None else sys.argv[1:]):
        print("Set parameters in config.py and run ipe without arguments.", file=sys.stderr)
        return 2
    log = logging.getLogger("ipe")
    try:
        if settings.RUN_MODE not in ("run", "discover", "explain", "bootstrap"):
            raise ConfigError("config.py: RUN_MODE must be run, discover, explain, or bootstrap")
        if not isinstance(settings.RESET_AE, bool):
            raise ConfigError("config.py: RESET_AE must be True or False")
        os.environ["TZ"] = settings.TIMEZONE
        time.tzset()
        config = load_config()
        os.environ["PGPASSWORD"] = settings.POSTGRES_PASSWORD
    except (ConfigError, TypeError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    setup_logging(config["logging"]["level"])

    try:
        rc = resolve(config, discovered=None)
    except ResolveError as exc:
        print(f"Resolution error: {exc}", file=sys.stderr)
        return 1

    try:
        _configure_ros_environment(rc)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    if settings.RUN_MODE in ("explain", "discover"):
        return _discover(log, rc, explain_plan=settings.RUN_MODE == "explain")
    if settings.RESET_AE:
        _reset_ae(rc, log)

    from ipe.runtime.app import run
    return run(rc, SimpleNamespace(bootstrap_only=settings.RUN_MODE == "bootstrap"))


def _configure_ros_environment(rc: ResolvedConfig) -> None:
    """Apply config.py values before starting DDS; inherited overrides are ignored."""
    domain_id = rc.discovery["domain_id"]
    peer = rc.discovery["ros_peer"]
    implementation = rc.discovery["rmw_implementation"].strip()
    if peer:
        try:
            ipaddress.ip_address(peer)
        except ValueError as exc:
            raise ConfigError(f"config.py: discovery.ros_peer must be an IP address: {peer!r}") from exc
    os.environ["ROS_DOMAIN_ID"] = str(domain_id)
    os.environ.pop("CYCLONEDDS_URI", None)
    os.environ.pop("RMW_IMPLEMENTATION", None)
    if not implementation:
        robots = list(rc.robots.values())
        try:
            selected = select_rmw(
                domain_id=domain_id,
                robot_namespace=robots[0].namespace if len(robots) == 1 else "",
                peer=peer or None,
                timeout_sec=rc.discovery["graph_settle_timeout_sec"],
            )
        except RMWSelectionError as exc:
            raise ConfigError(str(exc)) from exc
        implementation = selected.implementation
    os.environ["RMW_IMPLEMENTATION"] = implementation
    log.info("DDS RMW selected: %s", implementation)
    if peer:
        if implementation == CYCLONE_RMW:
            os.environ["CYCLONEDDS_URI"] = cyclone_peer_uri(peer)
        else:
            log.warning("config.py: ros_peer %s requires %s; selected %s",
                        peer, CYCLONE_RMW, implementation)


def _discover(log: logging.Logger, rc: ResolvedConfig, *, explain_plan: bool = False) -> int:
    """Wait for graph convergence and print the graph or its binding plan."""
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
    client.start()
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
