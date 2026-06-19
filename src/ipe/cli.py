from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ipe.config.loader import ConfigError, load_config
from ipe.config.resolver import ResolveError, resolve
from ipe.config.spec import QoSSpec, ResolvedConfig


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="ipe", description="Generic ROS2 <-> oneM2M IPE")
    p.add_argument("--config", "-c", type=Path, required=True)
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
        config = load_config(args.config)
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

    if args.explain or args.dry_run:
        explain(rc, log)
        return 0

    if args.discover:
        return _discover(log)

    if args.reset:
        _reset_ae(rc, log)   # 삭제 후 정상 부트스트랩으로 계속

    # 런타임 import 실패는 조용한 no-op 대신 시끄럽게 실패시킨다.
    from ipe.runtime.app import run
    return run(rc, args)


def _discover(log: logging.Logger) -> int:
    """그래프 스냅숏 1회 출력 — 설정 작성 전 어떤 인터페이스가 있는지 확인용."""
    import rclpy
    from rclpy.node import Node

    from ipe.adapter.ros2 import GenericROS2Adapter

    rclpy.init()
    node = Node("ipe_discover")
    try:
        import time
        time.sleep(1.0)   # DDS 디스커버리 수렴 대기
        snap = GenericROS2Adapter(node, lambda _ir: None, lambda *_: None).snapshot()
        for kind in ("topics", "services", "actions"):
            log.info("--- %s (%d) ---", kind, len(snap.get(kind, [])))
            for name, types in sorted(snap.get(kind, [])):
                log.info("  %-40s %s", name, ",".join(types))
    finally:
        node.destroy_node()
        rclpy.shutdown()
    return 0


def _reset_ae(rc, log: logging.Logger) -> None:
    from ipe.onem2m.http_client import OneM2MHTTPClient

    client = OneM2MHTTPClient(rc.cse.endpoint, origin=rc.cse.origin, rvi=rc.cse.rvi)
    path = f"/{rc.cse.cse_base}/{rc.cse.ae_name}"
    r = client.delete(path)
    if r.status in (200, 204):
        log.info("DELETED AE %s (reset)", path)
    elif r.status == 404:
        log.info("AE %s already absent (reset noop)", path)
    else:
        log.warning("AE reset DELETE %s -> HTTP %d", path, r.status)


if __name__ == "__main__":
    sys.exit(main())
