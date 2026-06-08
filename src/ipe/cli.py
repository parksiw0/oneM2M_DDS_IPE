from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ipe.config.loader import ConfigError, load_config
from ipe.config.lookup import build_lookup_tables
from ipe.ir import TopicIR


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ipe",
        description="ROS2 to oneM2M Interworking Proxy Entity",
    )
    parser.add_argument("--config", "-c", type=Path, required=True)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and print mapping summary; do not start IPE",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Print discovered ROS2 topics and exit",
    )
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Create AE/CNT/FCNT in CSE and exit; do not start ROS2 subscription",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="DELETE existing AE in CSE before bootstrap (fresh aei capture)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)
    log = logging.getLogger("ipe")

    try:
        config = load_config(args.config)
    except ConfigError as e:
        log.error("Configuration error: %s", e)
        return 1

    log.info("Configuration loaded: %s", args.config)
    log.info("Platform: %s", config["platform"]["name"])
    log.info("CSE endpoint: %s", config["cse"]["endpoint"])

    tables = build_lookup_tables(config)
    log.info("Lookup tables built: %s", tables)

    if args.dry_run:
        log.info("--- Topic mapping summary ---")
        for topic in config.get("topics", []):
            path = tables.get_resource_path(
                topic["semantic_category"], topic["resource_alias"]
            )
            kind = "FCNT" if topic.get("flexcontainer") else "CNT"
            log.info(
                "  %-45s -> %s [%s, %s]",
                topic["name"],
                path,
                kind,
                topic["representation_policy"],
            )
        log.info("Dry run completed successfully.")
        return 0

    from ipe.runtime.lifecycle import BootstrapError, bootstrap

    try:
        ops = bootstrap(config, reset=args.reset)
    except BootstrapError as e:
        log.error("Bootstrap failed: %s", e)
        return 2

    if args.bootstrap_only:
        log.info("Bootstrap-only mode: exiting after resource creation.")
        return 0

    try:
        from ipe.adapter.ros2 import GenericROS2Adapter
        from ipe.runtime.node import create_node, spin
    except ImportError as e:
        log.error("ROS2 import failed: %s", e)
        log.error("Source ROS2: `source /opt/ros/humble/setup.bash`")
        log.error("Source workspace: `source ~/px4_ros_ws/install/setup.bash`")
        return 1

    node = create_node()
    adapter = GenericROS2Adapter(node, tables)

    if args.discover:
        log.info("--- Discovered ROS2 topics ---")
        for name, type_str in sorted(adapter.discover()):
            log.info("  %-50s %s", name, type_str)
        adapter.shutdown()
        return 0

    from ipe.core.policy import Op, Pipeline

    pipeline = Pipeline(config, tables.path_by_alias)
    counters: dict[str, int] = {}
    errors: dict[str, int] = {}

    def dispatch(op: Op) -> None:
        try:
            if op.kind == "create_cin":
                r = ops.create_cin(op.path, op.content)
                if r.status != 201:
                    raise RuntimeError(f"CIN create failed: {r.status} {r.body!r}")
            elif op.kind == "update_fcnt":
                r = ops.update_fcnt(op.path, op.content)
                if r.status not in (200, 204):
                    raise RuntimeError(f"FCNT update failed: {r.status} {r.body!r}")
            counters[op.topic] = counters.get(op.topic, 0) + 1
            n = counters[op.topic]
            if n in (1, 10, 100) or n % 500 == 0:
                log.info("[%s #%d] %s %s", op.kind, n, op.topic, op.path)
        except Exception as e:
            errors[op.topic] = errors.get(op.topic, 0) + 1
            if errors[op.topic] <= 3 or errors[op.topic] % 100 == 0:
                log.warning("Dispatch error on %s (#%d): %s", op.topic, errors[op.topic], e)

    def on_ir(ir: TopicIR) -> None:
        for op in pipeline.process(ir):
            dispatch(op)

    adapter.bind_topics(on_ir)
    log.info("Spinning... (Ctrl+C to stop)")
    try:
        spin(node)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        adapter.shutdown()
        log.info("Final dispatch counts: %s", dict(sorted(counters.items())))
        if errors:
            log.info("Errors per topic: %s", dict(sorted(errors.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
