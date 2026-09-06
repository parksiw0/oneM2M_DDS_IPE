"""Select the ROS 2 RMW implementation from discovered DDS endpoint GIDs."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

FAST_DDS_RMW = "rmw_fastrtps_cpp"
CYCLONE_DDS_RMW = "rmw_cyclonedds_cpp"
RMW_CANDIDATES = (FAST_DDS_RMW, CYCLONE_DDS_RMW)
VENDOR_RMW = {
    "01.0f": (FAST_DDS_RMW, "eProsima Fast DDS"),
    "01.10": (CYCLONE_DDS_RMW, "Eclipse Cyclone DDS"),
}
PROBE_PREFIX = "IPE_RMW_PROBE="


class RMWSelectionError(RuntimeError):
    """Report a safe automatic-selection failure."""


@dataclass(frozen=True)
class RMWSelection:
    implementation: str
    vendor_id: str
    vendor_name: str
    endpoint_count: int


def cyclone_peer_uri(peer: str) -> str:
    """Return a Cyclone DDS peer configuration."""
    return (
        "<CycloneDDS><Domain><Discovery><Peers>"
        f'<Peer address="{peer}"/>'
        "</Peers></Discovery></Domain></CycloneDDS>"
    )


def select_rmw(
    *,
    domain_id: int,
    robot_namespace: str,
    peer: str | None,
    timeout_sec: float,
) -> RMWSelection:
    """Probe the target graph with isolated processes and select one known vendor."""
    probe_timeout = max(0.5, timeout_sec / len(RMW_CANDIDATES))
    endpoints: dict[tuple[str, str, str], dict[str, Any]] = {}
    probe_errors: list[str] = []
    available_candidates: set[str] = set()
    for candidate in RMW_CANDIDATES:
        command = [
            sys.executable,
            "-m",
            "ipe.adapter.rmw",
            "--probe",
            "--timeout-sec",
            str(probe_timeout),
            "--robot-namespace",
            robot_namespace,
        ]
        env = os.environ.copy()
        env["ROS_DOMAIN_ID"] = str(domain_id)
        env["RMW_IMPLEMENTATION"] = candidate
        if candidate == CYCLONE_DDS_RMW and peer:
            env["CYCLONEDDS_URI"] = cyclone_peer_uri(peer)
        elif candidate != CYCLONE_DDS_RMW:
            env.pop("CYCLONEDDS_URI", None)
        try:
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except OSError as exc:
            probe_errors.append(f"{candidate}: {exc}")
            continue
        try:
            stdout, stderr = process.communicate(timeout=probe_timeout + 5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            probe_errors.append(f"{candidate}: probe process timed out")
            continue
        payload = _parse_probe_output(stdout)
        if process.returncode != 0 or payload is None:
            detail = stderr.strip().splitlines()[-1] if stderr.strip() else "no probe result"
            probe_errors.append(f"{candidate}: {detail}")
            continue
        available_candidates.add(candidate)
        for endpoint in payload.get("endpoints", []):
            key = (
                str(endpoint.get("topic", "")),
                str(endpoint.get("kind", "")),
                str(endpoint.get("gid", "")),
            )
            endpoints[key] = endpoint

    if not endpoints:
        target = f" in namespace {robot_namespace!r}" if robot_namespace else ""
        detail = "; ".join(probe_errors)
        suffix = f" Probe errors: {detail}" if detail else ""
        raise RMWSelectionError(
            f"No ROS 2 endpoints were discovered{target} on domain {domain_id}; "
            f"start the robot or set discovery.rmw_implementation in config.py.{suffix}"
        )

    vendors: dict[str, int] = {}
    for endpoint in endpoints.values():
        vendor_id = str(endpoint.get("vendor_id", ""))
        vendors[vendor_id] = vendors.get(vendor_id, 0) + 1
    if len(vendors) != 1:
        found = ", ".join(
            f"{vendor or 'unknown'} ({count})" for vendor, count in sorted(vendors.items())
        )
        raise RMWSelectionError(
            f"Mixed DDS endpoint vendors were discovered: {found}; "
            "set discovery.rmw_implementation in config.py."
        )

    vendor_id, endpoint_count = next(iter(vendors.items()))
    mapped = VENDOR_RMW.get(vendor_id)
    if mapped is None:
        raise RMWSelectionError(
            f"DDS vendor ID {vendor_id or 'unknown'} is not supported for automatic selection; "
            "set discovery.rmw_implementation in config.py."
        )
    implementation, vendor_name = mapped
    if implementation not in available_candidates:
        raise RMWSelectionError(
            f"The target uses {vendor_name} ({vendor_id}), but {implementation} is not available; "
            "install it or set discovery.rmw_implementation in config.py."
        )
    return RMWSelection(implementation, vendor_id, vendor_name, endpoint_count)


def _parse_probe_output(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith(PROBE_PREFIX):
            try:
                value = json.loads(line[len(PROBE_PREFIX) :])
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None
    return None


def _namespace_matches(endpoint_namespace: str, target_namespace: str) -> bool:
    target = target_namespace.strip()
    if not target or target == "/":
        return True
    if not target.startswith("/"):
        target = "/" + target
    target = target.rstrip("/")
    namespace = endpoint_namespace.rstrip("/") or "/"
    return namespace == target or namespace.startswith(target + "/")


def _endpoint_gid(info: Any) -> bytes:
    try:
        return bytes(info.endpoint_gid)
    except (TypeError, ValueError):
        return b""


def _snapshot_endpoints(node: Any, target_namespace: str) -> list[dict[str, str]]:
    own_name = node.get_name()
    own_namespace = node.get_namespace()
    endpoints: dict[tuple[str, str, str], dict[str, str]] = {}
    for topic, _types in node.get_topic_names_and_types():
        for kind, getter in (
            ("publisher", node.get_publishers_info_by_topic),
            ("subscription", node.get_subscriptions_info_by_topic),
        ):
            try:
                infos = getter(topic)
            except Exception:
                continue
            for info in infos:
                node_name = str(getattr(info, "node_name", ""))
                node_namespace = str(getattr(info, "node_namespace", ""))
                if node_name == own_name and node_namespace == own_namespace:
                    continue
                if not _namespace_matches(node_namespace, target_namespace):
                    continue
                gid = _endpoint_gid(info)
                if len(gid) < 2 or not any(gid):
                    continue
                gid_hex = gid.hex()
                key = (topic, kind, gid_hex)
                endpoints[key] = {
                    "topic": topic,
                    "kind": kind,
                    "node_name": node_name,
                    "node_namespace": node_namespace,
                    "gid": gid_hex,
                    "vendor_id": gid[:2].hex("."),
                }
    return [endpoints[key] for key in sorted(endpoints)]


def _run_probe(timeout_sec: float, robot_namespace: str) -> int:
    import rclpy
    from rclpy.node import Node

    rclpy.init(args=None)
    node: Any = None
    try:
        try:
            node = Node("ipe_rmw_probe", enable_rosout=False, start_parameter_services=False)
        except TypeError:
            node = Node("ipe_rmw_probe")
        deadline = time.monotonic() + max(0.5, timeout_sec)
        previous: list[dict[str, str]] = []
        stable_polls = 0
        while time.monotonic() < deadline:
            rclpy.spin_once(node, timeout_sec=0.2)
            current = _snapshot_endpoints(node, robot_namespace)
            if current and current == previous:
                stable_polls += 1
                if stable_polls >= 2:
                    break
            else:
                stable_polls = 0
            previous = current
        print(PROBE_PREFIX + json.dumps({"endpoints": previous}, separators=(",", ":")))
        return 0
    finally:
        if node is not None:
            node.destroy_node()
        rclpy.shutdown()


def _probe_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--robot-namespace", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _probe_parser().parse_args(argv)
    if not args.probe:
        print("ipe.adapter.rmw is an internal probe module", file=sys.stderr)
        return 2
    try:
        return _run_probe(args.timeout_sec, args.robot_namespace)
    except Exception as exc:
        print(f"RMW probe failed: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
