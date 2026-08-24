#!/usr/bin/env python3
"""Launch the discovery-driven IPE natively or in its ROS 2 Docker image."""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DEFAULT_IMAGE = "ipe:humble"
IMAGE_SOURCE_LABEL = "ipe.dockerfile.sha256"
PASSTHROUGH_ENV = (
    "IPE_CSE_ENDPOINT",
    "IPE_CSE_BASE",
    "IPE_CSE_TIMEZONE",
    "IPE_CSE_ORIGIN",
    "IPE_CSE_PROTOCOL",
    "IPE_CSE_ID",
    "IPE_AE_NAME",
    "IPE_INSTANCE_ID",
    "IPE_ROBOT_ID",
    "IPE_ROBOT_NAMESPACE",
    "IPE_REFRESH_SEC",
    "IPE_OBSERVE_ONLY",
    "IPE_STATE_DB",
    "IPE_RVI",
    "IPE_GRAPH_TIMEOUT_SEC",
    "IPE_GRAPH_STABLE_POLLS",
    "IPE_GRAPH_POLL_SEC",
    "IPE_ROS_PEER",
    "IPE_MQTT_HOST",
    "IPE_MQTT_PORT",
    "IPE_MQTT_CLIENT_ID",
    "IPE_MQTT_QOS",
    "IPE_MQTT_USERNAME",
    "IPE_MQTT_PASSWORD",
    "IPE_MQTT_TLS",
    "IPE_MQTT_TLS_CA",
    "IPE_MQTT_TLS_CERT",
    "IPE_MQTT_TLS_KEY",
    "IPE_MQTT_TLS_INSECURE",
)


def host_timezone() -> str:
    """Return the host timezone name so tinyIoT local timestamps stay parseable in Docker."""
    configured = os.environ.get("TZ", "").strip()
    if configured:
        return configured
    try:
        detected = Path("/etc/timezone").read_text(encoding="utf-8").strip()
    except OSError:
        detected = ""
    return detected or "UTC"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the discovery-driven ROS2-oneM2M IPE",
        epilog="Examples: main.py | main.py --explain | main.py --ros-peer 192.168.219.106",
    )

    launcher_options = parser.add_argument_group("launcher options")
    launcher_options.add_argument(
        "--docker",
        action="store_true",
        help="Run in the ROS 2 Humble Docker image",
    )
    launcher_options.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f"Docker image name (default: {DEFAULT_IMAGE})",
    )

    ipe_options = parser.add_argument_group("IPE options")
    ipe_options.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Set IPE log verbosity",
    )
    ipe_options.add_argument("--cse-endpoint", help="oneM2M HTTP binding endpoint")
    ipe_options.add_argument("--cse-base", help="CSEBase resource name")
    ipe_options.add_argument(
        "--cse-timezone",
        help="CSE timestamp timezone (IANA name such as Asia/Seoul, or local)",
    )
    ipe_options.add_argument("--ae-name", help="IPE AE resource name")
    ipe_options.add_argument("--instance-id", help="IPE instance identifier")
    ipe_options.add_argument("--robot-id", help="Fallback Robot ID for a root namespace")
    ipe_options.add_argument("--robot-namespace", help="ROS namespace owned by --robot-id")
    ipe_options.add_argument("--domain-id", type=int, help="ROS_DOMAIN_ID")
    ipe_options.add_argument("--ros-peer", help="Cyclone DDS unicast discovery peer IP")
    ipe_options.add_argument("--refresh-sec", type=float, help="ROS Graph reconcile interval")
    ipe_options.add_argument(
        "--observe-only",
        action="store_true",
        help="Disable command topics, services, and actions",
    )
    ipe_options.add_argument(
        "--explain",
        action="store_true",
        help="Print the resolved or discovered Binding Plan and exit",
    )
    ipe_options.add_argument(
        "--dry-run",
        action="store_true",
        help="Alias of --explain",
    )
    ipe_options.add_argument(
        "--discover",
        action="store_true",
        help="Print the discovered ROS2 graph and exit",
    )
    ipe_options.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Create CSE resources and exit without starting the bridge",
    )
    ipe_options.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing AE(s) before bootstrap",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    args, passthrough = build_parser().parse_known_args(argv)
    ipe_args: list[str] = []
    if args.log_level:
        ipe_args.extend(["--log-level", args.log_level])
    for name in (
        "cse_endpoint", "cse_base", "cse_timezone", "ae_name", "instance_id", "robot_id",
        "robot_namespace", "domain_id", "ros_peer", "refresh_sec",
    ):
        value = getattr(args, name)
        if value is not None:
            ipe_args.extend([f"--{name.replace('_', '-')}", str(value)])
    for name in (
        "observe_only", "explain", "dry_run", "discover", "bootstrap_only", "reset",
    ):
        if getattr(args, name):
            ipe_args.append(f"--{name.replace('_', '-')}")
    ipe_args.extend(passthrough)
    return args, ipe_args


def docker_command(
    image: str,
    ipe_args: list[str],
    *,
    tty: bool,
) -> list[str]:
    command = ["docker", "run", "--rm"]
    if tty:
        command.append("-it")
    command.extend(
        [
            "--net=host",
            "--ipc=host",
            "-v",
            f"{ROOT}:/ws",
            "-w",
            "/ws",
            "-e",
            f"ROS_DOMAIN_ID={os.environ.get('ROS_DOMAIN_ID', '30')}",
            "-e",
            f"TZ={host_timezone()}",
            "-e",
            "PYTHONPATH=/ws/src",
        ]
    )
    for name in ("RMW_IMPLEMENTATION", "CYCLONEDDS_URI"):
        value = os.environ.get(name, "").strip()
        if value:
            command.extend(["-e", f"{name}={value}"])
    display = os.environ.get("DISPLAY", "").strip()
    xauthority = os.environ.get("XAUTHORITY", "").strip()
    if display:
        command.extend([
            "-e",
            f"DISPLAY={display}",
            "-v",
            "/tmp/.X11-unix:/tmp/.X11-unix:rw",
        ])
        if xauthority and Path(xauthority).is_file():
            command.extend([
                "-e",
                "XAUTHORITY=/tmp/.ipe.xauthority",
                "-v",
                f"{xauthority}:/tmp/.ipe.xauthority:ro",
            ])
    for name in PASSTHROUGH_ENV:
        if name in os.environ:
            command.extend(["-e", name])
    command.extend([image, "python3", "-m", "ipe"])
    command.extend(ipe_args)
    return command


def ensure_image(image: str) -> None:
    """Build the default image when missing or stale against its Dockerfile."""
    dockerfile = ROOT / "Dockerfile"
    source_digest = hashlib.sha256(dockerfile.read_bytes()).hexdigest()
    inspect_command = ["docker", "image", "inspect"]
    inspect_kwargs: dict[str, Any] = {
        "stderr": subprocess.DEVNULL,
        "check": False,
    }
    if image == DEFAULT_IMAGE:
        inspect_command.extend(
            ["--format", f'{{{{ index .Config.Labels "{IMAGE_SOURCE_LABEL}" }}}}']
        )
        inspect_kwargs.update({"stdout": subprocess.PIPE, "text": True})
    else:
        inspect_kwargs["stdout"] = subprocess.DEVNULL
    inspect_command.append(image)

    try:
        inspected = subprocess.run(inspect_command, **inspect_kwargs)
    except FileNotFoundError as exc:
        raise RuntimeError("docker command was not found") from exc
    if inspected.returncode == 0 and (
        image != DEFAULT_IMAGE or inspected.stdout.strip() == source_digest
    ):
        return
    subprocess.run(
        [
            "docker",
            "build",
            "-f",
            str(dockerfile),
            "-t",
            image,
            "--label",
            f"{IMAGE_SOURCE_LABEL}={source_digest}",
            str(ROOT),
        ],
        check=True,
    )


def native_command(ipe_args: list[str]) -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    command = [sys.executable, "-m", "ipe"]
    command.extend(ipe_args)
    return command, env


def main(argv: list[str] | None = None) -> int:
    args, ipe_args = parse_args(argv)
    try:
        if args.docker:
            ensure_image(args.image)
            command = docker_command(
                args.image,
                ipe_args,
                tty=sys.stdin.isatty() and sys.stdout.isatty(),
            )
            env = os.environ.copy()
        else:
            command, env = native_command(ipe_args)
    except (RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"main.py: {exc}", file=sys.stderr)
        return 2

    mode = f"docker:{args.image}" if args.docker else "native"
    print(f"IPE source=ROS 2 graph runtime={mode}", flush=True)
    os.execvpe(command[0], command, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
