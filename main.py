#!/usr/bin/env python3
"""IPE 종합 실행기.

호스트에 ROS 2 Humble이 없어도 저장소의 Docker 이미지를 통해 IPE를 실행한다.
공식 ROS 이미지의 entrypoint가 ROS 환경을 설정하므로 별도 shell/source가 필요 없다.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROFILE_DIR = ROOT / "config" / "profiles"
DEFAULT_IMAGE = "ipe:humble"


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
        epilog=(
            "Examples: main.py --ros-peer 192.168.219.106 | "
            "main.py turtlebot3.yaml | "
            "main.py turtlebot3_report_motion | "
            "main.py config/profiles/turtlebot3.yaml --explain"
        ),
    )
    parser.add_argument(
        "config",
        nargs="?",
        help=(
            "optional legacy/profile YAML. When omitted, the live ROS 2 graph is authoritative"
        ),
    )

    launcher_options = parser.add_argument_group("launcher options")
    launcher_options.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f"Docker image name (default: {DEFAULT_IMAGE})",
    )
    launcher_options.add_argument(
        "--native",
        action="store_true",
        help="Run in the current ROS environment instead of Docker",
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
    for name in ("explain", "dry_run", "discover", "bootstrap_only", "reset"):
        if getattr(args, name):
            ipe_args.append(f"--{name.replace('_', '-')}")
    ipe_args.extend(passthrough)
    return args, ipe_args


def available_profiles() -> list[str]:
    """Return profile names accepted by the short launcher form."""
    return sorted(
        {
            profile.stem
            for pattern in ("*.yaml", "*.yml")
            for profile in PROFILE_DIR.glob(pattern)
            if profile.is_file()
        }
    )


def resolve_config(value: str) -> tuple[Path, Path]:
    candidate = Path(value)
    if candidate.is_absolute():
        config = candidate.resolve()
    elif candidate.parent == Path("."):
        filename = candidate.name
        if candidate.suffix == "":
            filename += ".yaml"
        config = (PROFILE_DIR / filename).resolve()
    else:
        config = (ROOT / candidate).resolve()
    try:
        relative = config.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError(f"config must be inside {ROOT}: {config}") from exc
    if not config.is_file():
        raise ValueError(f"config file does not exist: {config}")
    return config, relative


def docker_command(
    image: str,
    relative_config: Path | None,
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
            f"RMW_IMPLEMENTATION={os.environ.get('RMW_IMPLEMENTATION', 'rmw_cyclonedds_cpp')}",
            "-e",
            f"CYCLONEDDS_URI={os.environ.get('CYCLONEDDS_URI', '')}",
            "-e",
            f"TZ={host_timezone()}",
            "-e",
            "PYTHONPATH=/ws/src",
            image,
            "python3",
            "-m",
            "ipe",
        ]
    )
    if relative_config is not None:
        command.extend(["--config", f"/ws/{relative_config.as_posix()}"])
    command.extend(ipe_args)
    return command


def ensure_image(image: str) -> None:
    try:
        inspected = subprocess.run(
            ["docker", "image", "inspect", image],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("docker command was not found") from exc
    if inspected.returncode == 0:
        return
    subprocess.run(
        [
            "docker",
            "build",
            "-f",
            str(ROOT / "tools/humble.Dockerfile"),
            "-t",
            image,
            str(ROOT / "tools"),
        ],
        check=True,
    )


def native_command(config: Path | None, ipe_args: list[str]) -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    command = [sys.executable, "-m", "ipe"]
    if config is not None:
        command.extend(["--config", str(config)])
    command.extend(ipe_args)
    return command, env


def main(argv: list[str] | None = None) -> int:
    args, ipe_args = parse_args(argv)
    try:
        if args.config is None:
            config, relative = None, None
        else:
            config, relative = resolve_config(args.config)
        if args.native:
            command, env = native_command(config, ipe_args)
        else:
            ensure_image(args.image)
            command = docker_command(
                args.image,
                relative,
                ipe_args,
                tty=sys.stdin.isatty() and sys.stdout.isatty(),
            )
            env = os.environ.copy()
    except (RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"main.py: {exc}", file=sys.stderr)
        return 2

    mode = "native" if args.native else f"docker:{args.image}"
    source = relative.as_posix() if relative is not None else "ROS 2 graph (no YAML)"
    print(f"IPE source={source} runtime={mode}", flush=True)
    os.execvpe(command[0], command, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
