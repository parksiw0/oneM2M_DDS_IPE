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
        description="Run ROS2-oneM2M IPE with the selected YAML profile",
        epilog=(
            "Examples: main.py turtlebot3.yaml | "
            "main.py turtlebot3_report_motion | "
            "main.py config/profiles/turtlebot3.yaml --explain"
        ),
    )
    parser.add_argument(
        "config",
        nargs="?",
        help=(
            "profile filename under config/profiles (extension optional), "
            "or a path relative to this repository"
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
    ipe_options.add_argument(
        "--explain",
        action="store_true",
        help="Print the resolved bridge plan without connecting to ROS2 or the CSE",
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


def print_missing_config_help() -> None:
    print("오류: 실행할 YAML 프로파일 이름을 입력하지 않았습니다.", file=sys.stderr)
    print("", file=sys.stderr)
    print("사용법:", file=sys.stderr)
    print("  python3 main.py <프로파일명> [IPE 옵션]", file=sys.stderr)
    print("", file=sys.stderr)
    print("예시:", file=sys.stderr)
    print("  python3 main.py turtlebot3_report_motion", file=sys.stderr)
    print("  python3 main.py turtlebot3_qos_ok --explain", file=sys.stderr)

    profiles = available_profiles()
    if profiles:
        print("", file=sys.stderr)
        print("사용 가능한 프로파일 (config/profiles):", file=sys.stderr)
        for profile in profiles:
            print(f"  - {profile}", file=sys.stderr)

    print("", file=sys.stderr)
    print("전체 도움말: python3 main.py --help", file=sys.stderr)


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
    relative_config: Path,
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
            "--config",
            f"/ws/{relative_config.as_posix()}",
            *ipe_args,
        ]
    )
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


def native_command(config: Path, ipe_args: list[str]) -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return [sys.executable, "-m", "ipe", "--config", str(config), *ipe_args], env


def main(argv: list[str] | None = None) -> int:
    args, ipe_args = parse_args(argv)
    if args.config is None:
        print_missing_config_help()
        return 2

    try:
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
    print(f"IPE config={relative.as_posix()} runtime={mode}", flush=True)
    os.execvpe(command[0], command, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
