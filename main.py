#!/usr/bin/env python3
"""Launch the discovery-driven IPE natively or in its ROS 2 Docker image."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import config as settings

ROOT = Path(__file__).resolve().parent
DEFAULT_IMAGE = settings.DOCKER_IMAGE
IMAGE_SOURCE_LABEL = "ipe.dockerfile.sha256"


def docker_command(
    image: str,
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
            f"TZ={settings.TIMEZONE}",
            "-e",
            "PYTHONPATH=/ws:/ws/src",
        ]
    )
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
    command.extend([image, "python3", "-m", "ipe"])
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


def native_command() -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    src = os.pathsep.join((str(ROOT), str(ROOT / "src")))
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    command = [sys.executable, "-m", "ipe"]
    return command, env


def main(argv: list[str] | None = None) -> int:
    if (argv if argv is not None else sys.argv[1:]):
        print("Set parameters in config.py and run main.py without arguments.", file=sys.stderr)
        return 2
    try:
        if not isinstance(settings.USE_DOCKER, bool):
            raise ValueError("config.py: USE_DOCKER must be True or False")
        if settings.USE_DOCKER:
            ensure_image(settings.DOCKER_IMAGE)
            command = docker_command(
                settings.DOCKER_IMAGE,
                tty=sys.stdin.isatty() and sys.stdout.isatty(),
            )
            env = os.environ.copy()
        else:
            command, env = native_command()
    except (RuntimeError, ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"main.py: {exc}", file=sys.stderr)
        return 2

    mode = f"docker:{settings.DOCKER_IMAGE}" if settings.USE_DOCKER else "native"
    print(f"IPE config=config.py runtime={mode}", flush=True)
    os.execvpe(command[0], command, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
