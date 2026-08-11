from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("ipe_main_launcher", ROOT / "main.py")
assert SPEC is not None and SPEC.loader is not None
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


def test_resolve_config_accepts_repo_profile():
    config, relative = launcher.resolve_config("config/profiles/turtlebot3.yaml")

    assert config.is_file()
    assert relative == Path("config/profiles/turtlebot3.yaml")


def test_resolve_config_defaults_to_profile_directory_and_yaml_suffix():
    by_name, relative_by_name = launcher.resolve_config("turtlebot3.yaml")
    without_suffix, relative_without_suffix = launcher.resolve_config("turtlebot3")

    assert by_name == without_suffix
    assert relative_by_name == Path("config/profiles/turtlebot3.yaml")
    assert relative_without_suffix == relative_by_name


def test_profile_filename_is_a_positional_argument():
    args, ipe_args = launcher.parse_args(
        ["turtlebot3_report_motion.yaml", "--explain"]
    )

    assert args.config == "turtlebot3_report_motion.yaml"
    assert ipe_args == ["--explain"]


def test_profile_name_may_omit_yaml_suffix():
    args, ipe_args = launcher.parse_args(["turtlebot3_report_motion", "--discover"])

    assert args.config == "turtlebot3_report_motion"
    assert ipe_args == ["--discover"]


def test_help_lists_launcher_and_ipe_options():
    help_text = launcher.build_parser().format_help()

    for option in (
        "--image",
        "--native",
        "--log-level",
        "--explain",
        "--dry-run",
        "--discover",
        "--bootstrap-only",
        "--reset",
    ):
        assert option in help_text


def test_registered_ipe_options_are_forwarded():
    _, ipe_args = launcher.parse_args(
        ["turtlebot3", "--log-level", "DEBUG", "--explain", "--reset"]
    )

    assert ipe_args == ["--log-level", "DEBUG", "--explain", "--reset"]


def test_configless_discovery_options_do_not_become_a_profile_name():
    args, ipe_args = launcher.parse_args([
        "--ros-peer", "192.168.219.106",
        "--domain-id", "30",
        "--robot-id", "tb3",
    ])

    assert args.config is None
    assert ipe_args == [
        "--robot-id", "tb3",
        "--domain-id", "30",
        "--ros-peer", "192.168.219.106",
    ]


def test_missing_profile_builds_configless_docker_command():
    command = launcher.docker_command(
        "ipe:humble",
        None,
        ["--ros-peer", "192.168.219.106", "--robot-id", "tb3"],
        tty=False,
    )

    assert "--config" not in command
    assert command[-4:] == [
        "--ros-peer", "192.168.219.106", "--robot-id", "tb3",
    ]


def test_docker_command_runs_python_module_without_shell():
    command = launcher.docker_command(
        "ipe:humble",
        Path("config/profiles/turtlebot3_motion_demo.yaml"),
        ["--explain"],
        tty=False,
    )

    assert "bash" not in command
    assert f"TZ={launcher.host_timezone()}" in command
    assert command[-6:] == [
        "python3",
        "-m",
        "ipe",
        "--config",
        "/ws/config/profiles/turtlebot3_motion_demo.yaml",
        "--explain",
    ]


def test_native_command_passes_config_and_pythonpath():
    config, _ = launcher.resolve_config("config/profiles/turtlebot3.yaml")
    command, env = launcher.native_command(config, ["--discover"])

    assert command[-3:] == ["--config", str(config), "--discover"]
    assert str(ROOT / "src") in env["PYTHONPATH"].split(":")
