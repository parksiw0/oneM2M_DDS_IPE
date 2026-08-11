from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from ipe.cli import _configure_ros_environment
from ipe.config.loader import ConfigError, validate_config
from ipe.config.resolver import resolve
from ipe.config.runtime_config import discovery_runtime_config


def _resolved():
    args = SimpleNamespace(
        cse_endpoint=None,
        cse_base=None,
        ae_name=None,
        instance_id=None,
        robot_id=None,
        robot_namespace=None,
        refresh_sec=None,
        domain_id=None,
    )
    return resolve(validate_config(discovery_runtime_config(args, {})))


def test_ros_peer_and_domain_are_applied_before_rclpy_init(monkeypatch):
    monkeypatch.delenv("ROS_DOMAIN_ID", raising=False)
    monkeypatch.delenv("CYCLONEDDS_URI", raising=False)
    args = SimpleNamespace(domain_id=30, ros_peer="192.168.219.106")

    _configure_ros_environment(args, _resolved())

    assert os.environ["ROS_DOMAIN_ID"] == "30"
    uri = os.environ["CYCLONEDDS_URI"]
    assert '<Peer address="192.168.219.106"/>' in uri


def test_invalid_ros_peer_is_rejected():
    args = SimpleNamespace(domain_id=30, ros_peer="robot.local")

    with pytest.raises(ConfigError, match="must be an IP address"):
        _configure_ros_environment(args, _resolved())


def test_configless_origin_is_unique_per_ae():
    args = SimpleNamespace(
        cse_endpoint=None,
        cse_base=None,
        ae_name="warehouse-ipe",
        instance_id=None,
        robot_id=None,
        robot_namespace=None,
        refresh_sec=None,
        domain_id=None,
    )

    generated = resolve(validate_config(discovery_runtime_config(args, {})))
    overridden = resolve(validate_config(discovery_runtime_config(
        args, {"IPE_CSE_ORIGIN": "CExplicitIPE"},
    )))

    assert generated.cse.origin == "Cwarehouse-ipe"
    assert overridden.cse.origin == "CExplicitIPE"
