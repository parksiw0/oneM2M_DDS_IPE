from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ipe.config.loader import validate_config
from ipe.config.resolver import ResolveError, resolve
from ipe.core.normalize import ct_to_epoch


def _epoch(value: str) -> float:
    return datetime.fromisoformat(value).astimezone(timezone.utc).timestamp()


def test_cse_local_timestamp_uses_explicit_timezone():
    now = _epoch("2026-08-17T21:00:01+00:00")

    parsed = ct_to_epoch(
        "20260818T060000,250",
        now=now,
        cse_timezone="Asia/Seoul",
    )

    assert parsed == pytest.approx(_epoch("2026-08-17T21:00:00.250+00:00"))


def test_standard_utc_timestamp_remains_valid_with_cse_timezone():
    now = _epoch("2026-08-17T21:00:01+00:00")

    parsed = ct_to_epoch(
        "20260817T210000,500",
        now=now,
        cse_timezone="Asia/Seoul",
    )

    assert parsed == pytest.approx(_epoch("2026-08-17T21:00:00.500+00:00"))


def test_invalid_cse_timezone_is_rejected_during_resolution():
    cfg = validate_config({
        "cse": {
            "endpoint": "http://127.0.0.1:3000",
            "cse_base": "TinyIoT",
            "ae_name": "ros2-ipe",
            "timezone": "Mars/Olympus_Mons",
        },
        "qos_profiles": {
            "sensor_data": {"reliability": "best_effort", "depth": 5},
        },
    })

    with pytest.raises(ResolveError, match="invalid cse.timezone"):
        resolve(cfg)
