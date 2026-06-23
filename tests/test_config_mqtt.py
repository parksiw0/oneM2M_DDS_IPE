"""cse.protocol(http|mqtt) 설정 검증/해석 테스트."""
from __future__ import annotations

import pytest

from ipe.config.loader import ConfigError, validate_config
from ipe.config.resolver import resolve

_QOS = {"qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}}}


def _http(**cse_over):
    cse = {"endpoint": "http://localhost:3000", "cse_base": "TinyIoT", "ae_name": "ros2-ipe"}
    cse.update(cse_over)
    return {"cse": cse, **_QOS}


def _mqtt(**cse_over):
    cse = {"protocol": "mqtt", "cse_base": "TinyIoT", "cse_id": "tinyiot",
           "ae_name": "ros2-ipe", "mqtt": {"host": "127.0.0.1", "port": 1883, "qos": 1}}
    cse.update(cse_over)
    return {"cse": cse, **_QOS}


def test_http_config_resolves_with_no_mqtt():
    rc = resolve(validate_config(_http()))
    assert rc.cse.protocol == "http"
    assert rc.cse.endpoint == "http://localhost:3000"
    assert rc.cse.mqtt is None


def test_http_missing_endpoint_rejected():
    cfg = {"cse": {"cse_base": "TinyIoT", "ae_name": "ros2-ipe"}, **_QOS}  # endpoint 누락
    with pytest.raises(ConfigError, match="endpoint is required"):
        validate_config(cfg)


def test_mqtt_config_resolves():
    rc = resolve(validate_config(_mqtt()))
    assert rc.cse.protocol == "mqtt"
    assert rc.cse.cse_id == "tinyiot"
    assert rc.cse.cse_base == "TinyIoT"
    assert rc.cse.endpoint == ""              # http 엔드포인트 불필요
    assert rc.cse.mqtt is not None
    assert rc.cse.mqtt.host == "127.0.0.1"
    assert rc.cse.mqtt.port == 1883
    assert rc.cse.mqtt.qos == 1
    assert rc.cse.mqtt.max_payload == 65536   # 기본값


def test_mqtt_missing_cse_id_rejected():
    cfg = _mqtt()
    del cfg["cse"]["cse_id"]
    with pytest.raises(ConfigError, match="cse_id is required"):
        validate_config(cfg)


def test_mqtt_defaults_applied_when_block_minimal():
    rc = resolve(validate_config(_mqtt(mqtt={})))
    assert rc.cse.mqtt.host == "127.0.0.1" and rc.cse.mqtt.port == 1883
    assert rc.cse.mqtt.qos == 1 and rc.cse.mqtt.clean_session is False
    assert rc.cse.mqtt.response_timeout_ms == 5000
