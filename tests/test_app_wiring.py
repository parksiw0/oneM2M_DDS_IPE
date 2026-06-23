"""IPEApp 배선이 protocol에 따라 올바른 전송/리스너/POA를 구성하는지 (연결 없이)."""
from __future__ import annotations

import types

from ipe.config.loader import validate_config
from ipe.config.resolver import resolve
from ipe.onem2m.http_client import OneM2MHTTPClient
from ipe.onem2m.mqtt_client import OneM2MMQTTClient
from ipe.runtime.app import IPEApp

_QOS = {"qos_profiles": {"sensor_data": {"reliability": "best_effort", "depth": 5}}}


def _app(cfg, tmp_path):
    cfg = dict(cfg)
    cfg["storage"] = {"state_db": str(tmp_path / "s.db")}
    rc = resolve(validate_config(cfg))
    return IPEApp(rc, types.SimpleNamespace())


def test_http_wiring(tmp_path):
    app = _app({"cse": {"endpoint": "http://localhost:3000", "cse_base": "TinyIoT",
                        "ae_name": "ros2-ipe"}, **_QOS}, tmp_path)
    try:
        assert app.protocol == "http"
        assert app.poa.startswith("http://")
        assert isinstance(app.worker_client, OneM2MHTTPClient)
        assert isinstance(app.prov_client, OneM2MHTTPClient)
        assert app.provisioner.protocol == "http"
    finally:
        app.state.close()


def test_mqtt_wiring(tmp_path):
    app = _app({"cse": {"protocol": "mqtt", "cse_base": "TinyIoT", "cse_id": "tinyiot",
                        "ae_name": "ros2-ipe", "mqtt": {"host": "127.0.0.1", "port": 1883}},
                **_QOS}, tmp_path)
    try:
        assert app.protocol == "mqtt"
        assert app.poa == "mqtt://127.0.0.1:1883/ros2-ipe"
        assert app.poa_path == "ros2-ipe"
        assert isinstance(app.worker_client, OneM2MMQTTClient)
        assert isinstance(app.prov_client, OneM2MMQTTClient)
        assert app.provisioner.protocol == "mqtt"
        assert app.provisioner.poa_base == "mqtt://127.0.0.1:1883/ros2-ipe"
        assert app.worker_client.cse_id == "tinyiot"
    finally:
        app.state.close()
