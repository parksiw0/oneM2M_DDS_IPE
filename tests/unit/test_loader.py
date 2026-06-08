from __future__ import annotations

import pytest

from ipe.config.loader import ConfigError, load_config, validate_config


class TestValidation:
    def test_minimal_valid_config_passes(self, minimal_config):
        result = validate_config(minimal_config)
        assert result["platform"]["name"] == "test"
        assert len(result["topics"]) == 1

    def test_missing_platform_fails(self, minimal_config):
        del minimal_config["platform"]
        with pytest.raises(ConfigError, match="platform"):
            validate_config(minimal_config)

    def test_invalid_semantic_category_fails(self, minimal_config):
        minimal_config["topics"][0]["semantic_category"] = "invalid"
        with pytest.raises(ConfigError):
            validate_config(minimal_config)

    def test_invalid_representation_policy_fails(self, minimal_config):
        minimal_config["topics"][0]["representation_policy"] = "invalid_policy"
        with pytest.raises(ConfigError):
            validate_config(minimal_config)

    def test_invalid_reliability_fails(self, minimal_config):
        minimal_config["qos_profiles"]["default"]["reliability"] = "INVALID"
        with pytest.raises(ConfigError):
            validate_config(minimal_config)

    def test_undefined_qos_profile_fails(self, minimal_config):
        minimal_config["topics"][0]["qos_profile"] = "nonexistent"
        with pytest.raises(ConfigError, match="undefined"):
            validate_config(minimal_config)

    def test_sampled_without_sampling_fails(self, minimal_config):
        minimal_config["topics"][0]["representation_policy"] = "sampled"
        with pytest.raises(ConfigError, match="sampling"):
            validate_config(minimal_config)

    def test_sampled_with_sampling_passes(self, minimal_config):
        minimal_config["topics"][0]["representation_policy"] = "sampled"
        minimal_config["topics"][0]["sampling"] = {"interval_sec": 0.2}
        result = validate_config(minimal_config)
        assert result["topics"][0]["sampling"]["interval_sec"] == 0.2

    def test_duplicate_alias_same_category_fails(self, minimal_config):
        minimal_config["topics"].append({
            "name": "/test/topic2",
            "message_type": "px4_msgs/msg/SensorCombined",
            "qos_profile": "default",
            "semantic_category": "sensors",
            "resource_alias": "test_imu",
            "representation_policy": "latest_only",
        })
        with pytest.raises(ConfigError, match="Duplicate"):
            validate_config(minimal_config)

    def test_duplicate_alias_different_category_passes(self, minimal_config):
        minimal_config["topics"].append({
            "name": "/test/topic2",
            "message_type": "px4_msgs/msg/SensorCombined",
            "qos_profile": "default",
            "semantic_category": "entities",
            "resource_alias": "test_imu",
            "representation_policy": "latest_only",
        })
        result = validate_config(minimal_config)
        assert len(result["topics"]) == 2

    def test_invalid_message_type_format_fails(self, minimal_config):
        minimal_config["topics"][0]["message_type"] = "InvalidFormat"
        with pytest.raises(ConfigError):
            validate_config(minimal_config)


class TestLoad:
    def test_load_px4_config(self, px4_config_path):
        config = load_config(px4_config_path)
        assert config["platform"]["name"] == "px4"
        assert config["qos_profiles"]["px4_default"]["reliability"] == "BEST_EFFORT"
        assert config["qos_profiles"]["px4_default"]["durability"] == "TRANSIENT_LOCAL"
        assert len(config["topics"]) == 5
        fcnt_topics = [t for t in config["topics"] if t.get("flexcontainer")]
        assert len(fcnt_topics) == 2

    def test_load_nonexistent_file_fails(self, tmp_path):
        with pytest.raises(ConfigError, match="not found"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_load_non_mapping_root_fails(self, tmp_path):
        bad = tmp_path / "list.yaml"
        bad.write_text("- this is\n- a list\n")
        with pytest.raises(ConfigError, match="mapping"):
            load_config(bad)
