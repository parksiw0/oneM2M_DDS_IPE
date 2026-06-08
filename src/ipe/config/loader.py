from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from cerberus import Validator

from ipe.config.schema import CONFIG_SCHEMA


class ConfigError(Exception):
    pass


def load_config(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError(
            f"Config root must be a mapping, got {type(raw).__name__}"
        )

    return validate_config(raw)


def validate_config(raw: dict[str, Any]) -> dict[str, Any]:
    v = Validator(CONFIG_SCHEMA, purge_unknown=False)
    if not v.validate(raw):
        raise ConfigError(f"Schema validation failed: {v.errors}")

    normalized = v.normalized(raw)
    _check_semantics(normalized)
    return normalized


def _check_semantics(config: dict[str, Any]) -> None:
    qos_names = set(config["qos_profiles"].keys())

    for topic in config.get("topics", []):
        if topic["qos_profile"] not in qos_names:
            raise ConfigError(
                f"Topic '{topic['name']}' references undefined "
                f"qos_profile '{topic['qos_profile']}'. "
                f"Defined: {sorted(qos_names)}"
            )

        if topic["representation_policy"] == "sampled":
            if "sampling" not in topic:
                raise ConfigError(
                    f"Topic '{topic['name']}' has policy 'sampled' "
                    f"but no 'sampling' block."
                )

    seen: dict[tuple[str, str], str] = {}
    for topic in config.get("topics", []):
        key = (topic["semantic_category"], topic["resource_alias"])
        if key in seen:
            raise ConfigError(
                f"Duplicate resource_alias '{topic['resource_alias']}' "
                f"in category '{topic['semantic_category']}': "
                f"used by '{seen[key]}' and '{topic['name']}'"
            )
        seen[key] = topic["name"]
