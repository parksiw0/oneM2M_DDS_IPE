from __future__ import annotations

from typing import Any


class LookupTables:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

        self.topic_by_name: dict[str, dict[str, Any]] = {
            t["name"]: t for t in config.get("topics", [])
        }

        ae_base = f"/{config['cse']['cse_base']}/{config['cse']['ae_name']}"
        self.path_by_alias: dict[tuple[str, str], str] = {}
        for t in config.get("topics", []):
            key = (t["semantic_category"], t["resource_alias"])
            self.path_by_alias[key] = (
                f"{ae_base}/ros2Data/{t['semantic_category']}/{t['resource_alias']}"
            )

        self.qos_profiles: dict[str, dict[str, Any]] = config["qos_profiles"]

    def get_topic_config(self, name: str) -> dict[str, Any] | None:
        return self.topic_by_name.get(name)

    def get_resource_path(self, category: str, alias: str) -> str | None:
        return self.path_by_alias.get((category, alias))

    def get_qos_profile(self, name: str) -> dict[str, Any] | None:
        return self.qos_profiles.get(name)

    def __repr__(self) -> str:
        return (
            f"LookupTables(topics={len(self.topic_by_name)}, "
            f"paths={len(self.path_by_alias)}, "
            f"qos_profiles={len(self.qos_profiles)})"
        )


def build_lookup_tables(config: dict[str, Any]) -> LookupTables:
    return LookupTables(config)
