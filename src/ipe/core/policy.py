from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from ipe.core.normalize import normalize_ir
from ipe.core.payload import build_cin_content, build_fcnt_attrs, build_fcnt_update
from ipe.ir import TopicIR


@dataclass
class Op:
    kind: str            # "create_cin" | "update_fcnt"
    path: str            # oneM2M resource path
    content: dict[str, Any]
    topic: str


class SamplingGate:
    def __init__(self) -> None:
        self._last: dict[str, float] = {}

    def allow(self, key: str, interval: float, now: float) -> bool:
        last = self._last.get(key, 0.0)
        if now - last >= interval:
            self._last[key] = now
            return True
        return False


class Pipeline:
    def __init__(self, config: dict[str, Any], path_by_alias: dict[tuple[str, str], str]) -> None:
        self.config = config
        self.path_by_alias = path_by_alias
        self.topic_cfg: dict[str, dict[str, Any]] = {
            t["name"]: t for t in config.get("topics", [])
        }
        self.sampler = SamplingGate()

    def process(self, ir: TopicIR) -> list[Op]:
        topic = ir["interface_name"]
        cfg = self.topic_cfg.get(topic)
        if cfg is None:
            return []

        policy = cfg["representation_policy"]
        if policy == "sampled":
            interval = cfg.get("sampling", {}).get("interval_sec", 1.0)
            now = ir["timestamp"] if ir.get("timestamp") else time.time()
            if not self.sampler.allow(topic, interval, now):
                return []

        normalized = normalize_ir(ir, cfg.get("selected_fields"))
        path = self.path_by_alias[(cfg["semantic_category"], cfg["resource_alias"])]

        fc = cfg.get("flexcontainer")
        if fc:
            attrs = build_fcnt_attrs(
                normalized,
                fc["field_map"],
                cfg.get("frame_convention"),
            )
            return [Op("update_fcnt", path, build_fcnt_update(fc["type"], attrs), topic)]

        return [Op("create_cin", path, build_cin_content(normalized), topic)]
