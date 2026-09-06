"""QoS state publication and cache ownership on the ROS executor thread."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from ipe.core.pipeline import Op
from ipe.core.vocab import CLASS_OBSERVE_BULK, CLASS_TERMINAL

if TYPE_CHECKING:
    from ipe.runtime.bindings import BindingRegistry
    from ipe.runtime.outbound import OutboundProcessor

log = logging.getLogger(__name__)


class StatusPublisher:
    def __init__(self, registry: BindingRegistry, outbound: OutboundProcessor) -> None:
        self.registry = registry
        self.outbound = outbound
        self.adapter: Any = None
        self._qos_fcnt_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._qos_fcnt_last_pub: dict[tuple[str, str, str], float] = {}
        self._qos_fcnt_revision: dict[tuple[str, str, str], int] = {}
        self._qos_resource_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._qos_republish = threading.Event()

    def attach_adapter(self, adapter: Any) -> None:
        self.adapter = adapter

    def request_republish(self) -> None:
        """Thread-safe request; cache changes and ROS reads run on the executor."""
        self._qos_republish.set()

    def invalidate_qos(self, key: tuple[str, str, str]) -> None:
        self._qos_fcnt_cache.pop(key, None)
        self._qos_fcnt_last_pub.pop(key, None)
        self._qos_resource_cache.pop((key[0], key[1], "history"), None)

    def revision(self, key: tuple[str, str, str]) -> int:
        return self._qos_fcnt_revision.get(key, 0)

    def tick(self) -> None:
        self.adapter.tick()
        for key in self.adapter.pop_qos_dirty():
            self.adapter.refresh_qos(key)
            self.publish_qos(only_key=key)
        if self._qos_republish.is_set():
            self._qos_republish.clear()
            self._qos_fcnt_cache.clear()
            self._qos_fcnt_last_pub.clear()
            self._qos_resource_cache.clear()
            self.publish_qos()

    def _qos_mapping_targets(
        self,
        robot: str,
        iface: str,
        direction: str,
        applied: Any,
    ) -> dict[str, dict[str, Any]]:
        from ipe.qos.engine import topic_mapping
        paths = {view: self.registry.path_map[(robot, iface, view)]
                 for view in ("history", "latest", "fcnt", "command")
                 if (robot, iface, view) in self.registry.path_map}
        limit = self.registry.rc.policy.get("history_keep_all_limit", 1000)
        return topic_mapping(applied, direction, paths, limit)

    def _topic_data_refs(self, robot: str, iface: str, direction: str) -> list[dict[str, str]]:
        """Return the concrete oneM2M data resources managed by a topic QoS FCNT."""
        views = ("latest", "history", "fcnt") if direction == "observe" else ("command",)
        return [
            {"role": view, "path": self.registry.path_map[(robot, iface, view)]}
            for view in views
            if (robot, iface, view) in self.registry.path_map
        ]

    def _apply_qos_resource_mapping(
        self,
        robot: str,
        iface: str,
        direction: str,
        applied: Any,
    ) -> None:
        """Apply Category A attributes without treating the management FCNT as enforcement."""
        if direction != "observe" or applied.history != "KEEP_LAST":
            return
        path = self.registry.path_map.get((robot, iface, "history"))
        if path is None:
            return
        attrs = {"mni": applied.depth}
        key = (robot, iface, "history")
        if self._qos_resource_cache.get(key) == attrs:
            return
        self._qos_resource_cache[key] = attrs
        self.outbound.queue.put(
            Op("update_cnt", path, attrs, robot, iface, "historyQos", CLASS_TERMINAL),
            CLASS_TERMINAL,
        )

    def publish_qos(self, only_key: tuple[str, str] | None = None) -> None:
        """qos FCNT 총함수 게시 (QoS_FCNT_설계서 §4.5.2).

        어댑터의 QoS 상태 스냅숏을 방향별 전체 속성 레코드로 게시한다.
        캐시 비교(불변이면 생략)와 키별 최소 간격이 플래핑을 막고,
        lbl_compat면 기존 qos:* lbl 스킴을 포인터와 함께 병행 게시한다.
        """
        from ipe.qos.codec import spec_to_fcnt_attrs, spec_to_metadata

        qf = self.registry.rc.qos_fcnt
        smode = self.registry.rc.policy.get("qos_strictness", "reject")
        now = time.monotonic()
        for stt in self.adapter.qos_states():
            robot, iface, direction = stt["robot_id"], stt["interface"], stt["direction"]
            if only_key is not None and (robot, iface) != only_key:
                continue
            view = "qosObserve" if direction == "observe" else "qosCommand"
            fcnt_path = self.registry.path_map.get((robot, iface, view))
            applied = stt["applied"]
            if applied is not None:
                self._apply_qos_resource_mapping(robot, iface, direction, applied)

            # lbl 병행(Phase 1) — 기존 스킴 유지: observe 실효값 + qosResource 포인터
            if qf.lbl_compat and direction == "observe" and applied is not None:
                lpath = (
                    self.registry.path_map.get((robot, iface, "history"))
                    or self.registry.path_map.get((robot, iface, "latest"))
                    or self.registry.path_map.get((robot, iface, "fcnt"))
                )
                if lpath is not None:
                    labels = [f"qos:{k}={v}" for k, v in spec_to_metadata(applied).items()]
                    if fcnt_path:
                        labels.append(f"ipe:qosResource={fcnt_path}")
                    self.outbound.queue.put(
                        Op(
                            "update_lbl",
                            lpath,
                            {"labels": labels},
                            robot,
                            iface,
                            "qosmeta",
                            CLASS_OBSERVE_BULK,
                        ),
                        CLASS_OBSERVE_BULK,
                    )

            if not qf.enabled or fcnt_path is None or applied is None:
                continue  # 비활성/lbl-only/미바인딩 — FCNT 게시 없음
            spec = self.registry.specs_by_key.get(
                (direction, robot, iface)
            ) or self.registry.specs_by_key.get(("observe", robot, iface))
            rec = spec_to_fcnt_attrs(
                direction=direction,
                interface=iface,
                robot_id=robot,
                configured=stt["configured"],
                applied=applied,
                msg_type=getattr(spec, "msg_type", None),
                smode=smode if direction == "observe" else None,
                events=stt["events"],
                peers=stt["peers"][: qf.peers_max],
                peer_count=len(stt["peers"]),
                mapping_targets=self._qos_mapping_targets(robot, iface, direction, applied),
                data_resource_refs=self._topic_data_refs(robot, iface, direction),
                policy_source=(
                    "DEFAULT"
                    if any(
                        event in {"noPublisherFallback", "noSubscriberFallback"}
                        for event in stt["events"]
                    )
                    else "ROS2_RMW"
                ),
            )
            ckey = (robot, iface, direction)
            previous = self._qos_fcnt_cache.get(ckey)
            previous_body = (
                {key: value for key, value in previous.items() if key != "rev"}
                if previous
                else None
            )
            if previous_body == rec:
                continue
            last = self._qos_fcnt_last_pub.get(ckey)
            if last is not None and now - last < qf.publish_min_interval_ms / 1000.0:
                continue  # 캐시 미갱신 — 다음 트리거가 재시도한다
            revision = self._qos_fcnt_revision.get(ckey, 0) + 1
            self._qos_fcnt_revision[ckey] = revision
            rec["rev"] = revision
            self._qos_fcnt_cache[ckey] = rec
            self._qos_fcnt_last_pub[ckey] = now
            self.outbound.queue.put(
                Op("update_fcnt", fcnt_path, {qf.type: rec}, robot, iface, view, CLASS_TERMINAL),
                CLASS_TERMINAL,
            )
