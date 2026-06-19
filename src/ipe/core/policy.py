"""표현(representation) 기반 관측 파이프라인 (DESIGN §7.1, §9, §16.3, §17).

representation만이 Op 구성을 결정하고, 경로는 프로비저닝이 채운 path_map에서
가져온다 — 항목이 없으면 예외(조용한 드롭 없음). 필터·샘플러 게이트는
ingest_ts로만 클럭한다(§16.4) — 메시지 내장 타임스탬프는 절대 쓰지 않는다.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ipe.config.spec import TopicSpec
from ipe.core.anomaly import AnomalyGate
from ipe.core.common import MinIntervalGate
from ipe.core.filter import DeltaFilter, WindowAggregator
from ipe.core.normalize import normalize_ir
from ipe.core.payload import (
    build_cin_content,
    build_fcnt_attrs,
    build_fcnt_update,
    build_reference_content,
)
from ipe.ir import TopicIR

# 큐 클래스 문자열을 의도적으로 로컬에 중복 정의 — core가 runtime.queues에
# 의존하지 않게 하기 위함. 값은 runtime 쪽 클래스 이름과 일치해야 한다.
from ipe.core.vocab import (
    CLASS_OBSERVE_BULK as QUEUE_OBSERVE_BULK,
    CLASS_OBSERVE_LATEST as QUEUE_OBSERVE_LATEST,
    CLASS_TERMINAL as QUEUE_TERMINAL,
)

# representation별로 path_map에 있어야 하는 뷰 — 프로비저닝과의 계약.
VIEWS_BY_REPRESENTATION: dict[str, tuple[str, ...]] = {
    "historical": ("history",),
    "latest": ("latest",),
    "both": ("history", "latest"),
    "sampled": ("history",),
}

# tinyIoT는 크기 초과 요청을 응답 없이 버린다 — 64KiB에서 여유분을 뺀 값.
DEFAULT_LARGE_PAYLOAD_BYTES = 49152


@dataclass
class Op:
    """파이프라인이 만드는 oneM2M 쓰기 연산 1건."""

    kind: str                    # "create_cin"
    path: str                    # oneM2M 절대 리소스 경로
    content: dict[str, Any]      # con 본문 dict (봉투는 oneM2M 계층이 씌운다)
    robot_id: str
    interface: str
    view: str                    # "history" | "latest" | "fcnt" | 게시물 종류
    queue_class: str             # QUEUE_OBSERVE_LATEST | QUEUE_OBSERVE_BULK
    oversized: bool = False      # 참조 콘텐츠로 강등됐으면 True
    rn: str | None = None        # 결정적 resourceName(멱등 게시) — 없으면 CSE 생성
    anomalous: bool = False      # escalate된 이상값 — 호출자가 이벤트를 낸다(§7.4)


SamplingGate = MinIntervalGate   # 샘플링 = 키별 최소 간격 게이트의 별칭


def _state_key(robot_id: str, interface: str) -> str:
    # core/filter는 상태를 str 키 dict에 보관한다. \x00은 두 성분 어느 쪽에도
    # 들어갈 수 없으므로 이 합성 키는 로봇 간에 단사로 유지된다.
    return f"{robot_id}\x00{interface}"


class Pipeline:
    def __init__(
        self,
        topics: Iterable[TopicSpec],
        path_map: dict[tuple[str, str, str], str],
        large_payload_bytes: int = DEFAULT_LARGE_PAYLOAD_BYTES,
    ) -> None:
        self._specs: dict[tuple[str, str], TopicSpec] = {
            (t.robot_id, t.interface): t for t in topics
        }
        self.path_map = path_map
        self.large_payload_bytes = large_payload_bytes
        self.sampler = SamplingGate()
        self.delta = DeltaFilter()
        self.window = WindowAggregator()
        self.anomaly = AnomalyGate()

    def add_spec(self, spec: TopicSpec) -> None:
        """디스커버리로 늦게 합류한 토픽 등록 — executor 스레드에서만 호출."""
        self._specs[(spec.robot_id, spec.interface)] = spec

    def process(self, ir: TopicIR) -> list[Op]:
        key = (ir["robot_id"], ir["interface_name"])
        spec = self._specs.get(key)
        if spec is None:
            return []

        now = ir["ingest_ts"]

        # 이상감지는 샘플링보다 먼저 — 샘플링이 먼저면 이상값이 게이트에서 죽는다(§7.4)
        flt = spec.filter or {}
        is_anomaly_filter = flt.get("type") == "anomaly"
        anomalous, a_score = False, 0.0
        a_mode = flt.get("anomaly_mode", "escalate")
        if is_anomaly_filter:
            anomalous, a_score = self.anomaly.evaluate(
                _state_key(spec.robot_id, spec.interface), flt, ir["payload"])
            if anomalous and a_mode == "suppress":
                self.anomaly.note_suppressed(_state_key(spec.robot_id, spec.interface))
                return []

        escalated = anomalous and a_mode == "escalate"
        if spec.representation == "sampled" and not escalated:
            interval = spec.sample.interval_sec if spec.sample else 0.0
            if not self.sampler.allow(key, interval, now):
                return []

        normalized = normalize_ir(ir, spec.selected_fields)

        if spec.filter and not is_anomaly_filter and \
                not self._apply_filter(spec, normalized, now):
            return []

        content = build_cin_content(normalized, ir)
        if is_anomaly_filter and (anomalous or a_mode == "tag"):
            content["anomaly"] = {"detector": flt.get("detector", "isolation_forest"),
                                  "score": round(a_score, 4), "isAnomaly": anomalous}
        content, oversized = self._guard_size(content)

        ops: list[Op] = []
        for view in VIEWS_BY_REPRESENTATION[spec.representation]:
            # latest 의미를 FCNT가 맡는 배치(프로비저닝이 fcnt 경로를 등록한 경우)
            if view == "latest" and spec.flexcontainer and \
                    (spec.robot_id, spec.interface, "fcnt") in self.path_map:
                attrs = build_fcnt_attrs(normalized, spec.flexcontainer["field_map"])
                if attrs:
                    ops.append(Op(
                        kind="update_fcnt",
                        path=self.path_map[(spec.robot_id, spec.interface, "fcnt")],
                        content=build_fcnt_update(spec.flexcontainer["type"], attrs),
                        robot_id=spec.robot_id,
                        interface=spec.interface,
                        view="fcnt",
                        queue_class=QUEUE_TERMINAL if escalated else QUEUE_OBSERVE_LATEST,
                        oversized=oversized,
                        anomalous=escalated,
                    ))
                continue
            path = self.path_map[(spec.robot_id, spec.interface, view)]
            queue_class = (
                QUEUE_OBSERVE_LATEST if view == "latest" else QUEUE_OBSERVE_BULK
            )
            if escalated:
                queue_class = QUEUE_TERMINAL   # 관측 백로그 추월 + 드롭 금지(§7.4)
            ops.append(
                Op(
                    kind="create_cin",
                    path=path,
                    content=content,
                    robot_id=spec.robot_id,
                    interface=spec.interface,
                    view=view,
                    queue_class=queue_class,
                    oversized=oversized,
                    anomalous=escalated,
                )
            )
        return ops

    def _apply_filter(
        self, spec: TopicSpec, normalized: dict[str, Any], now: float
    ) -> bool:
        """델타/윈도우 필터 적용 — normalized["fields"]를 제자리에서 바꿀 수 있다.

        메시지를 보류하면 False.
        """
        flt = spec.filter or {}
        skey = _state_key(spec.robot_id, spec.interface)
        ftype = flt.get("type")
        if ftype == "delta":
            max_interval_ms = flt.get("max_interval_ms")
            return self.delta.allow(
                skey,
                normalized["fields"],
                flt.get("fields"),
                flt["min_change"],
                max_interval_ms / 1000.0 if max_interval_ms else None,
                now,
            )
        if ftype == "window":
            agg = self.window.add(
                skey,
                normalized["fields"],
                flt.get("mode", "count"),
                flt["size"],
                flt["aggregations"],
                flt.get("fields"),
                now,
            )
            if agg is None:
                return False
            normalized["fields"] = agg
            return True
        return True

    def _guard_size(self, content: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """직렬화된 con 크기 검사 — 초과 시 참조 콘텐츠로 강등."""
        size = len(json.dumps(content, ensure_ascii=False).encode("utf-8"))
        if size <= self.large_payload_bytes:
            return content, False
        reference = build_reference_content(
            {
                "mime": "application/json",
                "size_bytes": size,
                "seq": content["seq"],
                "source_ts": content["source_ts"],
                "note": (
                    f"payload exceeds large_payload_bytes "
                    f"({size} > {self.large_payload_bytes}); body not transferred"
                ),
            }
        )
        return reference, True
