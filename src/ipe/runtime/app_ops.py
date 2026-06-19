"""IPEApp 분해 mixin — 상태는 전부 IPEApp.__init__이 소유한다.

각 mixin은 self의 구성요소(state/queues/adapter/path_map/...)를 공유하는
같은 객체의 단면이다. 단독 인스턴스화 금지.
"""

from __future__ import annotations

import logging
import time
from typing import Any


from ipe.core.policy import Op
from ipe.core.vocab import CLASS_OBSERVE_BULK, CLASS_TERMINAL

log = logging.getLogger(__name__)

SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2}

class OpsMixin:
    def _tick_1s(self) -> None:
        self.adapter.tick()
        now = time.time()
        for corr in self.svc_tx.sweep_timeouts(now):
            self.emit_event("serviceStatus", "warning",
                            {"event": "timeout", "requestId": corr})
        for corr in self.act_tx.sweep_timeouts(now):
            self.emit_event("actionStatus", "warning",
                            {"event": "timeout", "goalId": corr})

    def _heartbeat(self) -> None:
        self.emit_event("ipeHealth", "info",
                        {"event": "heartbeat",
                         "inbound": self.inbound.depths(),
                         "outbound": self.outbound.depths(),
                         "dropped": self.outbound.dropped_counters(),
                         "spool": self.state.spool_counts()})

    def _discovery_refresh(self) -> None:
        try:
            self.adapter.rebind_changed()   # offered 변화 → QoS 재계산/재구독 (§8.2)
            snap = self.adapter.snapshot()
        except Exception as e:
            log.warning("discovery snapshot failed: %s", e)
            return
        self._prov_jobs.put(("reconcile_discovery", snap))

    # ------------------------------------------------------------------
    # 아웃바운드 워커 — 유일한 CSE 쓰기 주체
    # ------------------------------------------------------------------

    @staticmethod
    def _safe(name: str) -> str:
        import re as _re
        return _re.sub(r"[^A-Za-z0-9_-]", "_", name).strip("_")

    def _publish_contracts(self) -> None:
        for key, spec in list(self.specs_by_key.items()):
            self._publish_contract_for(key, spec)
        self._publish_qos_labels()

    def _publish_contract_for(self, key: tuple[str, str, str], spec: Any) -> None:
        kind, robot, iface = key
        ae = f"/{self.rc.cse.cse_base}/{self.rc.cse.ae_name}"
        # 입력 계약 예시 — 외부 앱이 호출 형식을 참조한다(§3.3)
        if kind in ("command", "service", "action"):
            try:
                example = self._input_example(kind, spec)
            except Exception as e:
                log.debug("input example skipped for %s: %s", iface, e)
                example = None
            if example is not None:
                rn = f"ie_{kind}_{self._safe(robot)}_{self._safe(iface)}"[:60]
                self._put_terminal(Op(
                    "create_cin", f"{ae}/config/input_example",
                    {"kind": kind, "robot": robot, "interface": iface,
                     "type": getattr(spec, "msg_type", None)
                             or getattr(spec, "srv_type", None)
                             or getattr(spec, "action_type", None),
                     "example": example},
                    robot, iface, "input_example", CLASS_TERMINAL, rn=rn))
        # confirm: required → 제안 게시 + 보류 등록(§5.4)
        if kind in ("command", "service", "action") and spec.confirm == "required":
            proposal_id = f"{self._safe(robot)}_{self._safe(iface)}"
            self._confirm_pending[proposal_id] = key
            self._put_terminal(Op(
                "create_cin", f"{ae}/config/pendingMappingProposal",
                {"proposalId": proposal_id, "kind": kind, "robot": robot,
                 "interface": iface, "reason": "confirm: required"},
                robot, iface, "proposal", CLASS_TERMINAL, rn=f"pmp_{proposal_id}"[:60]))

    def _input_example(self, kind: str, spec: Any) -> dict[str, Any] | None:
        from rosidl_runtime_py.utilities import get_action, get_message, get_service

        from ipe.core.transcode import make_input_example
        if kind == "command" and spec.msg_type:
            return make_input_example(get_message(spec.msg_type))
        if kind == "service" and spec.srv_type:
            return make_input_example(get_service(spec.srv_type).Request)
        if kind == "action" and spec.action_type:
            return make_input_example(get_action(spec.action_type).Goal)
        return None

    def _publish_qos_labels(self) -> None:
        """resolved QoS를 관측 CNT lbl로 기록(§8.6) — 소비 AE 참고용, 비강제."""
        from ipe.core.qos import spec_to_metadata
        for key, st in getattr(self.adapter, "observes", {}).items():
            robot, iface = key
            path = (self.path_map.get((robot, iface, "history"))
                    or self.path_map.get((robot, iface, "latest")))
            if path is None or st.applied_qos is None:
                continue
            labels = [f"qos:{k}={v}" for k, v in spec_to_metadata(st.applied_qos).items()]
            self.outbound.put(Op("update_lbl", path, {"labels": labels},
                                 robot, iface, "qosmeta", CLASS_OBSERVE_BULK),
                              CLASS_OBSERVE_BULK)

    # ------------------------------------------------------------------
    # churn 상태기계 (§4.6) — 프로비저닝 워커 스레드에서 실행
    # ------------------------------------------------------------------

    def _churn_track(self, snap: dict[str, Any]) -> None:
        names = {n for n, _ in snap.get("topics", [])} \
            | {n for n, _ in snap.get("services", [])} \
            | {n for n, _ in snap.get("actions", [])}
        grace = int(self.rc.discovery.get("vanish_grace_polls", 2) or 2)
        for key in list(self.specs_by_key.keys()):
            kind, robot, iface = key
            st = self._avail.setdefault(key, {"state": "present", "miss": 0})
            if iface in names:
                if st["state"] != "present":
                    st.update(state="present", miss=0)
                    self._mark_avail(robot, iface, True)
                    self.emit_event("nodeStatus", "info",
                                    {"event": "rejoined", "interface": iface, "robot": robot})
                else:
                    st["miss"] = 0
                continue
            st["miss"] += 1
            if st["miss"] < grace:
                if st["state"] == "present":
                    st["state"] = "suspect"
                    self.emit_event("nodeStatus", "warning",
                                    {"event": "suspect", "interface": iface, "robot": robot})
            elif st["state"] != "vanished":
                st["state"] = "vanished"
                self._mark_avail(robot, iface, False)
                self.emit_event("nodeStatus", "warning",
                                {"event": "vanished", "interface": iface, "robot": robot})
                self._terminate_inflight(robot, iface)

    def _mark_avail(self, robot: str, iface: str, available: bool) -> None:
        path = None
        for view in ("history", "latest", "fcnt", "publishStatus", "response", "result"):
            path = self.path_map.get((robot, iface, view))
            if path:
                break
        if path is None:
            return
        try:
            self.prov_ops.update_lbl(
                path, [f"ipe:available={'true' if available else 'false'}",
                       f"ipe:lastSeen={time.time():.0f}"])
        except Exception as e:
            log.warning("availability lbl update failed for %s: %s", path, e)

    def _terminate_inflight(self, robot: str, iface: str) -> None:
        """소멸 확정된 인터페이스의 비종결 트랜잭션을 종결한다 — 무음 대기 금지."""
        now = time.time()
        for corr in list(self._inflight.get((robot, iface), set())):
            tx = self.state.get_transaction(corr)
            if tx is None:
                continue
            if tx["kind"] == "action" and not self.act_tx.is_terminal(tx["state"]):
                self.act_tx.set_state(corr, "serverUnavailable", now)
                self.emit_event("actionStatus", "warning",
                                {"event": "serverUnavailable", "goalId": corr,
                                 "interface": iface, "robot": robot})
            elif tx["kind"] == "service" and not self.svc_tx.is_terminal(tx["state"]):
                self.svc_tx.set_state(corr, "failed", now)
                self.emit_event("serviceStatus", "warning",
                                {"event": "serverUnavailable", "requestId": corr,
                                 "interface": iface, "robot": robot})
            self._finish(robot, iface, corr, "failed", now)

    def _diag(self) -> dict[str, Any]:
        return {
            "aei": self.aei,
            "bound": {f"{k[0]}:{k[1]}:{k[2]}": True for k in self.specs_by_key},
            "routes": len(self.routes),
            "inbound": self.inbound.depths(),
            "outbound": self.outbound.depths(),
            "dropped": self.outbound.dropped_counters(),
            "spool": self.state.spool_counts(),
            "muted": [f"{r}:{i}" for r, i in self._muted_pipeline],
            "anomaly_suppressed": dict(getattr(self.pipeline, "anomaly", None).suppressed
                                       if self.pipeline else {}),
            "budget_dropped": self._budget_dropped,
            "pending_confirm": dict(self._confirm_pending),
            "availability": {f"{k[1]}:{k[2]}": v["state"] for k, v in self._avail.items()},
        }

    # ------------------------------------------------------------------
    # 상태 이벤트
    # ------------------------------------------------------------------

    def emit_event(self, category: str, severity: str, payload: dict[str, Any]) -> None:
        min_sev = self.rc.logging.get("status_severity_min", "info")
        if SEVERITY_ORDER.get(severity, 0) < SEVERITY_ORDER.get(min_sev, 0):
            return
        body = {"severity": severity, "ts": time.time(), **payload}
        level = {"info": logging.INFO, "warning": logging.WARNING,
                 "error": logging.ERROR}.get(severity, logging.WARNING)
        log.log(level, "[%s] %s", category, body)
        path = self.status_paths.get(category)
        if path is None:
            return
        self._put_terminal(Op("create_cin", path, body,
                              payload.get("robot", "-"), payload.get("interface", "-"),
                              category, CLASS_TERMINAL))

    def _put_terminal(self, op: Op) -> None:
        if not self.outbound.put(op, CLASS_TERMINAL):
            self._spool_op(op)

    # ------------------------------------------------------------------
    # 부팅 스윕 + 종료
    # ------------------------------------------------------------------

    def _boot_sweep(self) -> None:
        swept = self.state.sweep_boot(time.time())
        for row in swept.get("dispatched", []):
            self.emit_event("ipeHealth", "warning",
                            {"event": "outcomeUnknownAtRestart",
                             "interface": row["interface"], "robot": row["robot_id"],
                             "correlationId": row["corr_id"]})
        now = time.time()
        for t in self.state.active_transactions("action"):
            if not self.act_tx.is_terminal(t["state"]):
                self.act_tx.set_state(t["corr_id"], "orphanedAtRestart", now)
                self.emit_event("actionStatus", "warning",
                                {"event": "orphanedAtRestart", "goalId": t["corr_id"]})
        for t in self.state.active_transactions("service"):
            if not self.svc_tx.is_terminal(t["state"]):
                self.svc_tx.set_state(t["corr_id"], "failed", now)
                self.emit_event("serviceStatus", "warning",
                                {"event": "orphanedAtRestart", "requestId": t["corr_id"]})

