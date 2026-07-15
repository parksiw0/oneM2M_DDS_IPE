"""IPEApp 분해 mixin — 상태는 전부 IPEApp.__init__이 소유한다.

각 mixin은 self의 구성요소(state/queues/adapter/path_map/...)를 공유하는
같은 객체의 단면이다. 단독 인스턴스화 금지.
"""

from __future__ import annotations

import logging
import time
from typing import Any


from ipe.config.spec import ActionSpec, ServiceSpec, TopicSpec
from ipe.core.common import deep_merge as _deep_merge, project_top_level as _project
from ipe.core.normalize import ct_to_epoch as _ct_to_epoch
from ipe.core.policy import Op
from ipe.core.vocab import CLASS_OBSERVE_BULK, CLASS_TERMINAL
from ipe.onem2m.notification import Notification
from ipe.runtime.dispatcher import InboundEvent

log = logging.getLogger(__name__)

GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}

class DispatchMixin:
    def _on_topic_ir(self, ir: Any) -> None:
        try:
            ops = self.pipeline.process(ir)
        except Exception as e:
            # 같은 인터페이스의 반복 실패는 1회만 보고 (이벤트 폭주 방지)
            key = (ir["robot_id"], ir["interface_name"])
            if key not in self._muted_pipeline:
                self._muted_pipeline.add(key)
                self.emit_event("topicHealth", "error",
                                {"event": "pipelineError", "interface": key[1],
                                 "robot": key[0], "error": str(e), "muted": True})
            return
        for op in ops:
            if op.oversized:
                self.emit_event("topicHealth", "warning",
                                {"event": "payloadOversize", "interface": op.interface,
                                 "robot": op.robot_id})
            if getattr(op, "anomalous", False):
                self._anomaly_event(op)
            if (op.queue_class == CLASS_OBSERVE_BULK and self._budget is not None
                    and not self._budget.allow()):
                self._budget_dropped += 1
                continue
            if not self.outbound.put(op, op.queue_class) and op.queue_class == CLASS_TERMINAL:
                self._spool_op(op)

    # ------------------------------------------------------------------
    # 인바운드 admission (리스너 스레드, 전역 락)
    # ------------------------------------------------------------------

    def _on_notify(self, path_key: str, notif: Notification) -> str:
        with self._admission_lock:
            return self._admit(path_key, notif)

    def _catchup_admit(self, path_key: str, cin_ri: str,
                       con: dict[str, Any] | None, ct: str | None) -> str:
        notif = Notification(vrq=False, sur=None, net=3, cr=None,
                             cin_ri=cin_ri, cin_ct=ct, con=con, raw={})
        with self._admission_lock:
            return self._admit(path_key, notif)

    def _admit(self, path_key: str, notif: Notification) -> str:
        ev = self.routes.route(path_key, notif)
        if ev is None:
            self.emit_event("ipeHealth", "warning",
                            {"event": "unknownRoute", "path_key": path_key})
            return "invalid"
        if notif.cr is not None and notif.cr == self.aei:
            return "denied"   # 알림 루프 방지 불변식 (cr == 자기 aei)
        # 결정(decision)은 같은 proposalId로 여러 번 온다(approve→revoke→…) —
        # 멱등 키는 CIN ri, proposalId는 페이로드다
        corr = (ev.event_id if ev.kind == "decision"
                else ev.correlation_id or ev.event_id) or ""
        ev.dedup_corr = corr   # 드레인의 CAS가 반드시 같은 키를 봐야 한다
        now = time.time()
        verdict = self.state.admit(ev.robot_id, ev.interface, corr,
                                   ev.event_id or "", now)
        if verdict == "duplicate":
            self.emit_event(self._status_category(ev.kind), "info",
                            {"event": "duplicate", "interface": ev.interface,
                             "robot": ev.robot_id, "correlationId": corr})
            return "duplicate"
        ev.ingest_monotonic = time.monotonic()
        ok = (self.inbound.put_control(ev) if ev.kind == "cancel"
              else self.inbound.put_normal(ev))
        if not ok:
            self.state.mark_overflow(ev.robot_id, ev.interface, corr, now)
            log.error("inbound overflow: %s (%s)", path_key, corr)
            return "overflow"
        self.catchup.mark_processed(path_key, ev.ct)
        if self.guard is not None:
            self.guard.trigger()
        return "ok"

    @staticmethod
    def _status_category(kind: str) -> str:
        return {"command": "commandStatus", "service": "serviceStatus",
                "action_goal": "actionStatus", "cancel": "actionStatus",
                "decision": "provisioningStatus",
                "qos_update": "qosStatus"}.get(kind, "ipeHealth")

    # ------------------------------------------------------------------
    # guard 드레인 (executor 스레드, 예산 제한)
    # ------------------------------------------------------------------

    def _drain_inbound(self) -> None:
        budget = int(self.rc.dispatch.get("drain_budget", 32))
        for ev in self.inbound.get_batch(budget):
            corr = ev.dedup_corr or ev.correlation_id or ev.event_id or ""
            try:
                self._dispatch_one(ev, corr)
            except Exception as e:
                log.exception("dispatch failed for %s/%s", ev.kind, ev.interface)
                self.emit_event(self._status_category(ev.kind), "error",
                                {"event": "dispatchError", "interface": ev.interface,
                                 "robot": ev.robot_id, "error": str(e),
                                 "correlationId": corr})
                self._finish(ev.robot_id, ev.interface, corr, "failed", time.time())
        if not self.inbound.empty() and self.guard is not None:
            self.guard.trigger()

    def _dispatch_one(self, ev: InboundEvent, corr: str) -> None:
        if ev.kind.startswith("_bind_"):
            self._bind_dynamic(ev)
            return
        if not self.state.cas_dispatch(ev.robot_id, ev.interface, corr, time.time()):
            return
        if ev.kind == "command":
            self._dispatch_command(ev, corr)
        elif ev.kind == "service":
            self._dispatch_service(ev, corr)
        elif ev.kind == "action_goal":
            self._dispatch_goal(ev, corr)
        elif ev.kind == "cancel":
            self._dispatch_cancel(ev, corr)
        elif ev.kind == "decision":
            self._dispatch_decision(ev, corr)
        elif ev.kind == "qos_update":
            self._dispatch_qos_update(ev, corr)
        else:
            self.emit_event("ipeHealth", "warning",
                            {"event": "unhandledKind", "kind": ev.kind})
            self._finish(ev.robot_id, ev.interface, corr, "rejected", time.time())

    def _bind_dynamic(self, ev: InboundEvent) -> None:
        spec = getattr(ev, "spec", None)
        if ev.kind == "_bind_service" and isinstance(spec, ServiceSpec):
            if self.adapter.bind_service(spec):
                key = ("service", spec.robot_id, spec.interface)
                self.specs_by_key[key] = spec
                self._publish_contract_for(key, spec)
            return
        if ev.kind == "_bind_action" and isinstance(spec, ActionSpec):
            if self.adapter.bind_action(spec):
                key = ("action", spec.robot_id, spec.interface)
                self.specs_by_key[key] = spec
                self._publish_contract_for(key, spec)
            return
        if isinstance(spec, TopicSpec):
            if spec.direction in ("observe", "both") and self.adapter.bind_observe(spec):
                self.specs_by_key[("observe", spec.robot_id, spec.interface)] = spec
                # Pipeline 스펙 사전은 기동 시점 스냅숏 — 늦게 합류한 토픽을
                # 등록하지 않으면 관측 IR이 조용히 버려진다
                self.pipeline.add_spec(spec)
            if (spec.direction in ("command", "both") and spec.access_enabled
                    and self.adapter.bind_command(spec)):
                key = ("command", spec.robot_id, spec.interface)
                self.specs_by_key[key] = spec
                self._publish_contract_for(key, spec)
            self._publish_qos_state(only_key=(spec.robot_id, spec.interface))

    # --- command ------------------------------------------------------

    def _publish_command(self, spec: TopicSpec, payload: dict[str, Any]) -> bool:
        return self.adapter.publish_command(spec, payload)

    def _dispatch_command(self, ev: InboundEvent, corr: str) -> None:
        spec = self.specs_by_key.get(("command", ev.robot_id, ev.interface))
        now = time.time()
        if spec is None:
            self.emit_event("commandStatus", "error",
                            {"event": "rejected", "reason": "notBound",
                             "interface": ev.interface, "robot": ev.robot_id,
                             "commandId": corr})
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        payload = dict(ev.payload or {})
        payload.pop("commandId", None)
        outcome = self.cmd_mgr.dispatch(spec, payload, _ct_to_epoch(ev.ct),
                                        getattr(ev, "ingest_monotonic", None)
                                        or time.monotonic())
        status_path = self.path_map.get((ev.robot_id, ev.interface, "publishStatus"))
        if status_path:
            self._put_terminal(Op("create_cin", status_path,
                                  {"commandId": corr, "status": outcome.status,
                                   "detail": outcome.detail, "clamped": outcome.clamped},
                                  ev.robot_id, ev.interface, "publishStatus",
                                  CLASS_TERMINAL))
        if not outcome.published:
            self.emit_event("commandStatus", "warning",
                            {"event": outcome.status, "interface": ev.interface,
                             "robot": ev.robot_id, "commandId": corr,
                             "detail": outcome.detail})
        terminal = {"published": "succeeded", "expired": "expired",
                    "accessDenied": "accessDenied"}.get(outcome.status, "rejected")
        self._finish(ev.robot_id, ev.interface, corr, terminal, time.time())

    # --- service ------------------------------------------------------

    def _dispatch_service(self, ev: InboundEvent, corr: str) -> None:
        spec: ServiceSpec | None = self.specs_by_key.get(
            ("service", ev.robot_id, ev.interface))
        now = time.time()
        if spec is None:
            self._service_event(ev, corr, "rejected", "notBound")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if spec.confirm == "required":
            self._service_event(ev, corr, "rejected", "pendingConfirmation")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if self.svc_tx.begin(corr, now, timeout_ms=spec.timeout_ms) == "duplicate":
            self._service_event(ev, corr, "duplicate", "")
            return
        self._inflight.setdefault((ev.robot_id, ev.interface), set()).add(corr)
        if not self.adapter.server_available("service", (ev.robot_id, ev.interface)):
            self.svc_tx.set_state(corr, "failed", now)
            self._service_event(ev, corr, "failed", "serverUnavailable")
            self._finish(ev.robot_id, ev.interface, corr, "failed", now)
            return
        payload = dict(ev.payload or {})
        payload.pop("requestId", None)
        merged = (_deep_merge(dict(spec.request_template), payload)
                  if spec.request_template else payload)
        self.svc_tx.set_state(corr, "accepted", now)

        def done(resp: dict[str, Any] | None, err: str | None,
                 _ev: InboundEvent = ev, _corr: str = corr) -> None:
            # executor Task 컨텍스트: 상태 기록 + enqueue만 허용
            t = time.time()
            if err is not None:
                self.svc_tx.set_state(_corr, "failed", t)
                self._service_event(_ev, _corr, "failed", err)
                self._finish(_ev.robot_id, _ev.interface, _corr, "failed", t)
                return
            self.svc_tx.set_state(_corr, "responded", t)
            resp_path = self.path_map.get((_ev.robot_id, _ev.interface, "response"))
            if resp_path:
                if spec.response_fields:
                    resp = _project(resp or {}, spec.response_fields)
                self._put_terminal(Op("create_cin", resp_path,
                                      {"requestId": _corr, "response": resp},
                                      _ev.robot_id, _ev.interface, "response",
                                      CLASS_TERMINAL))
            self._service_event(_ev, _corr, "responded", "")
            self._finish(_ev.robot_id, _ev.interface, _corr, "succeeded", t)

        try:
            sent = self.adapter.call_service(spec, merged, done)
            err = None if sent else "callFailed"
        except Exception as e:
            sent, err = False, str(e)
        if not sent:
            self.svc_tx.set_state(corr, "rejected", time.time())
            self._service_event(ev, corr, "rejected", err or "")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", time.time())
            return
        self.svc_tx.set_state(corr, "invoked", time.time())

    def _service_event(self, ev: InboundEvent, corr: str, status: str, detail: str) -> None:
        path = self.path_map.get((ev.robot_id, ev.interface, "invocationStatus"))
        if path:
            self._put_terminal(Op("create_cin", path,
                                  {"requestId": corr, "status": status, "detail": detail},
                                  ev.robot_id, ev.interface, "invocationStatus",
                                  CLASS_TERMINAL))
        if status in ("timeout", "rejected", "failed"):
            self.emit_event("serviceStatus", "warning",
                            {"event": status, "interface": ev.interface,
                             "robot": ev.robot_id, "requestId": corr, "detail": detail})

    # --- action -------------------------------------------------------

    def _dispatch_goal(self, ev: InboundEvent, corr: str) -> None:
        spec: ActionSpec | None = self.specs_by_key.get(
            ("action", ev.robot_id, ev.interface))
        now = time.time()
        if spec is None:
            self._action_event(ev, corr, 0, "goalRejected", "notBound")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if spec.confirm == "required":
            self._action_event(ev, corr, 0, "goalRejected", "pendingConfirmation")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if self.act_tx.begin(corr, now, timeout_ms=spec.timeout_ms) == "duplicate":
            self._action_event(ev, corr, 0, "duplicateGoal", "")
            return
        self._inflight.setdefault((ev.robot_id, ev.interface), set()).add(corr)
        if not self.adapter.server_available("action", (ev.robot_id, ev.interface)):
            self.act_tx.set_state(corr, "serverUnavailable", now)
            self._action_event(ev, corr, 0, "serverUnavailable", "")
            self._finish(ev.robot_id, ev.interface, corr, "failed", now)
            return
        payload = dict(ev.payload or {})
        payload.pop("goalId", None)
        goal = (_deep_merge(dict(spec.goal_template), payload)
                if spec.goal_template else payload)
        if spec.goal_fields:
            goal = _project(goal, spec.goal_fields, strict=True)

        fb_interval = spec.feedback_sample.interval_sec if spec.feedback_sample else 0.0
        fb_last = {"t": 0.0}

        def on_goal_response(goal_id: str, accepted: bool) -> None:
            t = time.time()
            if accepted:
                self.act_tx.set_state(goal_id, "goalAccepted", t)
                self._action_event(ev, goal_id, 2, None, "accepted")
            else:
                self.act_tx.set_state(goal_id, "goalRejected", t)
                self._action_event(ev, goal_id, 0, "goalRejected", "")
                self._finish(ev.robot_id, ev.interface, goal_id, "rejected", t)

        def on_feedback(goal_id: str, fb: dict[str, Any]) -> None:
            if spec.feedback != "log" and fb_interval:
                now_m = time.monotonic()
                if now_m - fb_last["t"] < fb_interval:
                    return   # 샘플링은 유일하게 허용된 feedback 드롭
                fb_last["t"] = now_m
            seq = self.act_tx.next_feedback_seq(goal_id, time.time())
            path = self.path_map.get((ev.robot_id, ev.interface, "feedback"))
            if path:
                if spec.feedback_fields:
                    fb = _project(fb, spec.feedback_fields)
                self.outbound.put(Op("create_cin", path,
                                     {"goalId": goal_id, "feedbackSeq": seq,
                                      "feedback": fb},
                                     ev.robot_id, ev.interface, "feedback",
                                     CLASS_OBSERVE_BULK), CLASS_OBSERVE_BULK)

        def on_result(goal_id: str, status_int: int, result: dict[str, Any]) -> None:
            t = time.time()
            self.act_tx.set_state(goal_id, "resultReceived", t)
            reason = GOAL_STATUS_TO_REASON.get(status_int, "failed")
            path = self.path_map.get((ev.robot_id, ev.interface, "result"))
            if path:
                if spec.result_fields:
                    result = _project(result, spec.result_fields)
                self._put_terminal(Op("create_cin", path,
                                      {"goalId": goal_id, "goalStatus": status_int,
                                       "terminationReason": reason, "result": result},
                                      ev.robot_id, ev.interface, "result",
                                      CLASS_TERMINAL))
            self._action_event(ev, goal_id, status_int, reason, "")
            self._finish(ev.robot_id, ev.interface, goal_id, "succeeded", t)

        try:
            sent = self.adapter.send_goal(spec, corr, goal, on_goal_response,
                                          on_feedback, on_result)
        except Exception as e:
            from ipe.core.transcode import TranscodeError
            reason = "goalRejected" if isinstance(e, TranscodeError) else "failed"
            terminal = "rejected" if isinstance(e, TranscodeError) else "failed"
            self.act_tx.set_state(corr, "goalRejected" if terminal == "rejected" else "failed",
                                  time.time())
            self._action_event(ev, corr, 0, reason, str(e))
            self._finish(ev.robot_id, ev.interface, corr, terminal, time.time())
            return
        if not sent:
            self.act_tx.set_state(corr, "failed", time.time())
            self._action_event(ev, corr, 0, "failed", "sendFailed")
            self._finish(ev.robot_id, ev.interface, corr, "failed", time.time())
            return
        self.act_tx.set_state(corr, "goalSent", time.time())

    def _dispatch_cancel(self, ev: InboundEvent, corr: str) -> None:
        spec: ActionSpec | None = self.specs_by_key.get(
            ("action", ev.robot_id, ev.interface))
        goal_id = (ev.payload or {}).get("goalId") or corr
        now = time.time()
        if spec is None:
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        verdict = self.adapter.cancel_goal(spec, goal_id)
        if verdict == "unknown":
            self._action_event(ev, goal_id, 0, "cancelRejected", "unknownGoal")
        else:
            self.act_tx.set_state(goal_id, "canceling", now)
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    def _action_event(self, ev: InboundEvent, goal_id: str, status_int: int,
                      reason: str | None, detail: str) -> None:
        path = self.path_map.get((ev.robot_id, ev.interface, "actionStatus"))
        if path:
            self._put_terminal(Op("create_cin", path,
                                  {"goalId": goal_id, "goalStatus": status_int,
                                   "terminationReason": reason, "detail": detail},
                                  ev.robot_id, ev.interface, "actionStatus",
                                  CLASS_TERMINAL))

    # --- qos_update (QoS_FCNT_설계서 §4.5.3) ---------------------------

    _QOS_CF_ENUMS = {
        "cfRlb": ("reliability", ("RELIABLE", "BEST_EFFORT")),
        "cfDrb": ("durability", ("VOLATILE", "TRANSIENT_LOCAL")),
        "cfHst": ("history", ("KEEP_LAST", "KEEP_ALL")),
        "cfLiv": ("liveliness", ("AUTOMATIC", "MANUAL_BY_TOPIC")),
    }
    _QOS_CF_DURS = {"cfDdl": "deadline_ms", "cfLsp": "lifespan_ms",
                    "cfLse": "liveliness_lease_duration_ms"}

    def _parse_cf_update(self, payload: dict[str, Any],
                         base: Any) -> tuple[Any | None, str]:
        """NOTIFY rep의 cf* → 후보 QoSSpec. (None, 사유) = 도메인 위반."""
        from dataclasses import replace
        updates: dict[str, Any] = {}
        for sn, (fld, allowed) in self._QOS_CF_ENUMS.items():
            if sn not in payload:
                continue
            v = str(payload[sn]).upper()
            if v not in allowed:
                return None, f"{sn}: '{payload[sn]}' not in {allowed}"
            updates[fld] = v
        if "cfDpt" in payload:
            d = payload["cfDpt"]
            if not isinstance(d, int) or isinstance(d, bool) or d < 1:
                return None, f"cfDpt: expected integer >= 1, got {d!r}"
            updates["depth"] = d
        for sn, fld in self._QOS_CF_DURS.items():
            if sn not in payload:
                continue
            v = payload[sn]
            if isinstance(v, str) and v.upper() == "INF":
                updates[fld] = None
            elif isinstance(v, int) and not isinstance(v, bool) and v >= 0:
                updates[fld] = v
            elif isinstance(v, str) and v.isdigit():
                updates[fld] = int(v)
            else:
                return None, f"{sn}: expected 'INF' or decimal ms, got {v!r}"
        cand = replace(base, **updates)
        if cand.liveliness == "MANUAL_BY_TOPIC" \
                and cand.liveliness_lease_duration_ms is None:
            return None, "liveliness MANUAL_BY_TOPIC requires cfLse (B8)"
        return cand, ""

    def _dispatch_qos_update(self, ev: InboundEvent, corr: str) -> None:
        from ipe.config.rules import command_qos_violation
        now = time.time()
        direction = (ev.meta or {}).get("direction", "observe")
        key = (ev.robot_id, ev.interface)
        spec_key = ("command" if direction == "command" else "observe",
                    ev.robot_id, ev.interface)
        spec = self.specs_by_key.get(spec_key)
        payload = ev.payload or {}

        def _reject(reason: str) -> None:
            self.emit_event("qosStatus", "warning",
                            {"event": "qosUpdateRejected", "interface": ev.interface,
                             "robot": ev.robot_id, "direction": direction,
                             "reason": reason})
            # 원복: 직전 정본 레코드 재게시로 CSE의 cf*를 되돌린다
            self._qos_fcnt_cache.pop((ev.robot_id, ev.interface, direction), None)
            self._qos_fcnt_last_pub.pop((ev.robot_id, ev.interface, direction), None)
            self._publish_qos_state(only_key=key)
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)

        if not self.rc.qos_fcnt.allow_update or spec is None:
            _reject("qos update not allowed" if spec is not None else "notBound")
            return
        candidate, why = self._parse_cf_update(payload, spec.qos)
        if candidate is None:
            _reject(why)
            return
        if candidate == spec.qos:
            # 에코 가드: 자기 총함수 게시(또는 무변경 UPDATE)의 NOTIFY
            self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)
            return
        if direction == "command":
            violation = command_qos_violation(candidate.liveliness,
                                              candidate.deadline_ms)
            if violation:
                _reject(f"command qos violation: {violation} (§8.5)")
                return
        ok, reasons = self.adapter.check_candidate(key, candidate, direction)
        if not ok:
            _reject(f"predicted incompatible: {'; '.join(reasons)}")
            return
        if reasons:
            self.emit_event("qosStatus", "warning",
                            {"event": "predictedIncompatible", "interface": ev.interface,
                             "robot": ev.robot_id, "reasons": reasons})
        if not self.adapter.rebind_interface(key, candidate):
            _reject("rebind failed")
            return
        self._qos_fcnt_cache.pop((ev.robot_id, ev.interface, direction), None)
        self._qos_fcnt_last_pub.pop((ev.robot_id, ev.interface, direction), None)
        self._publish_qos_state(only_key=key)   # 새 cf*+ap* 총함수 게시 = CSE 수렴
        self.emit_event("qosStatus", "info",
                        {"event": "qosConfigUpdated", "interface": ev.interface,
                         "robot": ev.robot_id, "direction": direction})
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    # ------------------------------------------------------------------
    # 타이머 (executor 스레드)
    # ------------------------------------------------------------------

    def _dispatch_decision(self, ev: InboundEvent, corr: str) -> None:
        """확인 워크플로 결정 수신(§5.4) — approve는 재시작 없이 게이트를 연다.
        corr는 dedup 키(CIN ri)이고, 제안 식별은 proposalId가 한다."""
        payload = ev.payload or {}
        decision = str(payload.get("decision", "")).lower()
        pid = ev.correlation_id or payload.get("proposalId") or ""
        key = self._confirm_pending.get(pid)
        now = time.time()
        if key is None:
            self.emit_event("provisioningStatus", "warning",
                            {"event": "unknownProposal", "proposalId": pid})
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if decision == "approve":
            spec = self.specs_by_key.get(key)
            if spec is not None:
                spec.confirm = "auto"
            # 제안 매핑은 유지한다 — 이후 revoke가 같은 proposalId로 철회할 수 있어야 함
            self.emit_event("provisioningStatus", "info",
                            {"event": "approved", "proposalId": pid,
                             "interface": key[2], "robot": key[1]})
        elif decision == "revoke":
            # 승인 철회 — 게이트를 required 보류로 되돌리고 제안을 재등록(§5.4)
            spec = self.specs_by_key.get(key)
            if spec is not None:
                spec.confirm = "required"
            self._confirm_pending[pid] = key
            self.emit_event("provisioningStatus", "warning",
                            {"event": "revoked", "proposalId": pid,
                             "interface": key[2], "robot": key[1]})
        elif decision in ("reject", "defer"):
            self.emit_event("provisioningStatus", "warning",
                            {"event": decision + "ed", "proposalId": pid,
                             "interface": key[2], "robot": key[1]})
        else:
            self.emit_event("provisioningStatus", "warning",
                            {"event": "invalidDecision", "proposalId": corr,
                             "decision": decision})
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    # ------------------------------------------------------------------
    # 계약 게시 (input_example, QoS 메타, 확인 제안)
    # ------------------------------------------------------------------

    def _finish(self, robot: str, iface: str, corr: str, terminal: str, ts: float) -> bool:
        self._inflight.get((robot, iface), set()).discard(corr)
        return self.state.finish(robot, iface, corr, terminal, ts)

    def _anomaly_event(self, op: Any) -> None:
        # CIN 자체는 매번 가고(fast-path), 알림 이벤트만 인터페이스당 5s coalesce
        key = (op.robot_id, op.interface)
        now = time.monotonic()
        last = getattr(self, "_anomaly_last", None)
        if last is None:
            last = self._anomaly_last = {}
        if key in last and now - last[key] < 5.0:
            return
        last[key] = now
        self.emit_event("topicHealth", "warning",
                        {"event": "anomalyDetected", "interface": op.interface,
                         "robot": op.robot_id,
                         "anomaly": (op.content or {}).get("anomaly")})

