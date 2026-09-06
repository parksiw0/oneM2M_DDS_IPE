"""Request admission, command/service/action execution, approvals, and QoS requests."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from ipe.core.command import CommandDispatchManager
from ipe.core.common import deep_merge as _deep_merge
from ipe.core.common import project_top_level as _project
from ipe.core.models import ActionSpec, ServiceSpec, TopicSpec
from ipe.core.payload import ct_to_epoch as _ct_to_epoch
from ipe.core.pipeline import Op
from ipe.core.transaction import ActionTransactionManager, ServiceTransactionManager
from ipe.core.vocab import CLASS_OBSERVE_BULK, CLASS_TERMINAL
from ipe.onem2m.catchup import CatchUpSweeper
from ipe.onem2m.notification import Notification
from ipe.onem2m.resource_ops import ResourceOps
from ipe.qos.requests import fields_in_update, parse_cf_update, policy_changes_to_cf
from ipe.runtime.dispatcher import InboundEvent
from ipe.runtime.queues import InboundQueue
from ipe.runtime.state import StatePersistence

if TYPE_CHECKING:
    from ipe.runtime.bindings import BindingRegistry
    from ipe.runtime.outbound import OutboundProcessor
    from ipe.runtime.status import StatusPublisher

log = logging.getLogger(__name__)


GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}


class InboundProcessor:
    def __init__(
        self,
        registry: BindingRegistry,
        state: StatePersistence,
        prov_ops: ResourceOps,
        outbound: OutboundProcessor,
        status: StatusPublisher,
        on_binding_event: Callable[[InboundEvent], None],
    ) -> None:
        self.registry = registry
        self.state = state
        self.outbound = outbound
        self.status = status
        self._on_binding_event = on_binding_event
        self.adapter: Any = None
        self._wake: Callable[[], None] = lambda: None
        rec = registry.rc.recovery
        self.queue = InboundQueue(
            maxsize=rec.get("inbound_max", 1000), control_maxsize=rec.get("control_lane_max", 64)
        )
        self._admission_lock = threading.Lock()
        self.catchup = CatchUpSweeper(state, prov_ops, self.catchup_admit)
        self.svc_tx = ServiceTransactionManager(state)
        self.act_tx = ActionTransactionManager(state)
        self.cmd_mgr = CommandDispatchManager(
            self._publish_command,
            lambda spec, payload: self.adapter.validate_command(spec, payload),
        )
        self._inflight: dict[tuple[str, str], set[str]] = {}
        self._confirm_pending: dict[str, tuple[str, str, str]] = {}
        self._approval_prompter: Any = None
        self._approval_requests_emitted: set[str] = set()

    def attach_adapter(self, adapter: Any, wake: Callable[[], None]) -> None:
        self.adapter = adapter
        self._wake = wake

    def enqueue_internal(self, event: InboundEvent) -> bool:
        if not self.queue.put_control(event):
            return False
        self._wake()
        return True

    def _handle_internal(self, event: InboundEvent) -> None:
        if event.kind == "_control_approval":
            self._apply_popup_control_decision(event)
        else:
            self._on_binding_event(event)

    def diagnostics(self) -> dict[str, Any]:
        return {"inbound": self.queue.depths(), "pending_confirm": dict(self._confirm_pending)}

    def sweep_timeouts(self) -> None:
        now = time.time()
        for corr in self.svc_tx.sweep_timeouts(now):
            self._finish_timed_out(corr, now)
            self.outbound.emit_event(
                "serviceStatus", "warning", {"event": "timeout", "requestId": corr}
            )
        for corr in self.act_tx.sweep_timeouts(now):
            self._finish_timed_out(corr, now)
            self.outbound.emit_event(
                "actionStatus", "warning", {"event": "timeout", "goalId": corr}
            )

    def _finish_timed_out(self, corr: str, now: float) -> None:
        for (robot, iface), requests in list(self._inflight.items()):
            if corr in requests:
                self._finish(robot, iface, corr, "failed", now)

    def shutdown(self, spin_once: Callable[[float], None]) -> None:
        now = time.time()
        for ev in self.queue.get_batch(10_000):
            corr = ev.dedup_corr or ev.correlation_id or ev.event_id or ""
            if ev.kind == "cancel":
                with suppress(Exception):
                    self._dispatch_one(ev, corr)
            elif ev.kind.startswith("_"):
                continue
            else:
                self.outbound.emit_event(
                    self._status_category(ev.kind),
                    "warning",
                    {
                        "event": "rejected",
                        "reason": "shuttingDown",
                        "interface": ev.interface,
                        "robot": ev.robot_id,
                        "correlationId": corr,
                    },
                )
                self._finish(ev.robot_id, ev.interface, corr, "shuttingDown", now)
        active_states = {"invoked", "goalSent", "goalAccepted", "executing", "canceling"}
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not any(t["state"] in active_states for t in self.state.active_transactions()):
                break
            with suppress(Exception):
                spin_once(0.2)
        now = time.time()
        for tx in self.state.active_transactions():
            if tx["state"] in active_states:
                terminal = "shutdownAbandoned" if tx["kind"] == "action" else "failed"
                self.state.update_transaction(tx["corr_id"], terminal, now)
                self.outbound.emit_event(
                    "actionStatus" if tx["kind"] == "action" else "serviceStatus",
                    "warning",
                    {"event": "shutdownAbandoned", "correlationId": tx["corr_id"]},
                )

    def on_notify(self, path_key: str, notif: Notification) -> str:
        with self._admission_lock:
            return self._admit(path_key, notif)

    def catchup_admit(
        self, path_key: str, cin_ri: str, con: dict[str, Any] | None, ct: str | None
    ) -> str:
        notif = Notification(
            vrq=False, sur=None, net=3, cr=None, cin_ri=cin_ri, cin_ct=ct, con=con, raw={}
        )
        with self._admission_lock:
            return self._admit(path_key, notif)

    def _admit(self, path_key: str, notif: Notification) -> str:
        ev = self.registry.routes.route(path_key, notif)
        if ev is None:
            if self.registry.routes_staging.is_set():
                return "denied"
            self.outbound.emit_event(
                "ipeHealth", "warning", {"event": "unknownRoute", "path_key": path_key}
            )
            return "invalid"
        if notif.cr is not None and notif.cr == self.registry.aei:
            return "denied"  # 알림 루프 방지 불변식 (cr == 자기 aei)
        if ev.kind == "decision":
            corr = ev.event_id or ev.correlation_id or ""
        elif ev.kind == "cancel":
            corr = f"cancel:{ev.event_id or ev.correlation_id or ''}"
        else:
            corr = ev.correlation_id or ev.event_id or ""
        ev.dedup_corr = corr  # 드레인의 CAS가 반드시 같은 키를 봐야 한다
        now = time.time()
        verdict = self.state.admit(ev.robot_id, ev.interface, corr, ev.event_id or "", now)
        if verdict == "duplicate":
            self.outbound.emit_event(
                self._status_category(ev.kind),
                "info",
                {
                    "event": "duplicate",
                    "interface": ev.interface,
                    "robot": ev.robot_id,
                    "correlationId": corr,
                },
            )
            return "duplicate"
        ev.ingest_monotonic = time.monotonic()
        ok = self.queue.put_control(ev) if ev.kind == "cancel" else self.queue.put_normal(ev)
        if not ok:
            self.state.mark_overflow(ev.robot_id, ev.interface, corr, now)
            log.error("inbound overflow: %s (%s)", path_key, corr)
            return "overflow"
        self.catchup.mark_processed(path_key, ev.ct)
        self._wake()
        return "ok"

    @staticmethod
    def _status_category(kind: str) -> str:
        return {
            "command": "commandStatus",
            "service": "serviceStatus",
            "action_goal": "actionStatus",
            "cancel": "actionStatus",
            "decision": "provisioningStatus",
            "qos_update": "qosStatus",
            "qos_policy": "qosStatus",
        }.get(kind, "ipeHealth")

    def drain(self) -> None:
        budget = int(self.registry.rc.dispatch.get("drain_budget", 32))
        for ev in self.queue.get_batch(budget):
            corr = ev.dedup_corr or ev.correlation_id or ev.event_id or ""
            try:
                self._dispatch_one(ev, corr)
            except Exception as e:
                log.exception("dispatch failed for %s/%s", ev.kind, ev.interface)
                self.outbound.emit_event(
                    self._status_category(ev.kind),
                    "error",
                    {
                        "event": "dispatchError",
                        "interface": ev.interface,
                        "robot": ev.robot_id,
                        "error": str(e),
                        "correlationId": corr,
                    },
                )
                self._finish(ev.robot_id, ev.interface, corr, "failed", time.time())
        if not self.queue.empty():
            self._wake()

    def _dispatch_one(self, ev: InboundEvent, corr: str) -> None:
        if ev.kind.startswith("_"):
            self._handle_internal(ev)
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
        elif ev.kind == "qos_policy":
            self._dispatch_qos_policy(ev, corr)
        else:
            self.outbound.emit_event(
                "ipeHealth", "warning", {"event": "unhandledKind", "kind": ev.kind}
            )
            self._finish(ev.robot_id, ev.interface, corr, "rejected", time.time())

    def _publish_command(self, spec: TopicSpec, payload: dict[str, Any]) -> bool:
        return bool(self.adapter.publish_command(spec, payload))

    def _dispatch_command(self, ev: InboundEvent, corr: str) -> None:
        spec = self.registry.specs_by_key.get(("command", ev.robot_id, ev.interface))
        now = time.time()
        if spec is None:
            self.outbound.emit_event(
                "commandStatus",
                "error",
                {
                    "event": "rejected",
                    "reason": "notBound",
                    "interface": ev.interface,
                    "robot": ev.robot_id,
                    "commandId": corr,
                },
            )
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        payload = dict(ev.payload or {})
        payload.pop("commandId", None)
        outcome = self.cmd_mgr.dispatch(
            spec,
            payload,
            _ct_to_epoch(ev.ct, cse_timezone=self.registry.rc.cse.timezone),
            getattr(ev, "ingest_monotonic", None) or time.monotonic(),
        )
        if outcome.status == "approvalRequired":
            self._request_control_approval(spec, payload)
        self.outbound.emit_event(
            "commandStatus",
            "info" if outcome.published else "warning",
            {
                "event": outcome.status,
                "interface": ev.interface,
                "robot": ev.robot_id,
                "commandId": corr,
                "detail": outcome.detail,
                "clamped": outcome.clamped,
            },
        )
        terminal = {
            "published": "succeeded",
            "expired": "expired",
            "accessDenied": "accessDenied",
        }.get(outcome.status, "rejected")
        self._finish(ev.robot_id, ev.interface, corr, terminal, time.time())

    def _request_control_approval(
        self,
        spec: TopicSpec,
        payload: dict[str, Any],
    ) -> None:
        from ipe.runtime.approval import (
            ApprovalRequest,
            DesktopApprovalPrompt,
            command_preview,
            control_approval_id,
        )

        if not spec.msg_type:
            return
        proposal_id = control_approval_id(
            spec.robot_id,
            spec.interface,
            spec.msg_type,
        )
        key = ("command", spec.robot_id, spec.interface)
        self._confirm_pending[proposal_id] = key
        preview = command_preview(payload)
        if proposal_id not in self._approval_requests_emitted:
            self._approval_requests_emitted.add(proposal_id)
            ae = f"/{self.registry.rc.cse.cse_base}/{self.registry.rc.cse.ae_name}"
            self.outbound.put_terminal(
                Op(
                    "create_cin",
                    f"{ae}/config/pendingMappingProposal",
                    {
                        "proposalId": proposal_id,
                        "kind": "command",
                        "robot": spec.robot_id,
                        "interface": spec.interface,
                        "type": spec.msg_type,
                        "reason": "firstUseOfAmbiguousTopic",
                        "commandPreview": preview,
                    },
                    spec.robot_id,
                    spec.interface,
                    "proposal",
                    CLASS_TERMINAL,
                    rn=f"pmp_{proposal_id}"[:60],
                )
            )
        if self._approval_prompter is None:
            self._approval_prompter = DesktopApprovalPrompt(self._enqueue_popup_control_decision)
        self._approval_prompter.request(
            ApprovalRequest(
                proposal_id=proposal_id,
                robot_id=spec.robot_id,
                interface=spec.interface,
                msg_type=spec.msg_type,
                preview=preview,
            )
        )

    def _enqueue_popup_control_decision(self, request: Any, decision: str) -> None:
        ev = InboundEvent(
            kind="_control_approval",
            robot_id=request.robot_id,
            interface=request.interface,
            correlation_id=request.proposal_id,
            event_id=f"popup:{time.time_ns()}",
            payload={"proposalId": request.proposal_id, "decision": decision},
            ct=None,
        )
        if not self.queue.put_control(ev):
            log.error("control lane full: popup decision was not applied")
            return
        self._wake()

    def _apply_popup_control_decision(self, ev: InboundEvent) -> None:
        payload = ev.payload or {}
        self._apply_control_decision(
            str(payload.get("proposalId", "")),
            str(payload.get("decision", "")),
            source="popup",
        )

    @staticmethod
    def _control_approval_record(spec: TopicSpec) -> dict[str, str]:
        return {
            "robot": spec.robot_id,
            "interface": spec.interface,
            "type": spec.msg_type or "",
        }

    def apply_saved_control_approvals(self, rc: Any) -> None:
        from ipe.runtime.approval import control_approval_id

        saved = self.state.get_kv("control_approvals", {})
        for spec in rc.topics:
            if spec.direction != "both" or spec.confirm != "on_first_use" or not spec.msg_type:
                continue
            proposal_id = control_approval_id(
                spec.robot_id,
                spec.interface,
                spec.msg_type,
            )
            if saved.get(proposal_id) == self._control_approval_record(spec):
                spec.confirm = "auto"

    def _persist_control_approval(self, proposal_id: str, spec: TopicSpec) -> None:
        saved = self.state.get_kv("control_approvals", {})
        saved[proposal_id] = self._control_approval_record(spec)
        self.state.set_kv("control_approvals", saved)

    def _remove_control_approval(self, proposal_id: str) -> None:
        saved = self.state.get_kv("control_approvals", {})
        if proposal_id in saved:
            saved.pop(proposal_id)
            self.state.set_kv("control_approvals", saved)

    def _apply_control_decision(
        self,
        proposal_id: str,
        decision: str,
        *,
        source: str,
    ) -> bool:
        key = self._confirm_pending.get(proposal_id)
        if key is None:
            self.outbound.emit_event(
                "provisioningStatus",
                "warning",
                {"event": "unknownProposal", "proposalId": proposal_id},
            )
            return False
        spec = self.registry.specs_by_key.get(key)
        if spec is None:
            self.outbound.emit_event(
                "provisioningStatus",
                "warning",
                {"event": "proposalNotBound", "proposalId": proposal_id},
            )
            return False
        normalized = decision.lower()
        if normalized == "approve":
            if isinstance(spec, TopicSpec) and not self.adapter.bind_command(spec):
                self.outbound.emit_event(
                    "provisioningStatus",
                    "error",
                    {"event": "approvalBindFailed", "proposalId": proposal_id},
                )
                return False
            spec.confirm = "auto"
            if isinstance(spec, TopicSpec) and spec.direction == "both":
                self._persist_control_approval(proposal_id, spec)
            event = "approved"
            severity = "info"
        elif normalized == "revoke":
            if isinstance(spec, TopicSpec) and spec.direction == "both":
                self.adapter.unbind_command((spec.robot_id, spec.interface))
                spec.confirm = "on_first_use"
                self._remove_control_approval(proposal_id)
            else:
                spec.confirm = "required"
            event = "revoked"
            severity = "warning"
        elif normalized in ("reject", "defer"):
            event = "rejected" if normalized == "reject" else "deferred"
            severity = "warning"
        elif normalized == "unavailable":
            event = "approvalPopupUnavailable"
            severity = "warning"
        else:
            event = "invalidDecision"
            severity = "warning"
        self.outbound.emit_event(
            "provisioningStatus",
            severity,
            {
                "event": event,
                "proposalId": proposal_id,
                "interface": key[2],
                "robot": key[1],
                "source": source,
            },
        )
        return normalized == "approve"

    def _dispatch_service(self, ev: InboundEvent, corr: str) -> None:
        spec: ServiceSpec | None = self.registry.specs_by_key.get(
            ("service", ev.robot_id, ev.interface)
        )
        now = time.time()
        if spec is None:
            self._service_event(ev, corr, "rejected", "notBound")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if not spec.access_enabled:
            self._service_event(ev, corr, "rejected", "accessDenied")
            self._finish(ev.robot_id, ev.interface, corr, "accessDenied", now)
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
        merged = (
            _deep_merge(dict(spec.request_template), payload) if spec.request_template else payload
        )
        self.svc_tx.set_state(corr, "accepted", now)

        def done(
            resp: dict[str, Any] | None, err: str | None, _ev: InboundEvent = ev, _corr: str = corr
        ) -> None:
            # executor Task 컨텍스트: 상태 기록 + enqueue만 허용
            t = time.time()
            if err is not None:
                if not self.svc_tx.set_state(_corr, "failed", t):
                    return
                self._service_event(_ev, _corr, "failed", err)
                self._finish(_ev.robot_id, _ev.interface, _corr, "failed", t)
                return
            if not self.svc_tx.set_state(_corr, "responded", t):
                return
            resp_path = self.registry.path_map.get((_ev.robot_id, _ev.interface, "response"))
            if resp_path:
                if spec.response_fields:
                    resp = _project(resp or {}, spec.response_fields)
                self.outbound.put_terminal(
                    Op(
                        "create_cin",
                        resp_path,
                        {"requestId": _corr, "response": resp},
                        _ev.robot_id,
                        _ev.interface,
                        "response",
                        CLASS_TERMINAL,
                    )
                )
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
        self.outbound.emit_event(
            "serviceStatus",
            "warning" if status in ("timeout", "rejected", "failed") else "info",
            {
                "event": status,
                "interface": ev.interface,
                "robot": ev.robot_id,
                "requestId": corr,
                "detail": detail,
            },
        )

    def _dispatch_goal(self, ev: InboundEvent, corr: str) -> None:
        spec: ActionSpec | None = self.registry.specs_by_key.get(
            ("action", ev.robot_id, ev.interface)
        )
        now = time.time()
        if spec is None:
            self._action_event(ev, corr, 0, "goalRejected", "notBound")
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if not spec.access_enabled:
            self._action_event(ev, corr, 0, "goalRejected", "accessDenied")
            self._finish(ev.robot_id, ev.interface, corr, "accessDenied", now)
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
        goal = _deep_merge(dict(spec.goal_template), payload) if spec.goal_template else payload
        if spec.goal_fields:
            goal = _project(goal, spec.goal_fields, strict=True)

        fb_interval = spec.feedback_sample.interval_sec if spec.feedback_sample else 0.0
        fb_last = {"t": 0.0}

        def on_goal_response(goal_id: str, accepted: bool) -> None:
            t = time.time()
            if not self.act_tx.set_state(goal_id, "goalAccepted" if accepted else "goalRejected", t):
                return
            if accepted:
                self._action_event(ev, goal_id, 2, None, "accepted")
            else:
                self._action_event(ev, goal_id, 0, "goalRejected", "")
                self._finish(ev.robot_id, ev.interface, goal_id, "rejected", t)

        def on_feedback(goal_id: str, fb: dict[str, Any]) -> None:
            if goal_id not in self._inflight.get((ev.robot_id, ev.interface), ()):
                return
            if spec.feedback != "log" and fb_interval:
                now_m = time.monotonic()
                if now_m - fb_last["t"] < fb_interval:
                    return  # 샘플링은 유일하게 허용된 feedback 드롭
                fb_last["t"] = now_m
            seq = self.act_tx.next_feedback_seq(goal_id, time.time())
            path = self.registry.path_map.get((ev.robot_id, ev.interface, "feedback"))
            if path:
                if spec.feedback_fields:
                    fb = _project(fb, spec.feedback_fields)
                self.outbound.queue.put(
                    Op(
                        "create_cin",
                        path,
                        {"goalId": goal_id, "feedbackSeq": seq, "feedback": fb},
                        ev.robot_id,
                        ev.interface,
                        "feedback",
                        CLASS_OBSERVE_BULK,
                    ),
                    CLASS_OBSERVE_BULK,
                )

        def on_result(goal_id: str, status_int: int, result: dict[str, Any]) -> None:
            t = time.time()
            if not self.act_tx.set_state(goal_id, "resultReceived", t):
                return
            reason = GOAL_STATUS_TO_REASON.get(status_int, "failed")
            path = self.registry.path_map.get((ev.robot_id, ev.interface, "result"))
            if path:
                if spec.result_fields:
                    result = _project(result, spec.result_fields)
                self.outbound.put_terminal(
                    Op(
                        "create_cin",
                        path,
                        {
                            "goalId": goal_id,
                            "goalStatus": status_int,
                            "terminationReason": reason,
                            "result": result,
                        },
                        ev.robot_id,
                        ev.interface,
                        "result",
                        CLASS_TERMINAL,
                    )
                )
            self._action_event(ev, goal_id, status_int, reason, "")
            self._finish(ev.robot_id, ev.interface, goal_id, "succeeded", t)

        try:
            sent = self.adapter.send_goal(
                spec, corr, goal, on_goal_response, on_feedback, on_result
            )
        except Exception as e:
            from ipe.adapter.messages import TranscodeError

            reason = "goalRejected" if isinstance(e, TranscodeError) else "failed"
            terminal = "rejected" if isinstance(e, TranscodeError) else "failed"
            self.act_tx.set_state(
                corr, "goalRejected" if terminal == "rejected" else "failed", time.time()
            )
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
        spec: ActionSpec | None = self.registry.specs_by_key.get(
            ("action", ev.robot_id, ev.interface)
        )
        goal_id = (ev.payload or {}).get("goalId") or corr
        now = time.time()
        if spec is None:
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)
            return
        if not spec.access_enabled:
            self._action_event(ev, goal_id, 0, "cancelRejected", "accessDenied")
            self._finish(ev.robot_id, ev.interface, corr, "accessDenied", now)
            return
        verdict = self.adapter.cancel_goal(spec, goal_id)
        if verdict == "unknown":
            self._action_event(ev, goal_id, 0, "cancelRejected", "unknownGoal")
        else:
            self.act_tx.set_state(goal_id, "canceling", now)
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    def _action_event(
        self, ev: InboundEvent, goal_id: str, status_int: int, reason: str | None, detail: str
    ) -> None:
        path = self.registry.path_map.get((ev.robot_id, ev.interface, "actionStatus"))
        if path:
            self.outbound.put_terminal(
                Op(
                    "create_cin",
                    path,
                    {
                        "goalId": goal_id,
                        "goalStatus": status_int,
                        "terminationReason": reason,
                        "detail": detail,
                    },
                    ev.robot_id,
                    ev.interface,
                    "actionStatus",
                    CLASS_TERMINAL,
                )
            )


    def _apply_qos_candidate(
        self,
        spec: Any,
        key: tuple[str, str],
        direction: str,
        candidate: Any,
        explicit_fields: frozenset[str] | None = None,
    ) -> tuple[bool, str, list[str]]:
        """Validate, rebind, persist, and publish one topic-direction QoS change."""
        from ipe.qos.configuration import command_qos_violation

        if direction == "command":
            violation = command_qos_violation(
                candidate.liveliness,
                candidate.deadline_ms,
            )
            if violation:
                return False, f"command qos violation: {violation} (§8.5)", []
        ok, reasons = self.adapter.check_candidate(key, candidate, direction, explicit_fields=explicit_fields)
        if not ok:
            return False, f"predicted incompatible: {'; '.join(reasons)}", reasons
        rebind = getattr(self.adapter, "rebind_direction", None)
        rebound = (
            rebind(key, candidate, direction, explicit_fields=explicit_fields)
            if rebind is not None
            else self.adapter.rebind_interface(key, candidate)
        )
        if not rebound:
            return False, "rebind failed and the previous endpoint was restored", reasons
        self._persist_qos_override(key[0], key[1], direction, candidate, spec.qos_explicit_fields)
        cache_key = (key[0], key[1], direction)
        self.status.invalidate_qos(cache_key)
        self.status.publish_qos(only_key=key)
        return True, "", reasons

    def _persist_qos_override(
        self,
        robot: str,
        interface: str,
        direction: str,
        qos: Any,
        explicit_fields: frozenset[str] = frozenset(),
    ) -> None:
        saved = self.state.get_kv("qos_overrides", {})
        saved[f"{robot}|{direction}|{interface}"] = {**qos.__dict__, "_explicit_fields": sorted(explicit_fields)}
        self.state.set_kv("qos_overrides", saved)

    def apply_saved_qos_overrides(self, rc: Any) -> None:
        """Restore accepted topic QoS requests before DDS endpoints are created."""
        from ipe.qos.models import QoSSpec

        saved = self.state.get_kv("qos_overrides", {})
        for spec in rc.topics:
            for direction in ("observe", "command"):
                if direction == "observe" and spec.direction not in ("observe", "both"):
                    continue
                if direction == "command" and spec.direction not in ("command", "both"):
                    continue
                value = saved.get(f"{spec.robot_id}|{direction}|{spec.interface}")
                if not isinstance(value, dict):
                    continue
                try:
                    values = {key: item for key, item in value.items() if key != "_explicit_fields"}
                    spec.set_qos_for(direction, QoSSpec(**values))
                    if direction == "observe":
                        spec.qos_explicit_fields |= frozenset(value.get("_explicit_fields", set(values) - {"profile"}))
                        spec.qos_explicit = bool(spec.qos_explicit_fields)
                except (TypeError, ValueError) as exc:
                    log.warning(
                        "ignored invalid saved QoS override for %s: %s", spec.interface, exc
                    )


    def _dispatch_qos_update(self, ev: InboundEvent, corr: str) -> None:
        now = time.time()
        direction = (ev.meta or {}).get("direction", "observe")
        key = (ev.robot_id, ev.interface)
        spec_key = ("command" if direction == "command" else "observe", ev.robot_id, ev.interface)
        spec = self.registry.specs_by_key.get(spec_key)
        payload = ev.payload or {}

        def _reject(reason: str) -> None:
            self.outbound.emit_event(
                "qosStatus",
                "warning",
                {
                    "event": "qosUpdateRejected",
                    "interface": ev.interface,
                    "robot": ev.robot_id,
                    "direction": direction,
                    "reason": reason,
                },
            )
            # 원복: 직전 정본 레코드 재게시로 CSE의 cf*를 되돌린다
            self.status.invalidate_qos((ev.robot_id, ev.interface, direction))
            self.status.publish_qos(only_key=key)
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)

        if not self.registry.rc.qos_fcnt.allow_update or spec is None:
            _reject("qos update not allowed" if spec is not None else "notBound")
            return
        base = spec.qos_for(direction)
        candidate, why = parse_cf_update(payload, base)
        if candidate is None:
            _reject(why)
            return
        if candidate == base:
            # 에코 가드: 자기 총함수 게시(또는 무변경 UPDATE)의 NOTIFY
            self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)
            return
        ok, why, reasons = self._apply_qos_candidate(spec, key, direction, candidate, fields_in_update(payload))
        if not ok:
            _reject(why)
            return
        if reasons:
            self.outbound.emit_event(
                "qosStatus",
                "warning",
                {
                    "event": "predictedIncompatible",
                    "interface": ev.interface,
                    "robot": ev.robot_id,
                    "reasons": reasons,
                },
            )
        self.outbound.emit_event(
            "qosStatus",
            "info",
            {
                "event": "qosConfigUpdated",
                "interface": ev.interface,
                "robot": ev.robot_id,
                "direction": direction,
            },
        )
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    def _dispatch_qos_policy(self, ev: InboundEvent, corr: str) -> None:
        now = time.time()
        payload = ev.payload or {}
        request_id = str(payload.get("requestId") or corr)
        target = payload.get("target")

        def reject(reason: str, *, robot: str = "-", interface: str = "") -> None:
            self.outbound.emit_event(
                "qosStatus",
                "warning",
                {
                    "event": "qosPolicyRejected",
                    "requestId": request_id,
                    "robot": robot,
                    "interface": interface,
                    "reason": reason,
                },
            )
            self._finish(ev.robot_id, ev.interface, corr, "rejected", now)

        if not isinstance(target, dict):
            reject("target must be an object")
            return
        robot = str(target.get("robot", ""))
        interface = str(target.get("interface", ""))
        direction = str(target.get("direction", "observe")).lower()
        interface_kind = str(target.get("kind", "topic")).lower()
        if interface_kind != "topic":
            reject(
                "dynamic QoS updates are currently supported only for topic interfaces",
                robot=robot,
                interface=interface,
            )
            return
        if direction not in ("observe", "command"):
            reject("direction must be observe or command", robot=robot, interface=interface)
            return
        spec = self.registry.specs_by_key.get((direction, robot, interface))
        if spec is None:
            reject("target topic direction is not bound", robot=robot, interface=interface)
            return
        changes = payload.get("changes")
        if not isinstance(changes, dict):
            reject("changes must be an object", robot=robot, interface=interface)
            return
        cache_key = (robot, interface, direction)
        base_revision = payload.get("baseRevision")
        if base_revision is not None:
            if not isinstance(base_revision, int) or isinstance(base_revision, bool):
                reject("baseRevision must be an integer", robot=robot, interface=interface)
                return
            current_revision = self.status.revision(cache_key)
            if base_revision != current_revision:
                reject(
                    f"stale baseRevision {base_revision}; current is {current_revision}",
                    robot=robot,
                    interface=interface,
                )
                return
        translated, why = policy_changes_to_cf(changes)
        if translated is None:
            reject(why, robot=robot, interface=interface)
            return
        candidate, why = parse_cf_update(translated, spec.qos_for(direction))
        if candidate is None:
            reject(why, robot=robot, interface=interface)
            return
        requested_fields = fields_in_update(translated)
        if candidate == spec.qos_for(direction) and (
            direction != "observe" or requested_fields <= spec.qos_explicit_fields
        ):
            self.outbound.emit_event(
                "qosStatus",
                "info",
                {
                    "event": "qosPolicyUnchanged",
                    "requestId": request_id,
                    "robot": robot,
                    "interface": interface,
                    "direction": direction,
                },
            )
            self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)
            return
        ok, why, warnings = self._apply_qos_candidate(
            spec,
            (robot, interface),
            direction,
            candidate,
            requested_fields,
        )
        if not ok:
            reject(why, robot=robot, interface=interface)
            return
        self.outbound.emit_event(
            "qosStatus",
            "info",
            {
                "event": "qosPolicyApplied",
                "requestId": request_id,
                "robot": robot,
                "interface": interface,
                "direction": direction,
                "warnings": warnings,
            },
        )
        self._finish(ev.robot_id, ev.interface, corr, "succeeded", now)

    def _dispatch_decision(self, ev: InboundEvent, corr: str) -> None:
        """확인 워크플로 결정 수신(§5.4) — approve는 재시작 없이 게이트를 연다.
        corr는 dedup 키(CIN ri)이고, 제안 식별은 proposalId가 한다."""
        payload = ev.payload or {}
        decision = str(payload.get("decision", "")).lower()
        pid = ev.correlation_id or payload.get("proposalId") or ""
        now = time.time()
        applied = self._apply_control_decision(pid, decision, source="cse")
        accepted = applied or decision in ("reject", "defer", "revoke")
        self._finish(
            ev.robot_id,
            ev.interface,
            corr,
            "succeeded" if accepted else "rejected",
            now,
        )

    def _finish(self, robot: str, iface: str, corr: str, terminal: str, ts: float) -> bool:
        requests = self._inflight.get((robot, iface))
        if requests is not None:
            requests.discard(corr)
            if not requests:
                self._inflight.pop((robot, iface), None)
        return bool(self.state.finish(robot, iface, corr, terminal, ts))

    @staticmethod
    def _safe(name: str) -> str:
        import re as _re

        return _re.sub(r"[^A-Za-z0-9_-]", "_", name).strip("_")

    def publish_contracts(self) -> None:
        for key, spec in list(self.registry.specs_by_key.items()):
            self.publish_contract(key, spec)
        self.status.publish_qos()

    def publish_contract(self, key: tuple[str, str, str], spec: Any) -> None:
        kind, robot, iface = key
        ae = f"/{self.registry.rc.cse.cse_base}/{self.registry.rc.cse.ae_name}"
        if kind == "command" and spec.direction == "both" and spec.msg_type:
            from ipe.runtime.approval import control_approval_id

            proposal_id = control_approval_id(robot, iface, spec.msg_type)
            self._confirm_pending[proposal_id] = key
        # 입력 계약 예시 — 외부 앱이 호출 형식을 참조한다(§3.3)
        if kind in ("command", "service", "action"):
            try:
                example = self._input_example(kind, spec)
            except Exception as e:
                log.debug("input example skipped for %s: %s", iface, e)
                example = None
            if example is not None:
                rn = f"ie_{kind}_{self._safe(robot)}_{self._safe(iface)}"[:60]
                self.outbound.put_terminal(
                    Op(
                        "create_cin",
                        f"{ae}/config/input_example",
                        {
                            "kind": kind,
                            "robot": robot,
                            "interface": iface,
                            "type": getattr(spec, "msg_type", None)
                            or getattr(spec, "srv_type", None)
                            or getattr(spec, "action_type", None),
                            "example": example,
                        },
                        robot,
                        iface,
                        "input_example",
                        CLASS_TERMINAL,
                        rn=rn,
                    )
                )
        # confirm: required → 제안 게시 + 보류 등록(§5.4)
        if kind in ("command", "service", "action") and spec.confirm == "required":
            proposal_id = f"{self._safe(robot)}_{self._safe(iface)}"
            self._confirm_pending[proposal_id] = key
            self.outbound.put_terminal(
                Op(
                    "create_cin",
                    f"{ae}/config/pendingMappingProposal",
                    {
                        "proposalId": proposal_id,
                        "kind": kind,
                        "robot": robot,
                        "interface": iface,
                        "reason": "confirm: required",
                    },
                    robot,
                    iface,
                    "proposal",
                    CLASS_TERMINAL,
                    rn=f"pmp_{proposal_id}"[:60],
                )
            )

    def _input_example(self, kind: str, spec: Any) -> dict[str, Any] | None:
        from rosidl_runtime_py.utilities import get_action, get_message, get_service

        from ipe.adapter.messages import make_input_example

        if kind == "command" and spec.msg_type:
            return make_input_example(get_message(spec.msg_type))
        if kind == "service" and spec.srv_type:
            return make_input_example(get_service(spec.srv_type).Request)
        if kind == "action" and spec.action_type:
            return make_input_example(get_action(spec.action_type).Goal)
        return None

    def terminate_inflight(self, robot: str, iface: str) -> None:
        """소멸 확정된 인터페이스의 비종결 트랜잭션을 종결한다 — 무음 대기 금지."""
        now = time.time()
        for corr in list(self._inflight.get((robot, iface), set())):
            tx = self.state.get_transaction(corr)
            if tx is None:
                continue
            if tx["kind"] == "action" and not self.act_tx.is_terminal(tx["state"]):
                self.act_tx.set_state(corr, "serverUnavailable", now)
                self.outbound.emit_event(
                    "actionStatus",
                    "warning",
                    {
                        "event": "serverUnavailable",
                        "goalId": corr,
                        "interface": iface,
                        "robot": robot,
                    },
                )
            elif tx["kind"] == "service" and not self.svc_tx.is_terminal(tx["state"]):
                self.svc_tx.set_state(corr, "failed", now)
                self.outbound.emit_event(
                    "serviceStatus",
                    "warning",
                    {
                        "event": "serverUnavailable",
                        "requestId": corr,
                        "interface": iface,
                        "robot": robot,
                    },
                )
            self._finish(robot, iface, corr, "failed", now)

    def boot_sweep(self) -> None:
        swept = self.state.sweep_boot(time.time())
        for row in swept.get("dispatched", []):
            self.outbound.emit_event(
                "ipeHealth",
                "warning",
                {
                    "event": "outcomeUnknownAtRestart",
                    "interface": row["interface"],
                    "robot": row["robot_id"],
                    "correlationId": row["corr_id"],
                },
            )
        now = time.time()
        for t in self.state.active_transactions("action"):
            if not self.act_tx.is_terminal(t["state"]):
                self.act_tx.set_state(t["corr_id"], "orphanedAtRestart", now)
                self.outbound.emit_event(
                    "actionStatus",
                    "warning",
                    {"event": "orphanedAtRestart", "goalId": t["corr_id"]},
                )
        for t in self.state.active_transactions("service"):
            if not self.svc_tx.is_terminal(t["state"]):
                self.svc_tx.set_state(t["corr_id"], "failed", now)
                self.outbound.emit_event(
                    "serviceStatus",
                    "warning",
                    {"event": "orphanedAtRestart", "requestId": t["corr_id"]},
                )

    def close(self) -> None:
        if self._approval_prompter is not None:
            self._approval_prompter.close()
            self._approval_prompter = None
