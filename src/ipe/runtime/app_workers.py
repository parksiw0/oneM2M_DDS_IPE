"""IPEApp 분해 mixin — 상태는 전부 IPEApp.__init__이 소유한다.

각 mixin은 self의 구성요소(state/queues/adapter/path_map/...)를 공유하는
같은 객체의 단면이다. 단독 인스턴스화 금지.
"""

from __future__ import annotations

import logging
import queue
import time
from typing import Any

from ipe.core.policy import Op
from ipe.core.vocab import CLASS_TERMINAL
from ipe.onem2m.client import OversizeError, TransportError, classify
from ipe.runtime.dispatcher import InboundEvent

log = logging.getLogger(__name__)

GOAL_STATUS_TO_REASON = {4: "succeeded", 5: "canceled", 6: "aborted"}

class WorkersMixin:
    def _spool_op(self, op: Op) -> None:
        import json as _json
        self.state.spool_put(op.queue_class, f"{op.robot_id}:{op.interface}:{op.view}",
                             _json.dumps({"kind": op.kind, "path": op.path,
                                          "content": op.content, "rn": op.rn}),
                             time.time())

    def _outbound_worker(self) -> None:
        rec = self.rc.recovery
        retries = int(rec.get("retry_count", 3))
        base_ms = int(rec.get("retry_delay_ms", 500))
        while not self._stop_worker.is_set():
            # 유일한 CSE 쓰기 스레드 — 어떤 예외에도 죽지 않는다
            try:
                try:
                    op = self.outbound.get(timeout=0.5)
                except queue.Empty:
                    self._drain_spool()
                    continue
                self._send_with_retry(op, retries, base_ms)
            except Exception:
                log.exception("outbound worker iteration failed (isolated)")

    def _send_with_retry(self, op: Op, retries: int, base_ms: int) -> None:
        from ipe.onem2m.client import backoff_delays
        delays = iter(backoff_delays(retries, base_ms))
        attempt = 0
        while True:
            failure: Any = None
            try:
                if op.kind == "update_fcnt":
                    fr = self.worker_ops.update_fcnt(op.path, op.content)
                    if fr.ok:
                        return
                    failure = fr
                elif op.kind == "update_lbl":
                    lr = self.worker_ops.update_lbl(op.path, op.content["labels"])
                    if lr.ok:
                        return
                    failure = lr
                else:
                    r = self.worker_ops.create_cin(op.path, op.content,
                                                   rn=getattr(op, "rn", None))
                    if r.created or r.duplicate:
                        return
                    failure = r.response
            except (TransportError, OversizeError) as e:
                failure = e
            cls = classify(failure)
            if cls == "non_recoverable":
                log.error("non-recoverable op dropped: %s (%s)", op.path, failure)
                # §15.5: 무음 금지 — 4xx는 CSE 생존 상태이므로 이벤트 송신 가능
                self.emit_event("ipeHealth", "error",
                                {"event": "opFailed", "path": op.path,
                                 "interface": op.interface, "robot": op.robot_id,
                                 "kind": op.kind})
                return
            if cls == "policy_dependent":
                self._prov_jobs.put(("reconcile", None))
                if op.queue_class == CLASS_TERMINAL:
                    self._spool_op(op)
                return
            attempt += 1
            if attempt > retries:
                if op.queue_class == CLASS_TERMINAL:
                    self._spool_op(op)
                self._prov_jobs.put(("catchup", "cse-recovered"))
                return
            self._stop_worker.wait(next(delays) / 1000.0)

    def _drain_spool(self) -> None:
        import json as _json
        for row in self.state.spool_list(limit=20):
            data = _json.loads(row["payload"])
            kind = data.get("kind", "create_cin")
            try:
                if kind == "update_fcnt":
                    r_ok = self.worker_ops.update_fcnt(data["path"], data["content"]).ok
                elif kind == "update_lbl":
                    r_ok = self.worker_ops.update_lbl(
                        data["path"], data["content"]["labels"]).ok
                else:
                    r = self.worker_ops.create_cin(data["path"], data["content"],
                                                   rn=data.get("rn"))
                    if not (r.created or r.duplicate) and classify(r.response) == "non_recoverable":
                        # poison row — 영구 실패 op가 드레인을 막으면 안 된다
                        log.error("poison spool row %s dropped: %s", row["id"], data["path"])
                        self.state.spool_delete([row["id"]])
                        continue
                    r_ok = r.created or r.duplicate
            except (TransportError, OversizeError):
                return   # CSE 불가 — 다음 유휴 사이클에 재시도
            if r_ok:
                self.state.spool_delete([row["id"]])
            else:
                return

    # ------------------------------------------------------------------
    # 프로비저닝 워커
    # ------------------------------------------------------------------

    def _prov_worker(self) -> None:
        while not self._stop_worker.is_set():
            try:
                job, arg = self._prov_jobs.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                if job == "reconcile":
                    restarted = self.provisioner.check_cse_identity()
                    self._absorb_provision(self.provisioner.provision_all())
                    # 재프로비저닝 후 캐시 1회 무효화(§4.8.4) — 재생성된 qos
                    # FCNT가 CREATE 초기 속성만 든 채 캐시에 가려지는 것을 막는다.
                    # 게시는 executor 틱이 수행한다(스레드 소유권).
                    self._qos_republish.set()
                    if restarted:
                        self.catchup.sweep("cse-restart")
                elif job == "catchup":
                    self.catchup.sweep(str(arg or "manual"))
                elif job == "reconcile_discovery":
                    self._reconcile_discovery(arg)
            except Exception:
                log.exception("provisioning job %s failed", job)

    def _reconcile_discovery(self, snap: dict[str, Any]) -> None:
        from ipe.config.resolver import resolve
        try:
            new_rc = resolve(self.rc.raw, discovered=snap)
        except Exception as e:
            log.warning("discovery re-resolve failed: %s", e)
            return
        self._churn_track(snap)

        def fresh_of(new_list, cur_list, type_attr):
            known = {(x.robot_id, x.interface) for x in cur_list}
            return [x for x in new_list
                    if (x.robot_id, x.interface) not in known and getattr(x, type_attr)]

        fresh_t = fresh_of(new_rc.topics, self.rc.topics, "msg_type")
        fresh_s = fresh_of(new_rc.services, self.rc.services, "srv_type")
        fresh_a = fresh_of(new_rc.actions, self.rc.actions, "action_type")
        if not (fresh_t or fresh_s or fresh_a):
            return
        self.rc.topics.extend(fresh_t)
        self.rc.services.extend(fresh_s)
        self.rc.actions.extend(fresh_a)
        self._absorb_provision(self.provisioner.provision_all())
        # 엔티티 생성은 executor 스레드에서만 해야 한다
        for kind, items in (("_bind_topic", fresh_t), ("_bind_service", fresh_s),
                            ("_bind_action", fresh_a)):
            for x in items:
                ev = InboundEvent(kind=kind, robot_id=x.robot_id,
                                  interface=x.interface, correlation_id=None,
                                  event_id=f"bind:{x.interface}", payload=None, ct=None)
                ev.spec = x
                self.inbound.put_control(ev)
        if self.guard is not None:
            self.guard.trigger()

