"""프로비저닝 워커 (DESIGN §6, §14.1, §16.2).

전용 워커 스레드에서 자체 HTTP 세션으로 GET-or-create 체인, SUB 생성·검증,
가용성 라벨, CSE 인스턴스 정체성 감지를 수행한다. ROS2 엔티티는 절대 건드리지
않는다 — 라우트/엔티티 갱신은 인바운드 큐를 통해 executor에 넘긴다.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from ipe.models import ResolvedConfig
from ipe.runtime.naming import sanitize_segment

log = logging.getLogger(__name__)

IPE_ROOT_CNTS = ("robots", "status", "config")
ROBOT_CNTS = ("topics", "services", "actions")
TOPIC_CNTS = ("observe", "command")
STATUS_CNTS = ("topicHealth", "nodeStatus", "qosStatus", "commandStatus",
               "serviceStatus", "actionStatus", "provisioningStatus", "ipeHealth")
CONFIG_CNTS = ("mappingPolicy", "transferPolicy", "accessPolicyConfig",
               "pendingMappingProposal", "decisions", "input_example")


@dataclass
class ProvisionResult:
    ok: bool
    # (robot_id, interface, view) -> oneM2M 절대 경로 (Pipeline path_map)
    path_map: dict[tuple[str, str, str], str] = field(default_factory=dict)
    # path_key -> dict(kind, robot_id, interface, input_cnt_path)
    routes: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 상태 카테고리 -> CNT 절대 경로
    status_paths: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    fallbacks: list[str] = field(default_factory=list)   # FCNT→CNT 폴백 등 비치명 이탈
    # qos FCNT 생성 실패 → 해당 인터페이스는 lbl-only 모드 (§4.5.1)
    qos_fcnt_failed: list[dict[str, Any]] = field(default_factory=list)


class Provisioner:
    def __init__(self, rc: ResolvedConfig, ops: Any, state: Any, poa_base: str,
                 *, protocol: str = "http") -> None:
        self.rc = rc
        self.ops = ops
        self.state = state
        self.poa_base = poa_base.rstrip("/")
        self.protocol = protocol
        self._lock = threading.Lock()

    # ------------------------------------------------------------------

    def _ae_root(self) -> str:
        return f"/{self.rc.cse.cse_base}/{self.rc.cse.ae_name}"

    def _robot_root(self, ae: str, robot_id: str) -> str:
        robot = sanitize_segment(robot_id, self.rc.naming.get("sanitize", "_"))
        return f"{ae}/robots/{robot}"

    def _branch_root(self, ae: str, robot_id: str, branch: str) -> str:
        return f"{self._robot_root(ae, robot_id)}/{branch}"

    def ensure_ae_identity(self) -> str:
        """AE를 등록(또는 재사용)하고 aei를 영속화한다. aei는 절대 추측하지 않는다."""
        cse = self.rc.cse
        parent = f"/{cse.cse_base}"
        # MQTT는 AE에 mqtt POA를 등록해 CSE가 NOTIFY 토픽을 알게 한다(HTTP는 nu가 URL).
        poa = [self.poa_base] if self.protocol == "mqtt" else None
        path, aei = self.ops.ensure_ae(parent, cse.ae_name,
                                       api=f"N{self.rc.instance_id}", poa=poa)
        kv_key = f"aei:{cse.ae_name}"
        if aei:
            self.state.set_kv(kv_key, aei)
            return str(aei)
        stored = self.state.get_kv(kv_key)
        if stored:
            return str(stored)
        r = self.ops.retrieve(path)
        body = r.body if (r.ok and isinstance(r.body, dict)) else {}
        aei = body.get("m2m:ae", {}).get("aei")
        if not aei:
            raise RuntimeError(
                f"cannot determine aei for existing AE {path}; refusing to operate "
                f"with a guessed origin (DESIGN §15.2)")
        self.state.set_kv(kv_key, aei)
        return str(aei)

    def check_cse_identity(self) -> str | None:
        """CSEBase ct가 바뀌었으면 'restarted' 반환."""
        r = self.ops.retrieve(f"/{self.rc.cse.cse_base}")
        body = r.body if (r.ok and isinstance(r.body, dict)) else {}
        ct = body.get("m2m:cb", {}).get("ct")
        if not ct:
            return None
        prev = self.state.get_kv("csebase_ct")
        self.state.set_kv("csebase_ct", ct)
        if prev is not None and prev != ct:
            return "restarted"
        return None

    # ------------------------------------------------------------------

    def provision_all(self) -> ProvisionResult:
        with self._lock:
            return self._provision_all_locked()

    def _provision_all_locked(self) -> ProvisionResult:
        res = ProvisionResult(ok=True)
        rc = self.rc
        ae = self._ae_root()
        try:
            for name in IPE_ROOT_CNTS:
                self.ops.ensure_cnt(ae, name)
            for robot_id in rc.robots:
                robot_root = self.ops.ensure_cnt(f"{ae}/robots", sanitize_segment(
                    robot_id, rc.naming.get("sanitize", "_")))
                for name in ROBOT_CNTS:
                    self.ops.ensure_cnt(robot_root, name)
                for name in TOPIC_CNTS:
                    self.ops.ensure_cnt(f"{robot_root}/topics", name)
            for name in STATUS_CNTS:
                res.status_paths[name] = self.ops.ensure_cnt(f"{ae}/status", name)
            for name in CONFIG_CNTS:
                self.ops.ensure_cnt(f"{ae}/config", name)
            self._input_sub(
                res,
                f"{ae}/config/mappingPolicy",
                "qos_policy",
                "-",
                "qosMappingPolicy",
                "qosMappingPolicy",
            )
            # 확인 워크플로 입력 채널 — 결정 CIN이 이 CNT로 들어온다
            self._input_sub(res, f"{ae}/config/decisions", "decision", "-", "decisions",
                            "decisions")
        except Exception as e:
            res.ok = False
            res.errors.append(f"root provisioning failed: {e}")
            return res

        for t in rc.topics:
            if not t.msg_type:  # ambiguous/missing type는 다음 graph 세대까지 defer
                continue
            try:
                if t.direction in ("observe", "both"):
                    self._provision_observe(res, ae, t)
                if t.direction in ("command", "both") and t.access_enabled:
                    self._provision_command(res, ae, t)
            except Exception as e:
                res.errors.append(f"topic {t.interface}: {e}")
        for s in rc.services:
            if not s.srv_type or not s.access_enabled:
                continue
            try:
                self._provision_service(res, ae, s)
            except Exception as e:
                res.errors.append(f"service {s.interface}: {e}")
        for a in rc.actions:
            if not a.action_type or not a.access_enabled:
                continue
            try:
                self._provision_action(res, ae, a)
            except Exception as e:
                res.errors.append(f"action {a.interface}: {e}")
        if res.errors:
            res.ok = False
            log.warning("provisioning completed with %d error(s)", len(res.errors))
        return res

    # ------------------------------------------------------------------

    def _ensure_chain(self, root: str, rel_path: str, mni: int | None = None) -> str:
        parent = root
        segs = [s for s in rel_path.split("/") if s]
        for seg in segs[:-1]:
            parent = self.ops.ensure_cnt(parent, seg)
        return str(self.ops.ensure_cnt(parent, segs[-1], mni=mni))

    def _provision_observe(self, res: ProvisionResult, ae: str, t: Any) -> None:
        base = self._branch_root(ae, t.robot_id, "topics/observe")
        rep = t.representation
        from ipe.qos.history import container_attrs
        history_mni = container_attrs(t.qos_for("observe"),
            self.rc.policy.get("history_keep_all_limit", 1000))["mni"]
        if rep in ("latest",):
            if t.flexcontainer and self._try_fcnt_leaf(res, base, t):
                # FCNT 표현 토픽도 qos가 붙는다(T2: FCNT 자식 허용) — 3.5절 #2 해소
                node = res.path_map[(t.robot_id, t.interface, "fcnt")]
            else:
                node = self._ensure_chain(base, t.rel_path, mni=1)
                res.path_map[(t.robot_id, t.interface, "latest")] = node
        elif rep == "both":
            node = self._ensure_chain(base, t.rel_path)
            # CNT 이름에 latest/oldest/la/ol 금지 — tinyIoT 가상 리소스 예약어(rn invalid 405)
            if not (t.flexcontainer and self._try_fcnt_child(res, node, t)):
                res.path_map[(t.robot_id, t.interface, "latest")] = \
                    self.ops.ensure_cnt(node, "last", mni=1)
            res.path_map[(t.robot_id, t.interface, "history")] = \
                self.ops.ensure_cnt(node, "hist", mni=history_mni)
        else:   # historical | sampled
            node = self._ensure_chain(base, t.rel_path, mni=history_mni)
            res.path_map[(t.robot_id, t.interface, "history")] = node
        self._provision_qos_fcnt(
            res,
            node,
            t,
            "observe",
            self._data_refs(res, t.robot_id, t.interface, ("latest", "history", "fcnt")),
        )

    def _provision_qos_fcnt(self, res: ProvisionResult, node: str, t: Any,
                            direction: str,
                            data_resource_refs: list[dict[str, str]]) -> None:
        """인터페이스 노드 아래 rn='qos' FCNT — 배치 단일 규칙(§4.4).
        실패는 치명 아님: lbl-only 모드로 기록하고 계속 간다(§4.5.1)."""
        qf = self.rc.qos_fcnt
        if not qf.enabled:
            return
        from ipe.qos.codec import spec_to_fcnt_attrs
        attrs = spec_to_fcnt_attrs(
            direction=direction, interface=t.interface, robot_id=t.robot_id,
            configured=t.qos_for(direction), msg_type=t.msg_type,
            smode=(self.rc.policy.get("qos_strictness", "reject")
                   if direction == "observe" else None),
            data_resource_refs=data_resource_refs)
        attrs["lbl"] = ["Iwked-Technology:ROS2", "Iwked-Entity-Type:topic",
                        f"Iwked-Entity-ID:{t.interface}"]
        view = "qosObserve" if direction == "observe" else "qosCommand"
        try:
            path = self.ops.ensure_fcnt(node, "qos", qf.cnd, qf.type, attrs)
        except Exception as e:
            res.qos_fcnt_failed.append({"robot": t.robot_id, "interface": t.interface,
                                        "direction": direction, "error": str(e)})
            return
        res.path_map[(t.robot_id, t.interface, view)] = path
        if qf.allow_update:
            self._input_sub(res, path, "qos_update", t.robot_id, t.interface,
                            f"{direction}/{t.rel_path}", net=[1])
            res.routes[f"qos_update/{t.robot_id}/{direction}/{t.rel_path}"][
                "direction"] = direction

    def _try_fcnt_leaf(self, res: ProvisionResult, base: str, t: Any) -> bool:
        segs = [x for x in t.rel_path.split("/") if x]
        parent = base
        for seg in segs[:-1]:
            parent = self.ops.ensure_cnt(parent, seg)
        return self._try_fcnt(res, parent, segs[-1], t)

    def _try_fcnt_child(self, res: ProvisionResult, parent: str, t: Any) -> bool:
        return self._try_fcnt(res, parent, "state", t)

    def _try_fcnt(self, res: ProvisionResult, parent: str, name: str, t: Any) -> bool:
        """FCNT 생성 시도. 스키마 미등록(501) 등 실패는 CNT 폴백으로 — §9.2 게이트
        조건 2·3의 런타임 판정이다."""
        fc = t.flexcontainer
        try:
            path = self.ops.ensure_fcnt(parent, name, fc["cnd"], fc["type"])
        except Exception as e:
            res.fallbacks.append(
                f"{t.interface}: FCNT({fc['type']}) unavailable, CNT fallback — {e}")
            return False
        res.path_map[(t.robot_id, t.interface, "fcnt")] = path
        return True

    def _input_sub(self, res: ProvisionResult, cnt_path: str, kind: str,
                   robot_id: str, interface: str, rel_path: str,
                   net: list[int] | None = None) -> None:
        path_key = f"{kind}/{robot_id}/{rel_path}"
        # mqtt: 모든 SUB가 단일 POA URI를 nu로 공유, 경로 구분은 sur (app이 별칭 등록)
        nu = (self.poa_base if self.protocol == "mqtt"
              else f"{self.poa_base}/notify/{path_key}")
        # qos FCNT SUB에 atr 필터 금지(T9) — net만 제어한다
        sub = self.ops.ensure_sub(cnt_path, "ipeSub", [nu],
                                  net=net if net is not None else [3], nct=1)
        if not sub.ok:
            res.errors.append(f"SUB on {cnt_path} not active: {sub.detail}")
        res.routes[path_key] = {"kind": kind, "robot_id": robot_id,
                                "interface": interface, "input_cnt_path": cnt_path,
                                "sub_ri": sub.ri}

    def _provision_command(self, res: ProvisionResult, ae: str, t: Any) -> None:
        # 명령 CIN은 topic CNT에 직접 생성된다. SUB도 같은 CNT를 감시한다.
        parent = self._ensure_chain(
            self._branch_root(ae, t.robot_id, "topics/command"), t.rel_path)
        res.path_map[(t.robot_id, t.interface, "command")] = parent
        self._input_sub(res, parent, "command", t.robot_id, t.interface, t.rel_path)
        self._provision_qos_fcnt(
            res,
            parent,
            t,
            "command",
            self._data_refs(res, t.robot_id, t.interface, ("command",)),
        )

    def _provision_service(self, res: ProvisionResult, ae: str, s: Any) -> None:
        parent = self._ensure_chain(
            self._branch_root(ae, s.robot_id, "services"), s.rel_path)
        req = self.ops.ensure_cnt(parent, "request")
        response = self.ops.ensure_cnt(parent, "response")
        res.path_map[(s.robot_id, s.interface, "request")] = req
        res.path_map[(s.robot_id, s.interface, "response")] = response
        self._input_sub(res, req, "service", s.robot_id, s.interface, s.rel_path)
        self._provision_static_qos_fcnt(
            res,
            parent,
            s,
            "service",
            {"request": s.qos, "response": s.qos},
            self._data_refs(res, s.robot_id, s.interface, ("request", "response")),
        )

    def _provision_action(self, res: ProvisionResult, ae: str, a: Any) -> None:
        parent = self._ensure_chain(
            self._branch_root(ae, a.robot_id, "actions"), a.rel_path)
        goal = self.ops.ensure_cnt(parent, "goal")
        feedback = self.ops.ensure_cnt(parent, "feedback")
        result = self.ops.ensure_cnt(parent, "result")
        cancel = self.ops.ensure_cnt(parent, "cancel")
        action_status = self.ops.ensure_cnt(parent, "actionStatus")
        for view, path in (
            ("goal", goal),
            ("feedback", feedback),
            ("result", result),
            ("cancel", cancel),
            ("actionStatus", action_status),
        ):
            res.path_map[(a.robot_id, a.interface, view)] = path
        self._input_sub(res, goal, "action_goal", a.robot_id, a.interface, a.rel_path)
        self._input_sub(res, cancel, "cancel", a.robot_id, a.interface, a.rel_path)
        from ipe.models import ACTION_QOS_CHANNELS
        self._provision_static_qos_fcnt(
            res,
            parent,
            a,
            "action",
            {channel: a.qos.get(channel) for channel in ACTION_QOS_CHANNELS},
            self._data_refs(
                res,
                a.robot_id,
                a.interface,
                ("goal", "feedback", "result", "cancel", "actionStatus"),
            ),
        )

    def _provision_static_qos_fcnt(
        self,
        res: ProvisionResult,
        parent: str,
        spec: Any,
        interface_kind: str,
        channels: dict[str, Any],
        data_resource_refs: list[dict[str, str]],
    ) -> None:
        """Create one management FCNT for a logical service or action interface."""
        if not self.rc.qos_fcnt.enabled:
            return
        from ipe.qos.codec import interface_qos_fcnt_attrs
        attrs = interface_qos_fcnt_attrs(
            interface_kind=interface_kind,
            interface=spec.interface,
            robot_id=spec.robot_id,
            channels=channels,
            msg_type=getattr(spec, "srv_type", None) or getattr(spec, "action_type", None),
            data_resource_refs=data_resource_refs,
        )
        attrs["lbl"] = ["Iwked-Technology:ROS2", f"Iwked-Entity-Type:{interface_kind}",
                        f"Iwked-Entity-ID:{spec.interface}"]
        try:
            fcnt_type, cnd = self.rc.qos_fcnt.specialization(interface_kind)
            path = self.ops.ensure_fcnt(
                parent,
                "qos",
                cnd,
                fcnt_type,
                attrs,
            )
        except Exception as exc:
            res.qos_fcnt_failed.append({
                "robot": spec.robot_id,
                "interface": spec.interface,
                "direction": interface_kind,
                "error": str(exc),
            })
            return
        view = "qosService" if interface_kind == "service" else "qosAction"
        res.path_map[(spec.robot_id, spec.interface, view)] = path

    @staticmethod
    def _data_refs(
        res: ProvisionResult,
        robot_id: str,
        interface: str,
        views: tuple[str, ...],
    ) -> list[dict[str, str]]:
        """Build stable role-to-resource references for the management FCNT."""
        return [
            {"role": view, "path": res.path_map[(robot_id, interface, view)]}
            for view in views
            if (robot_id, interface, view) in res.path_map
        ]

    # ------------------------------------------------------------------

    def mark_availability(self, rel_branch: str, available: bool, last_seen: str) -> None:
        """가용성 lbl만 갱신한다 — 리소스는 보존."""
        path = f"{self._ae_root()}/{rel_branch}"
        try:
            self.ops.update_lbl(path, [f"ipe:available={'true' if available else 'false'}",
                                       f"ipe:lastSeen={last_seen}"])
        except Exception as e:
            log.warning("availability label update failed for %s: %s", path, e)

    def remove_interface(self, kind: str, spec: Any) -> list[str]:
        """활성 generation에서 빠진 interface subtree를 제거한다.

        호출자는 새 generation을 먼저 활성화해야 한다(make-before-break).
        """
        ae = self._ae_root()
        branches: list[str]
        if kind == "topic":
            branches = []
            if spec.direction in ("observe", "both"):
                branches.append("topics/observe")
            if spec.direction in ("command", "both"):
                branches.append("topics/command")
        elif kind == "service":
            branches = ["services"]
        elif kind == "action":
            branches = ["actions"]
        else:
            raise ValueError(f"unknown interface kind: {kind}")

        removed: list[str] = []
        for branch in branches:
            path = f"{self._branch_root(ae, spec.robot_id, branch)}/{spec.rel_path}"
            response = self.ops.delete_resource(path)
            if response.ok or response.status == 404:
                removed.append(path)
            else:
                log.warning("resource removal failed for %s: status=%s rsc=%s",
                            path, response.status, response.rsc)
        return removed
