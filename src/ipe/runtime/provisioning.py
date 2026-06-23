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

from ipe.config.spec import ResolvedConfig

log = logging.getLogger(__name__)

ROOT_CNTS = ("ros2Data", "ros2Command", "services", "actions", "status", "config")
STATUS_CNTS = ("topicHealth", "nodeStatus", "qosStatus", "commandStatus",
               "serviceStatus", "actionStatus", "provisioningStatus", "ipeHealth")
CONFIG_CNTS = ("mappingPolicy", "transferPolicy", "qosMappingPolicy",
               "accessPolicyConfig", "pendingMappingProposal", "decisions", "input_example")


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
            return aei
        stored = self.state.get_kv(kv_key)
        if stored:
            return stored
        r = self.ops.retrieve(path)
        body = r.body if (r.ok and isinstance(r.body, dict)) else {}
        aei = body.get("m2m:ae", {}).get("aei")
        if not aei:
            raise RuntimeError(
                f"cannot determine aei for existing AE {path}; refusing to operate "
                f"with a guessed origin (DESIGN §15.2)")
        self.state.set_kv(kv_key, aei)
        return aei

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
            for name in ROOT_CNTS:
                self.ops.ensure_cnt(ae, name)
            for name in STATUS_CNTS:
                res.status_paths[name] = self.ops.ensure_cnt(f"{ae}/status", name)
            for name in CONFIG_CNTS:
                self.ops.ensure_cnt(f"{ae}/config", name)
            # 확인 워크플로 입력 채널 — 결정 CIN이 이 CNT로 들어온다
            self._input_sub(res, f"{ae}/config/decisions", "decision", "-", "decisions",
                            "decisions")
        except Exception as e:
            res.ok = False
            res.errors.append(f"root provisioning failed: {e}")
            return res

        for t in rc.topics:
            try:
                if t.direction in ("observe", "both"):
                    self._provision_observe(res, ae, t)
                if t.direction in ("command", "both") and t.access_enabled:
                    self._provision_command(res, ae, t)
            except Exception as e:
                res.errors.append(f"topic {t.interface}: {e}")
        for s in rc.services:
            try:
                self._provision_service(res, ae, s)
            except Exception as e:
                res.errors.append(f"service {s.interface}: {e}")
        for a in rc.actions:
            try:
                self._provision_action(res, ae, a)
            except Exception as e:
                res.errors.append(f"action {a.interface}: {e}")
        if res.errors:
            log.warning("provisioning completed with %d error(s)", len(res.errors))
        return res

    # ------------------------------------------------------------------

    def _ensure_chain(self, root: str, rel_path: str, mni: int | None = None) -> str:
        parent = root
        segs = [s for s in rel_path.split("/") if s]
        for seg in segs[:-1]:
            parent = self.ops.ensure_cnt(parent, seg)
        return self.ops.ensure_cnt(parent, segs[-1], mni=mni)

    def _provision_observe(self, res: ProvisionResult, ae: str, t: Any) -> None:
        base = f"{ae}/ros2Data"
        rep = t.representation
        if rep in ("latest",):
            if t.flexcontainer and self._try_fcnt_leaf(res, base, t):
                return
            path = self._ensure_chain(base, t.rel_path, mni=1)
            res.path_map[(t.robot_id, t.interface, "latest")] = path
        elif rep == "both":
            parent = self._ensure_chain(base, t.rel_path)
            # CNT 이름에 latest/oldest/la/ol 금지 — tinyIoT 가상 리소스 예약어(rn invalid 405)
            if not (t.flexcontainer and self._try_fcnt_child(res, parent, t)):
                res.path_map[(t.robot_id, t.interface, "latest")] = \
                    self.ops.ensure_cnt(parent, "last", mni=1)
            res.path_map[(t.robot_id, t.interface, "history")] = \
                self.ops.ensure_cnt(parent, "hist")
        else:   # historical | sampled
            path = self._ensure_chain(base, t.rel_path)
            res.path_map[(t.robot_id, t.interface, "history")] = path

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
                   robot_id: str, interface: str, rel_path: str) -> None:
        path_key = f"{kind}/{robot_id}/{rel_path}"
        # mqtt: 모든 SUB가 단일 POA URI를 nu로 공유, 경로 구분은 sur (app이 별칭 등록)
        nu = (self.poa_base if self.protocol == "mqtt"
              else f"{self.poa_base}/notify/{path_key}")
        sub = self.ops.ensure_sub(cnt_path, "ipeSub", [nu], net=[3], nct=1)
        if not sub.ok:
            res.errors.append(f"SUB on {cnt_path} not active: {sub.detail}")
        res.routes[path_key] = {"kind": kind, "robot_id": robot_id,
                                "interface": interface, "input_cnt_path": cnt_path,
                                "sub_ri": sub.ri}

    def _provision_command(self, res: ProvisionResult, ae: str, t: Any) -> None:
        parent = self._ensure_chain(f"{ae}/ros2Command", t.rel_path)
        req = self.ops.ensure_cnt(parent, "publishRequest")
        self.ops.ensure_cnt(parent, "publishStatus")
        res.path_map[(t.robot_id, t.interface, "publishStatus")] = f"{parent}/publishStatus"
        self._input_sub(res, req, "command", t.robot_id, t.interface, t.rel_path)

    def _provision_service(self, res: ProvisionResult, ae: str, s: Any) -> None:
        parent = self._ensure_chain(f"{ae}/services", s.rel_path)
        req = self.ops.ensure_cnt(parent, "request")
        self.ops.ensure_cnt(parent, "response")
        self.ops.ensure_cnt(parent, "invocationStatus")
        res.path_map[(s.robot_id, s.interface, "response")] = f"{parent}/response"
        res.path_map[(s.robot_id, s.interface, "invocationStatus")] = f"{parent}/invocationStatus"
        self._input_sub(res, req, "service", s.robot_id, s.interface, s.rel_path)

    def _provision_action(self, res: ProvisionResult, ae: str, a: Any) -> None:
        parent = self._ensure_chain(f"{ae}/actions", a.rel_path)
        goal = self.ops.ensure_cnt(parent, "goal")
        self.ops.ensure_cnt(parent, "feedback")
        self.ops.ensure_cnt(parent, "result")
        cancel = self.ops.ensure_cnt(parent, "cancel")
        self.ops.ensure_cnt(parent, "actionStatus")
        for view in ("feedback", "result", "actionStatus"):
            res.path_map[(a.robot_id, a.interface, view)] = f"{parent}/{view}"
        self._input_sub(res, goal, "action_goal", a.robot_id, a.interface, a.rel_path)
        self._input_sub(res, cancel, "cancel", a.robot_id, a.interface, a.rel_path)

    # ------------------------------------------------------------------

    def mark_availability(self, rel_branch: str, available: bool, last_seen: str) -> None:
        """가용성 lbl만 갱신한다 — 리소스는 보존."""
        path = f"{self._ae_root()}/{rel_branch}"
        try:
            self.ops.update_lbl(path, [f"ipe:available={'true' if available else 'false'}",
                                       f"ipe:lastSeen={last_seen}"])
        except Exception as e:
            log.warning("availability label update failed for %s: %s", path, e)
