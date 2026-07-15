"""HTTP 클라이언트 위의 oneM2M 리소스 연산 (DESIGN §15).

멱등성 계약: 재시도되는 CREATE는 결정적 resourceName을 쓰고 409/4105(rn 중복)를
성공으로 취급한다. AE 식별은 origin 변형 탐색 없이 푼다(201→aei, 409→None을
받아 호출자가 저장된 aei를 읽거나 CAdmin으로 GET).
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Any

from ipe.onem2m.client import (
    OneM2MClient,
    OneM2MResponse,
    OversizeError,
    TransportError,
    classify,
)

log = logging.getLogger(__name__)

TY_AE = 2
TY_CNT = 3
TY_CIN = 4
TY_SUB = 23
TY_FCNT = 28


class ResourceOpsError(Exception):
    pass


def _is_duplicate(r: OneM2MResponse) -> bool:
    """409 / RSC 4105 = 같은 rn의 리소스가 이미 존재."""
    return r.status == 409 or r.rsc == 4105


@dataclass(frozen=True)
class CinResult:
    """create_cin의 결과. 예외로 던지지 않는다 — 워커가 response로 실패를
    분류한다(classify(result.response))."""

    created: bool            # 이번 호출에서 201
    duplicate: bool          # 결정적 rn 재전송이 성공으로 풀린 경우
    response: OneM2MResponse

    @property
    def ok(self) -> bool:
        return self.created or self.duplicate

    @property
    def status(self) -> int:
        return self.response.status

    @property
    def rsc(self) -> int | None:
        return self.response.rsc


@dataclass(frozen=True)
class SubStatus:
    """ensure_sub의 결과 — 예외 대신 반환해서 호출자가 provisioningStatus
    이벤트를 낼 수 있게 한다(실패는 409 스킵이 아니다)."""

    path: str
    ok: bool                 # SUB 존재 + 생성 후 읽기 성공
    created: bool            # 이번 호출에서 201 (기존재 시 False)
    verified: bool           # 생성 후 GET이 SUB를 반환
    detail: str = ""
    response: OneM2MResponse | None = None   # 마지막 유효 응답 (create 또는 GET)
    classification: str | None = None        # 실패의 classify() 결과, ok면 None
    ri: str | None = None                    # 생성된 SUB의 resourceId (MQTT sur 보조 별칭)


class ResourceOps:
    def __init__(self, client: OneM2MClient) -> None:
        self.client = client

    # -- AE ------------------------------------------------------------------

    def ensure_ae(self, parent: str, name: str, api: str | None = None,
                  poa: list[str] | None = None) -> tuple[str, str | None]:
        """AE를 생성한다. origin 변형 탐색은 하지 않는다.

        (path, aei) 반환: 201이면 본문의 aei, 409면 None(호출자가 저장된
        aei를 읽거나 CAdmin으로 GET). aei는 본문에 절대 넣지 않는다 —
        tinyIoT가 origin에서 유도한다.

        poa가 주어지면 AE에 pointOfAccess로 등록한다(MQTT 바인딩: CSE가 NOTIFY를
        보낼 mqtt:// 토픽 대상). HTTP는 nu가 URL을 직접 실으므로 poa를 쓰지 않는다.
        """
        if api is None:
            api = f"N{name}"
        ae_body: dict[str, Any] = {"rn": name, "api": api, "rr": True, "srv": ["3"]}
        if poa:
            ae_body["poa"] = poa
        body = {"m2m:ae": ae_body}
        r = self.client.create(parent, TY_AE, body)
        path = f"{parent.rstrip('/')}/{name}"
        if r.status == 201:
            aei = r.body.get("m2m:ae", {}).get("aei") if isinstance(r.body, dict) else None
            if not aei:
                raise ResourceOpsError(
                    f"AE created at {path} but aei missing in response body={r.body!r}"
                )
            log.info("CREATED AE %s (aei=%s)", path, aei)
            return path, aei
        if _is_duplicate(r):
            log.info("EXISTS  AE %s — aei unknown (load from KV or GET as CAdmin)", path)
            return path, None
        raise ResourceOpsError(
            f"Failed to ensure AE at {path}: HTTP {r.status} rsc={r.rsc} body={r.body!r}"
        )

    # -- CNT / FCNT ----------------------------------------------------------

    def ensure_cnt(
        self,
        parent: str,
        name: str,
        mni: int | None = None,
        lbl: list[str] | None = None,
    ) -> str:
        """컨테이너 생성 또는 스킵. mni는 주어질 때만 본문에 넣는다
        (최신 상태 CNT는 mni=1); tinyIoT 기본값은 1000."""
        attrs: dict[str, Any] = {"rn": name}
        if mni is not None:
            attrs["mni"] = mni
        if lbl is not None:
            attrs["lbl"] = lbl
        r = self.client.create(parent, TY_CNT, {"m2m:cnt": attrs})
        path = f"{parent.rstrip('/')}/{name}"
        return self._check(r, path, "CNT")

    def ensure_fcnt(
        self,
        parent: str,
        name: str,
        cnd: str,
        fcnt_type: str,
        initial_attrs: dict[str, Any] | None = None,
    ) -> str:
        """FCNT(선택): mni/mbs/mia를 보내면 안 된다 — rvi < 4 요청에서
        CSE가 NOT_IMPLEMENTED로 거부한다."""
        body: dict[str, Any] = {"rn": name, "cnd": cnd}
        if initial_attrs:
            body.update(initial_attrs)
        r = self.client.create(parent, TY_FCNT, {fcnt_type: body})
        path = f"{parent.rstrip('/')}/{name}"
        return self._check(r, path, f"FCNT ({fcnt_type})")

    def update_fcnt(self, path: str, content: dict[str, Any]) -> OneM2MResponse:
        return self.client.update(path, content)

    # -- CIN -----------------------------------------------------------------

    def create_cin(
        self,
        parent: str,
        con: dict[str, Any],
        rn: str | None = None,
        lbl: list[str] | None = None,
    ) -> CinResult:
        """contentInstance CREATE. con은 JSON 문자열로 직렬화한다
        (tinyIoT는 문자열 con에만 cs를 계산).

        결정적 rn이 있으면 409/4105 응답은 재시도 재전송이라 duplicate
        성공으로 반환한다. rn이 없으면 409는 그냥 실패. 2xx가 아닌 결과는
        던지지 않고 반환해 워커가 분류한다.
        """
        attrs: dict[str, Any] = {"con": json.dumps(con, ensure_ascii=False)}
        if rn is not None:
            attrs["rn"] = rn
        if lbl is not None:
            attrs["lbl"] = lbl
        r = self.client.create(parent, TY_CIN, {"m2m:cin": attrs})
        if r.status == 201:
            return CinResult(created=True, duplicate=False, response=r)
        if rn is not None and _is_duplicate(r):
            log.debug("CIN %s/%s duplicate rn — idempotent success", parent, rn)
            return CinResult(created=False, duplicate=True, response=r)
        return CinResult(created=False, duplicate=False, response=r)

    # -- SUB -----------------------------------------------------------------

    def ensure_sub(
        self,
        parent: str,
        name: str,
        nu: list[str],
        net: list[int] | None = None,
        nct: int = 1,
    ) -> SubStatus:
        """구독을 생성하고 생성 후 읽기로 확인한다. 예외 대신 SubStatus를
        반환해 프로비저닝이 실패를 provisioningStatus 이벤트로 보고한다.

        nct=1(전체 리소스)이 알림의 con 전달을 보장한다. nu에는 포트를
        명시해야 한다 — tinyIoT의 기본 포트 처리 코드는 동작하지 않는다.
        """
        body = {
            "m2m:sub": {
                "rn": name,
                "nu": nu,
                "enc": {"net": net if net is not None else [3]},
                "nct": nct,
            }
        }
        path = f"{parent.rstrip('/')}/{name}"
        try:
            r = self.client.create(parent, TY_SUB, body)
        except (TransportError, OversizeError) as e:
            return SubStatus(
                path=path, ok=False, created=False, verified=False,
                detail=f"create failed: {e}", classification=classify(e),
            )

        if r.status == 201:
            created = True
        elif _is_duplicate(r):
            created = False
        else:
            # 예: 5204 vrq 검증 실패가 HTTP 500으로 매핑되는 경우
            return SubStatus(
                path=path, ok=False, created=False, verified=False,
                detail=f"create rejected: HTTP {r.status} rsc={r.rsc} body={r.body!r}",
                response=r, classification=classify(r),
            )

        try:
            probe = self.client.get(path)
        except (TransportError, OversizeError) as e:
            return SubStatus(
                path=path, ok=False, created=created, verified=False,
                detail=f"post-create read failed: {e}", classification=classify(e),
            )
        if probe.ok:
            # 검증은 존재가 아니라 속성 일치 — 잔존 SUB의 nu가 다르면 알림이
            # 미등록 라우트로 떨어져 무음 손실된다
            existing = (probe.body or {}).get("m2m:sub", {}) if isinstance(probe.body, dict) else {}
            if not created and sorted(existing.get("nu") or []) != sorted(nu):
                log.warning("SUB %s nu mismatch (%s != %s) — recreating",
                            path, existing.get("nu"), nu)
                try:
                    self.client.delete(path)
                    r2 = self.client.create(parent, TY_SUB, body)
                except (TransportError, OversizeError) as e:
                    return SubStatus(path=path, ok=False, created=False, verified=False,
                                     detail=f"recreate failed: {e}",
                                     classification=classify(e))
                if r2.status != 201:
                    return SubStatus(path=path, ok=False, created=False, verified=False,
                                     detail=f"recreate rejected: HTTP {r2.status} rsc={r2.rsc}",
                                     response=r2, classification=classify(r2))
                created = True
            log.info("%s SUB %s (verified)", "CREATED" if created else "EXISTS ", path)
            sub_ri = existing.get("ri") if isinstance(existing, dict) else None
            return SubStatus(path=path, ok=True, created=created, verified=True,
                             response=probe, ri=sub_ri)
        return SubStatus(
            path=path, ok=False, created=created, verified=False,
            detail=f"post-create read: HTTP {probe.status} rsc={probe.rsc}",
            response=probe, classification=classify(probe),
        )

    # -- 기타 ----------------------------------------------------------------

    @staticmethod
    def _lbl_key(label: str) -> str:
        """'k=v' 라벨의 병합 키('k='). '='가 없는 라벨은 라벨 전체가 키다."""
        head, sep, _ = label.partition("=")
        return head + sep

    def update_lbl(
        self, path: str, labels: list[str], ty_key: str = "m2m:cnt"
    ) -> OneM2MResponse:
        """lbl을 키 단위 병합으로 UPDATE — 같은 'k=' 라벨만 교체, 나머지 보존.

        oneM2M UPDATE는 lbl 전체를 교체하므로 통짜 전송은 qos:*와
        ipe:available=*이 서로를 지우는 경합이 된다(설계서 3.5절 #3).
        루트 키는 RETRIEVE 응답에서 감지한다(FCNT 특화 키 포함).
        """
        existing: list[str] = []
        try:
            r = self.client.get(path)
        except (TransportError, OversizeError):
            r = None
        if r is not None and r.ok and isinstance(r.body, dict) and r.body:
            ty_key = next(iter(r.body))
            inner = r.body.get(ty_key)
            if isinstance(inner, dict) and isinstance(inner.get("lbl"), list):
                existing = [x for x in inner["lbl"] if isinstance(x, str)]
        new_keys = {self._lbl_key(lb) for lb in labels}
        merged = [lb for lb in existing if self._lbl_key(lb) not in new_keys] + labels
        return self.client.update(path, {ty_key: {"lbl": merged}})

    def retrieve(self, path: str) -> OneM2MResponse:
        return self.client.get(path)

    def list_child_cins(self, parent: str, limit: int | None = None) -> list[dict[str, Any]]:
        """캐치업 스윕용으로 컨테이너의 자식 CIN을 RETRIEVE한다.

        ct 오름차순(== CSE 생성 순서)으로 [{ri, ct, con}, ...]를 반환한다.
        JSON 문자열 con은 객체로 되돌린다.
        """
        params: dict[str, Any] = {"rcn": 4}  # 속성 + 자식 리소스
        if limit is not None:
            params["lim"] = limit
        r = self.client.get(parent, params=params)
        if not r.ok:
            raise ResourceOpsError(
                f"list_child_cins({parent}) failed: HTTP {r.status} rsc={r.rsc} body={r.body!r}"
            )
        if not isinstance(r.body, dict):
            return []
        cnt = r.body.get("m2m:cnt")
        if not isinstance(cnt, dict):
            return []
        raw = cnt.get("m2m:cin", [])
        if isinstance(raw, dict):  # 자식이 하나면 리스트로 안 싸여 올 수 있다
            raw = [raw]
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for cin in raw:
            if not isinstance(cin, dict):
                continue
            con = cin.get("con")
            if isinstance(con, str):
                with contextlib.suppress(ValueError, TypeError):
                    con = json.loads(con)  # JSON이 아니면 원문 문자열 유지
            out.append({"ri": cin.get("ri"), "ct": cin.get("ct"), "con": con})
        out.sort(key=lambda c: c["ct"] or "")
        return out

    def delete_resource(self, path: str) -> OneM2MResponse:
        return self.client.delete(path)

    # -- 내부 --------------------------------------------------------------

    def _check(self, r: OneM2MResponse, path: str, kind: str) -> str:
        if r.status == 201:
            log.info("CREATED %s %s", kind, path)
            return path
        if _is_duplicate(r):
            log.info("EXISTS  %s %s — skip", kind, path)
            return path
        raise ResourceOpsError(
            f"Failed to ensure {kind} at {path}: HTTP {r.status} rsc={r.rsc} body={r.body!r}"
        )
