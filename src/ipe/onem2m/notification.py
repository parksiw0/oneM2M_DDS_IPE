"""oneM2M 알림 파싱 (DESIGN §14.2/§14.3).

CIN 추출은 엄격한 경로 기반: ``m2m:sgn.nev.rep["m2m:cin"]``.
rep 값에 키 추측 휴리스틱을 쓰지 않는다 — ``m2m:cin`` 키 아래가 아니면 무시.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Notification:
    """파싱된 ``m2m:sgn``.

    ``vrq``가 True면 ``raw``를 뺀 모든 필드가 비어 있다: 검증 요청은
    내용 파싱 전에 응답하므로 깨진 ``nev``/``rep``가 예외를 일으킬 수 없다.
    """

    vrq: bool
    sur: str | None
    net: int | None
    cr: str | None
    cin_ri: str | None
    cin_ct: str | None
    con: dict[str, Any] | None
    raw: dict[str, Any]


def _parse_con(con: Any) -> dict[str, Any] | None:
    """CIN con을 dict로, 안 되면 None.

    tinyIoT는 con을 JSON 문자열로 저장한다. JSON 객체로 디코드되지 않는
    문자열은 None — 원문은 진단용으로 ``raw``에 남고, 상관관계 추출에는
    객체 키가 필요하다.
    """
    if isinstance(con, dict):
        return con
    if isinstance(con, str):
        try:
            parsed = json.loads(con)
        except (ValueError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def parse_notification(body: Any) -> Notification | None:
    """``m2m:sgn`` 본문을 파싱한다. 아니면 None."""
    sgn = body.get("m2m:sgn") if isinstance(body, dict) else None
    if not isinstance(sgn, dict):
        return None
    # vrq 감지가 다른 필드 파싱보다 먼저다 — 깨진 nev/rep에도 ack 보장
    if sgn.get("vrq") is True:
        return Notification(vrq=True, sur=None, net=None, cr=None,
                            cin_ri=None, cin_ct=None, con=None, raw=body)
    nev = sgn.get("nev")
    if not isinstance(nev, dict):
        nev = {}
    rep = nev.get("rep")
    cin = rep.get("m2m:cin") if isinstance(rep, dict) else None
    if not isinstance(cin, dict):
        cin = {}
    return Notification(
        vrq=False,
        sur=sgn.get("sur"),
        net=nev.get("net"),
        cr=sgn.get("cr"),
        cin_ri=cin.get("ri"),
        cin_ct=cin.get("ct"),
        con=_parse_con(cin.get("con")),
        raw=body,
    )
