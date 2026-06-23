"""oneM2M HTTP 바인딩 클라이언트 (tinyIoT 전제, DESIGN §15).

단일 시도 전송 계층: 모든 메서드는 HTTP 교환을 정확히 한 번 수행한다.
재시도/백오프/스풀 판단은 client.py의 classify()와 backoff_delays()를 쓰는
oneM2M 클라이언트 워커 몫이다.

전송-중립 타입/함수(OneM2MResponse·classify·backoff_delays·OversizeError·
TransportError·DEFAULT_MAX_PAYLOAD)는 client.py가 정본이며, 여기서 재-export해
기존 import 경로(`from ipe.onem2m.http_client import ...`)를 보존한다.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Mapping
from typing import Any

import requests

# 전송-중립 계약을 정본(client.py)에서 가져와 재-export한다.
from ipe.onem2m.client import (  # noqa: F401  (re-export)
    DEFAULT_MAX_PAYLOAD,
    NON_RECOVERABLE,
    POLICY_DEPENDENT,
    RECOVERABLE,
    Classification,
    OneM2MResponse,
    OversizeError,
    TransportError,
    backoff_delays,
    classify,
)

log = logging.getLogger(__name__)


def _parse_rsc(headers: Mapping[str, str]) -> int | None:
    value = headers.get("X-M2M-RSC")
    if value is None:  # 테스트/목의 일반 dict 헤더는 대소문자를 구분한다
        value = headers.get("x-m2m-rsc")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class OneM2MHTTPClient:
    """단일 시도 HTTP 바인딩. origin은 필수(기본값 없음).

    주소는 CSE 상대 구조 경로("/<CSE_BASE_NAME>/...") — 표준 SP 상대
    표기와 다른 tinyIoT 관용 형식이다. 연결/타임아웃 등 requests 예외는
    transport-중립 TransportError(recoverable=True)로 감싸 올린다.
    """

    def __init__(
        self,
        endpoint: str,
        origin: str,
        *,
        rvi: str = "3",
        timeout: float = 5.0,
        max_payload: int = DEFAULT_MAX_PAYLOAD,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.origin = origin
        self.rvi = rvi
        self.timeout = timeout
        self.max_payload = max_payload
        self.session = requests.Session()

    # 연결 생명주기 훅 — HTTP는 세션이 지연 연결이라 no-op (OneM2MClient 계약 충족).
    def start(self) -> None:
        return None

    def stop(self) -> None:
        self.session.close()

    def _headers(self, ty: int | None, accept: str = "application/json") -> dict[str, str]:
        h = {
            "X-M2M-Origin": self.origin,
            "X-M2M-RI": str(uuid.uuid4())[:12],
            "X-M2M-RVI": self.rvi,
            "Accept": accept,
        }
        if ty is not None:
            # ty 누락 시 tinyIoT가 POST를 NOTIFY로 조용히 재분류한다
            h["Content-Type"] = f"application/json;ty={ty}"
        else:
            h["Content-Type"] = "application/json"
        return h

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.endpoint + ("/" + path.lstrip("/"))

    def _serialize(self, body: dict[str, Any]) -> str:
        # ensure_ascii=False: 비ASCII 페이로드의 전송 크기를 절반으로 줄인다 —
        # CSE의 65536바이트 요청 예산에 직결.
        data = json.dumps(body, ensure_ascii=False)
        size = len(data.encode("utf-8"))
        if size > self.max_payload:
            raise OversizeError(size, self.max_payload)
        return data

    def _parse(self, r: requests.Response) -> OneM2MResponse:
        body: Any = None
        if r.text:
            try:
                body = r.json()
            except json.JSONDecodeError:
                body = r.text
        return OneM2MResponse(
            status=r.status_code,
            body=body,
            headers=dict(r.headers),
            rsc=_parse_rsc(r.headers),
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> OneM2MResponse:
        try:
            r = self.session.get(
                self._url(path),
                headers=self._headers(None),
                params=params,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            raise TransportError(str(e)) from e
        return self._parse(r)

    def create(self, path: str, ty: int, body: dict[str, Any]) -> OneM2MResponse:
        data = self._serialize(body)  # OversizeError는 감싸지 않고 그대로 전파
        try:
            r = self.session.post(
                self._url(path),
                headers=self._headers(ty),
                data=data,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            raise TransportError(str(e)) from e
        return self._parse(r)

    def update(self, path: str, body: dict[str, Any]) -> OneM2MResponse:
        data = self._serialize(body)
        try:
            r = self.session.put(
                self._url(path),
                headers=self._headers(None),
                data=data,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            raise TransportError(str(e)) from e
        return self._parse(r)

    def delete(self, path: str) -> OneM2MResponse:
        try:
            r = self.session.delete(
                self._url(path), headers=self._headers(None), timeout=self.timeout
            )
        except requests.exceptions.RequestException as e:
            raise TransportError(str(e)) from e
        return self._parse(r)
