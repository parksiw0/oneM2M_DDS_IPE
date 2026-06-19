"""oneM2M HTTP 바인딩 클라이언트 (tinyIoT 전제, DESIGN §15).

단일 시도 전송 계층: 모든 메서드는 HTTP 교환을 정확히 한 번 수행한다.
재시도/백오프/스풀 판단은 이 모듈의 classify()와 backoff_delays()를 쓰는
oneM2M 클라이언트 워커 몫이다.
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import requests

log = logging.getLogger(__name__)

# 복구 가능성 분류
Classification = Literal["recoverable", "non_recoverable", "policy_dependent"]
RECOVERABLE: Classification = "recoverable"
NON_RECOVERABLE: Classification = "non_recoverable"
POLICY_DEPENDENT: Classification = "policy_dependent"

# 재시도/드롭 대신 reconcile을 일으키는 RSC/HTTP 코드:
# 4004 부모 소실, 4105 rn 중복(멱등 성공), 4103 권한 없음.
_RSC_POLICY_DEPENDENT = frozenset({4004, 4103, 4105})
_HTTP_POLICY_DEPENDENT = frozenset({403, 404, 409})

# tinyIoT 기본 요청 버퍼 — 이보다 큰 본문은 응답 없이 버려진다.
DEFAULT_MAX_PAYLOAD = 65536


class OversizeError(Exception):
    """본문이 max_payload 초과 — 전송 자체를 거부한다(계약상 복구 불가).

    tinyIoT는 초과 요청을 응답 없이 버려서 클라이언트는 타임아웃만 보게
    되므로, 애초에 보내지 않는다.
    """

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"serialized body is {size} bytes > max_payload {limit}")
        self.size = size
        self.limit = limit


@dataclass
class OneM2MResponse:
    status: int
    body: dict[str, Any] | str | None
    headers: dict[str, str]
    rsc: int | None = None  # X-M2M-RSC 헤더 (있으면 int로 파싱)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


def classify(resp_or_exc: OneM2MResponse | BaseException) -> Classification:
    """실패 응답/예외를 복구 분류로 매핑한다.

    recoverable       — 연결 오류/타임아웃/5xx: 지수 백오프, 소진 시 클래스별 스풀.
    non_recoverable   — 400/4000번대, CONTENTS_UNACCEPTABLE(4102), 초과 본문:
                        재시도·스풀 없이 상태 이벤트로 종료.
    policy_dependent  — 404/4004, 409/4105, 403/4103: reconcile 트리거
                        (결정적 rn의 4105는 멱등 성공).

    RSC 헤더가 있으면 그것이 우선, HTTP 상태는 대비책.
    성공 응답이 들어오면 ValueError(분류할 것이 없음).
    """
    if isinstance(resp_or_exc, OversizeError):
        return NON_RECOVERABLE
    if isinstance(resp_or_exc, requests.exceptions.RequestException):
        return RECOVERABLE
    if isinstance(resp_or_exc, BaseException):
        # 직렬화 버그 등 — 재시도해도 소용없다
        return NON_RECOVERABLE

    resp = resp_or_exc
    if resp.rsc is not None:
        rsc = resp.rsc
        if rsc < 4000:
            raise ValueError(f"cannot classify success RSC {rsc}")
        if rsc in _RSC_POLICY_DEPENDENT:
            return POLICY_DEPENDENT
        if rsc == 5207:
            # NOT_ACCEPTABLE "result size too big" — 결정적 실패라 재시도가
            # 무의미. 단순 "5xx -> recoverable" 규칙에서 의도적으로 벗어남.
            return NON_RECOVERABLE
        if rsc >= 5000:
            return RECOVERABLE
        return NON_RECOVERABLE

    status = resp.status
    if status < 400:
        raise ValueError(f"cannot classify success HTTP status {status}")
    if status in _HTTP_POLICY_DEPENDENT:
        return POLICY_DEPENDENT
    if status >= 500:
        return RECOVERABLE
    return NON_RECOVERABLE


def backoff_delays(
    retry_count: int,
    base_ms: float = 500.0,
    jitter: float = 0.1,
    *,
    factor: float = 2.0,
    max_ms: float = 30000.0,
    rng: Callable[[], float] | None = None,
) -> list[float]:
    """워커용 지수 백오프 스케줄(밀리초).

    지연 i = min(base_ms * factor**i, max_ms)에 ±jitter 비율(균등분포)을 더한다.
    jitter=0이면 결정적 스케줄. rng는 테스트 주입용이며 [0, 1) 범위 float를
    반환해야 한다.
    """
    if retry_count <= 0:
        return []
    if not 0.0 <= jitter <= 1.0:
        raise ValueError(f"jitter must be within [0, 1], got {jitter}")
    if base_ms <= 0:
        raise ValueError(f"base_ms must be positive, got {base_ms}")
    draw = rng if rng is not None else random.random
    delays: list[float] = []
    for i in range(retry_count):
        d = min(base_ms * factor**i, max_ms)
        if jitter:
            d += d * jitter * (2.0 * draw() - 1.0)
        delays.append(max(0.0, d))
    return delays


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
    표기와 다른 tinyIoT 관용 형식이다.
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
        r = self.session.get(
            self._url(path),
            headers=self._headers(None),
            params=params,
            timeout=self.timeout,
        )
        return self._parse(r)

    def create(self, path: str, ty: int, body: dict[str, Any]) -> OneM2MResponse:
        data = self._serialize(body)
        r = self.session.post(
            self._url(path),
            headers=self._headers(ty),
            data=data,
            timeout=self.timeout,
        )
        return self._parse(r)

    def update(self, path: str, body: dict[str, Any]) -> OneM2MResponse:
        data = self._serialize(body)
        r = self.session.put(
            self._url(path),
            headers=self._headers(None),
            data=data,
            timeout=self.timeout,
        )
        return self._parse(r)

    def delete(self, path: str) -> OneM2MResponse:
        r = self.session.delete(self._url(path), headers=self._headers(None), timeout=self.timeout)
        return self._parse(r)
