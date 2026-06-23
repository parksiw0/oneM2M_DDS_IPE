"""oneM2M 전송 계층의 공용 계약 (HTTP/MQTT 공통, DESIGN §15).

전송 무관 타입과 함수를 모은다: 응답 표현(OneM2MResponse), 복구 분류(classify),
백오프 스케줄(backoff_delays), 본문 초과 예외(OversizeError), 전송 실패 예외
(TransportError), 그리고 두 바인딩이 구현하는 OneM2MClient Protocol.

http_client.py와 mqtt_client.py가 이 계약을 구현하고, make_onem2m_client 팩토리가
cse.protocol로 고른다. 이 모듈은 requests를 import하지 않는다 — 그래야 MQTT 전용
설치에서도 무거운/불필요한 의존이 없다(HTTP 바인딩이 자기 requests 예외를
TransportError로 감싸 올린다).
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ipe.config.spec import ResolvedConfig

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

# tinyIoT 기본 요청 버퍼(MAX_PAYLOAD_SIZE) — HTTP/MQTT 공통 65536.
# 이보다 큰 본문은 전송 자체를 거부한다(계약상 복구 불가).
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


class TransportError(Exception):
    """전송 계층 실패(연결 거부/타임아웃/발행 오류/브로커 단절 등).

    바인딩-중립 예외: HTTP는 requests 예외를, MQTT는 연결/타임아웃 오류를 이걸로
    감싸 올린다. ``recoverable``이 classify()의 판정을 결정한다 — 일시적 실패는
    True(지수 백오프 후 재시도, 소진 시 스풀), 결정적 실패는 False.
    """

    def __init__(self, message: str, *, recoverable: bool = True) -> None:
        super().__init__(message)
        self.recoverable = recoverable


@dataclass
class OneM2MResponse:
    status: int
    body: dict[str, Any] | str | None
    headers: dict[str, str]
    rsc: int | None = None  # oneM2M Response Status Code (X-M2M-RSC / primitive rsc)

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

    RSC가 있으면 그것이 우선, HTTP 상태는 대비책.
    성공 응답이 들어오면 ValueError(분류할 것이 없음).
    """
    if isinstance(resp_or_exc, OversizeError):
        return NON_RECOVERABLE
    if isinstance(resp_or_exc, TransportError):
        return RECOVERABLE if resp_or_exc.recoverable else NON_RECOVERABLE
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


# rsc -> HTTP 등가. MQTT엔 HTTP status가 없어 ResourceOps의 201/409 분기용으로 합성.
_RSC_TO_HTTP = {
    2000: 200, 2001: 201, 2002: 200, 2004: 200,
    4000: 400, 4004: 404, 4102: 400, 4103: 403, 4105: 409,
    5207: 500,
}


def http_status_from_rsc(rsc: int) -> int:
    if rsc in _RSC_TO_HTTP:
        return _RSC_TO_HTTP[rsc]
    if 2000 <= rsc < 3000:
        return 200
    if 5000 <= rsc < 6000:
        return 500
    return 400  # 4xxx 및 그 외 미상 코드


def idify(resource_id: str) -> str:
    """oneM2M 식별자를 MQTT 토픽 세그먼트로 변환한다.

    MQTT 토픽 세그먼트에 '/'가 들어갈 수 없으므로 tinyIoT 관용에 따라 '/'를
    ':'로 치환하고 선행 '/'는 제거한다(MqttClientIdToId의 역연산).
    """
    return resource_id.lstrip("/").replace("/", ":")


@runtime_checkable
class OneM2MClient(Protocol):
    """단일 시도 oneM2M 전송 클라이언트 — HTTP/MQTT 공통 인터페이스.

    각 메서드는 정확히 한 번의 교환을 수행하고 OneM2MResponse를 돌려준다.
    재시도/백오프/스풀 판단은 classify()/backoff_delays()를 쓰는 호출자(워커) 몫.
    실패는 OneM2MResponse(비-2xx) 또는 TransportError/OversizeError로 표현된다.
    ``origin``은 가변(AE 등록 후 aei로 재할당). ``start``/``stop``은 연결 생명주기
    훅으로, HTTP 구현에선 no-op이다.
    """

    origin: str
    endpoint: str
    rvi: str
    max_payload: int

    def get(self, path: str, params: dict[str, Any] | None = None) -> OneM2MResponse: ...
    def create(self, path: str, ty: int, body: dict[str, Any]) -> OneM2MResponse: ...
    def update(self, path: str, body: dict[str, Any]) -> OneM2MResponse: ...
    def delete(self, path: str) -> OneM2MResponse: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...


def make_onem2m_client(rc: ResolvedConfig, origin: str) -> OneM2MClient:
    """cse.protocol(http|mqtt)로 전송 클라이언트를 만든다.

    바인딩 모듈은 지연 import한다 — HTTP 전용 설치에 paho가, 그 반대로 requests가
    강제되지 않도록. 호출자는 반환된 클라이언트의 start()를 호출해야 한다
    (HTTP는 no-op, MQTT는 브로커 연결+구독).
    """
    proto = (getattr(rc.cse, "protocol", "http") or "http").lower()
    if proto == "http":
        from ipe.onem2m.http_client import OneM2MHTTPClient

        return OneM2MHTTPClient(rc.cse.endpoint, origin=origin, rvi=rc.cse.rvi)
    if proto == "mqtt":
        try:
            from ipe.onem2m.mqtt_client import OneM2MMQTTClient
        except ImportError as e:  # paho 미설치
            raise RuntimeError(
                "cse.protocol is 'mqtt' but the MQTT binding is unavailable "
                "(install the optional dependency: pip install \"ipe[mqtt]\" "
                "or \"paho-mqtt>=2.1\")"
            ) from e
        return OneM2MMQTTClient(rc.cse.mqtt, rc.cse.cse_id, rc.cse.cse_base,
                                origin=origin, rvi=rc.cse.rvi)
    raise ValueError(f"unknown cse.protocol {proto!r} (expected 'http' or 'mqtt')")
