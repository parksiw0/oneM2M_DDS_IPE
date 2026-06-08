from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import requests

log = logging.getLogger(__name__)


@dataclass
class OneM2MResponse:
    status: int
    body: dict[str, Any] | str | None
    headers: dict[str, str]


class OneM2MHTTPClient:
    def __init__(
        self,
        endpoint: str,
        origin: str = "admin",
        rvi: str = "2a",
        timeout: float = 5.0,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.origin = origin
        self.rvi = rvi
        self.timeout = timeout
        self.session = requests.Session()

    def _headers(self, ty: int | None, accept: str = "application/json") -> dict[str, str]:
        h = {
            "X-M2M-Origin": self.origin,
            "X-M2M-RI": str(uuid.uuid4())[:12],
            "X-M2M-RVI": self.rvi,
            "Accept": accept,
        }
        if ty is not None:
            h["Content-Type"] = f"application/json;ty={ty}"
        else:
            h["Content-Type"] = "application/json"
        return h

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.endpoint + ("/" + path.lstrip("/"))

    def _parse(self, r: requests.Response) -> OneM2MResponse:
        body: Any = None
        if r.text:
            try:
                body = r.json()
            except json.JSONDecodeError:
                body = r.text
        return OneM2MResponse(status=r.status_code, body=body, headers=dict(r.headers))

    def get(self, path: str) -> OneM2MResponse:
        r = self.session.get(self._url(path), headers=self._headers(None), timeout=self.timeout)
        return self._parse(r)

    def create(self, path: str, ty: int, body: dict[str, Any]) -> OneM2MResponse:
        r = self.session.post(
            self._url(path),
            headers=self._headers(ty),
            data=json.dumps(body),
            timeout=self.timeout,
        )
        return self._parse(r)

    def update(self, path: str, body: dict[str, Any]) -> OneM2MResponse:
        r = self.session.put(
            self._url(path),
            headers=self._headers(None),
            data=json.dumps(body),
            timeout=self.timeout,
        )
        return self._parse(r)

    def delete(self, path: str) -> OneM2MResponse:
        r = self.session.delete(self._url(path), headers=self._headers(None), timeout=self.timeout)
        return self._parse(r)
