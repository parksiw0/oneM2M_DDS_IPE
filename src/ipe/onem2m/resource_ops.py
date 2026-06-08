from __future__ import annotations

import logging
from typing import Any

from ipe.onem2m.http_client import OneM2MHTTPClient, OneM2MResponse

log = logging.getLogger(__name__)

TY_AE = 2
TY_CNT = 3
TY_CIN = 4
TY_FCNT = 28


class ResourceOpsError(Exception):
    pass


class ResourceOps:
    def __init__(self, client: OneM2MHTTPClient) -> None:
        self.client = client

    def ensure_ae(self, parent: str, name: str, app_id: str = "Nipe") -> tuple[str, str | None]:
        body = {"m2m:ae": {"rn": name, "api": app_id, "rr": True, "srv": ["2a"]}}
        r = self.client.create(parent, TY_AE, body)
        path = f"{parent.rstrip('/')}/{name}"
        aei: str | None = None
        if r.status == 201 and isinstance(r.body, dict):
            aei = r.body.get("m2m:ae", {}).get("aei")
            log.info("CREATED AE %s (aei=%s)", path, aei)
        elif r.status == 409:
            log.info("EXISTS  AE %s — skip (aei unknown, will probe via origin variants)", path)
        else:
            raise ResourceOpsError(
                f"Failed to ensure AE at {path}: HTTP {r.status} body={r.body!r}"
            )
        return path, aei

    def ensure_cnt(self, parent: str, name: str) -> str:
        body = {"m2m:cnt": {"rn": name}}
        r = self.client.create(parent, TY_CNT, body)
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
        body: dict[str, Any] = {"rn": name, "cnd": cnd}
        if initial_attrs:
            body.update(initial_attrs)
        r = self.client.create(parent, TY_FCNT, {fcnt_type: body})
        path = f"{parent.rstrip('/')}/{name}"
        return self._check(r, path, f"FCNT ({fcnt_type})")

    def create_cin(self, parent: str, content: dict[str, Any]) -> OneM2MResponse:
        return self.client.create(parent, TY_CIN, content)

    def update_fcnt(self, path: str, content: dict[str, Any]) -> OneM2MResponse:
        return self.client.update(path, content)

    def _check(self, r: OneM2MResponse, path: str, kind: str) -> str:
        if r.status == 201:
            log.info("CREATED %s %s", kind, path)
            return path
        if r.status == 409:
            log.info("EXISTS  %s %s — skip", kind, path)
            return path
        raise ResourceOpsError(
            f"Failed to ensure {kind} at {path}: HTTP {r.status} body={r.body!r}"
        )
