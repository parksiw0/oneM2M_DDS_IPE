from __future__ import annotations

import logging
from typing import Any

from ipe.onem2m.http_client import OneM2MHTTPClient
from ipe.onem2m.resource_ops import ResourceOps, ResourceOpsError

log = logging.getLogger(__name__)


class BootstrapError(Exception):
    pass


def bootstrap(config: dict[str, Any], reset: bool = False) -> ResourceOps:
    cse_cfg = config["cse"]
    endpoint = cse_cfg["endpoint"]
    cse_base = cse_cfg["cse_base"]
    ae_name = cse_cfg["ae_name"]
    origin = cse_cfg.get("origin", "admin")

    client = OneM2MHTTPClient(endpoint=endpoint, origin=origin)
    ops = ResourceOps(client)

    log.info("Bootstrap: CSE %s base=%s ae=%s reset=%s", endpoint, cse_base, ae_name, reset)

    health = client.get(f"/{cse_base}")
    if health.status not in (200, 403):
        raise BootstrapError(
            f"CSE health check failed: GET /{cse_base} -> {health.status}"
        )
    log.info("CSE alive (HTTP %d)", health.status)

    if reset:
        ae_full = f"/{cse_base}/{ae_name}"
        r = client.delete(ae_full)
        if r.status in (200, 204):
            log.info("DELETED AE %s (reset)", ae_full)
        elif r.status == 404:
            log.info("AE %s already absent (reset noop)", ae_full)
        else:
            log.warning("AE reset DELETE %s -> HTTP %d body=%r", ae_full, r.status, r.body)

    try:
        ae_path, aei = ops.ensure_ae(f"/{cse_base}", ae_name)
        if aei:
            client.origin = aei
            log.info("Switched origin to AE-ID: %s", aei)
        else:
            for candidate in (f"C{ae_name}", ae_name):
                client.origin = candidate
                probe = client.get(ae_path)
                if probe.status == 200:
                    log.info("Switched origin to: %s (probed)", candidate)
                    break
            else:
                client.origin = origin
                log.warning(
                    "Could not determine AE-ID for existing AE %s; "
                    "remaining as origin=%s (FCNT updates may fail with 403). "
                    "If so, restart TinyIoT to wipe state and re-bootstrap.",
                    ae_path, origin,
                )

        ros2_data = ops.ensure_cnt(ae_path, "ros2Data")

        categories_needed: set[str] = set()
        for t in config.get("topics", []):
            categories_needed.add(t["semantic_category"])
        category_paths: dict[str, str] = {}
        for cat in sorted(categories_needed):
            category_paths[cat] = ops.ensure_cnt(ros2_data, cat)

        for t in config.get("topics", []):
            cat = t["semantic_category"]
            alias = t["resource_alias"]
            parent = category_paths[cat]
            fc = t.get("flexcontainer")
            if fc:
                ops.ensure_fcnt(parent, alias, fc["cnd"], fc["type"])
            else:
                ops.ensure_cnt(parent, alias)
    except ResourceOpsError as e:
        raise BootstrapError(str(e)) from e

    log.info("Bootstrap complete.")
    return ops
