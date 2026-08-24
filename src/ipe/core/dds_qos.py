"""Canonical DDS QoS policy registry and oneM2M mapping records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ipe.config.spec import QoSSpec


@dataclass(frozen=True)
class DDSPolicyDefinition:
    name: str
    category: str
    scopes: tuple[str, ...]
    default_mapping: dict[str, Any]


DDS_QOS_POLICIES: tuple[DDSPolicyDefinition, ...] = (
    DDSPolicyDefinition("LIFESPAN", "A", ("DATAWRITER", "TOPIC"),
                        {"kind": "RESOURCE", "resourceType": "contentInstance",
                         "attribute": "expirationTime"}),
    DDSPolicyDefinition("HISTORY", "A", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "RESOURCE", "resourceType": "container",
                         "attribute": "maxNrOfInstances"}),
    DDSPolicyDefinition("RESOURCE_LIMITS", "A",
                        ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "RESOURCE_PARTIAL", "resourceType": "container",
                         "attributes": ["maxNrOfInstances", "maxByteSize"]}),
    DDSPolicyDefinition("RELIABILITY", "B", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "BEHAVIOR", "handler": "deliveryRetry"}),
    DDSPolicyDefinition("DEADLINE", "B", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "BEHAVIOR", "handler": "deadlineMonitor"}),
    DDSPolicyDefinition("LIVELINESS", "B", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "BEHAVIOR", "handler": "livelinessMonitor"}),
    DDSPolicyDefinition("OWNERSHIP", "B", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "BEHAVIOR", "handler": "writerArbitration"}),
    DDSPolicyDefinition("OWNERSHIP_STRENGTH", "B", ("DATAWRITER",),
                        {"kind": "BEHAVIOR", "handler": "writerArbitration"}),
    DDSPolicyDefinition("DURABILITY", "B", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "BEHAVIOR", "handler": "retentionReplay"}),
    DDSPolicyDefinition("TIME_BASED_FILTER", "B", ("DATAREADER",),
                        {"kind": "BEHAVIOR", "handler": "sampleSuppression"}),
    DDSPolicyDefinition("WRITER_DATA_LIFECYCLE", "B", ("DATAWRITER",),
                        {"kind": "BEHAVIOR", "handler": "writerLifecycle"}),
    DDSPolicyDefinition("READER_DATA_LIFECYCLE", "B", ("DATAREADER",),
                        {"kind": "BEHAVIOR", "handler": "readerLifecycle"}),
    DDSPolicyDefinition("DESTINATION_ORDER", "C", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("DURABILITY_SERVICE", "C", ("TOPIC",),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("PRESENTATION", "C", ("PUBLISHER", "SUBSCRIBER"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("PARTITION", "C", ("PUBLISHER", "SUBSCRIBER"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("LATENCY_BUDGET", "C", ("DATAWRITER", "DATAREADER", "TOPIC"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("TRANSPORT_PRIORITY", "C", ("DATAWRITER", "TOPIC"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("GROUP_DATA", "C", ("PUBLISHER", "SUBSCRIBER"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("TOPIC_DATA", "C", ("TOPIC",),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("USER_DATA", "C",
                        ("DOMAINPARTICIPANT", "DATAWRITER", "DATAREADER"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
    DDSPolicyDefinition("ENTITY_FACTORY", "C",
                        ("DOMAINPARTICIPANT", "PUBLISHER", "SUBSCRIBER"),
                        {"kind": "METADATA", "attribute": "ddsQoSProperties"}),
)

DDS_QOS_POLICY_NAMES = tuple(policy.name for policy in DDS_QOS_POLICIES)


def _duration(ms: int | None) -> str | int:
    return "INF" if ms is None else ms


def rmw_policy_values(spec: QoSSpec | None) -> dict[str, Any]:
    if spec is None:
        return {}
    return {
        "RELIABILITY": spec.reliability,
        "DURABILITY": spec.durability,
        "HISTORY": {"kind": spec.history, "depth": spec.depth},
        "DEADLINE": {"durationMs": _duration(spec.deadline_ms)},
        "LIFESPAN": {"durationMs": _duration(spec.lifespan_ms)},
        "LIVELINESS": {
            "kind": spec.liveliness,
            "leaseDurationMs": _duration(spec.liveliness_lease_duration_ms),
        },
    }


def _default_result(name: str, applied: Any) -> str:
    if applied is None:
        return "UNAVAILABLE"
    if name in {"LIFESPAN", "HISTORY", "RELIABILITY", "DURABILITY"}:
        return "APPROXIMATED"
    return "ENFORCED"


def build_policy_records(
    configured: QoSSpec | None,
    applied: QoSSpec | None,
    *,
    source: str = "ROS2_RMW",
    native_policies: dict[str, Any] | None = None,
    mapping_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Build a complete 22-policy snapshot without inventing unavailable values."""
    requested_values = rmw_policy_values(configured)
    applied_values = rmw_policy_values(applied)
    native_values = {str(k).upper(): v for k, v in (native_policies or {}).items()}
    overrides = mapping_overrides or {}
    records: dict[str, dict[str, Any]] = {}
    for definition in DDS_QOS_POLICIES:
        name = definition.name
        requested = requested_values.get(name)
        effective = applied_values.get(name)
        observed = native_values.get(name)
        policy_source = source if effective is not None else "UNAVAILABLE"
        result = _default_result(name, effective)
        if observed is not None:
            effective = observed if effective is None else effective
            policy_source = "NATIVE_DDS"
            result = "PRESERVED" if definition.category == "C" else result
        mapping = dict(definition.default_mapping)
        mapping.update(overrides.get(name, {}))
        if "result" in mapping:
            result = str(mapping.pop("result"))
        record: dict[str, Any] = {
            "category": definition.category,
            "scope": list(definition.scopes),
            "source": policy_source,
            "requested": requested,
            "applied": effective,
            "mapping": mapping,
            "result": result,
        }
        if policy_source == "UNAVAILABLE":
            record["reason"] = "not exposed by the active ROS 2/DDS adapter"
        records[name] = record
    return records


def metadata_properties(policies: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return category_records(policies, "C")


def category_records(
    policies: dict[str, dict[str, Any]], category: str
) -> dict[str, dict[str, Any]]:
    """Return policy records for one mapping category."""
    return {
        policy.name: policies[policy.name]
        for policy in DDS_QOS_POLICIES
        if policy.category == category
    }


def compact_policy_groups(
    configured: QoSSpec | None,
    applied: QoSSpec | None,
    *,
    source: str = "ROS2_RMW",
    native_policies: dict[str, Any] | None = None,
    mapping_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build compact A/B/C management state while keeping the 22-policy registry in code."""
    policies = build_policy_records(
        configured,
        applied,
        source=source,
        native_policies=native_policies,
        mapping_overrides=mapping_overrides,
    )
    resource_mappings: dict[str, Any] = {}
    behavior_status: dict[str, Any] = {}
    metadata: dict[str, Any] = {}
    unavailable: list[str] = []
    for definition in DDS_QOS_POLICIES:
        name = definition.name
        record = policies[name]
        if record["source"] == "UNAVAILABLE":
            unavailable.append(name)
            continue
        value = record["applied"]
        if definition.category == "C":
            metadata[name] = {"value": value, "source": record["source"]}
            continue
        entry: dict[str, Any] = {"applied": value, "result": record["result"]}
        if record["requested"] != value:
            entry["requested"] = record["requested"]
        mapping = record["mapping"]
        if definition.category == "A":
            entry.update(mapping)
            resource_mappings[name] = entry
        else:
            if mapping.get("handler"):
                entry["handler"] = mapping["handler"]
            behavior_status[name] = entry
    return {
        "resourceMappings": resource_mappings,
        "behaviorStatus": behavior_status,
        "ddsQoSProperties": metadata,
        "unavailablePolicies": unavailable,
    }
