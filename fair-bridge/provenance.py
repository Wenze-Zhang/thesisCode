"""
PROV O      https://www.w3.org/TR/prov-o/
PROV JSON   https://www.w3.org/Submission/2013/SUBM-prov-json-20130424/
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

MAX_ACTIVITIES = 10

_AGENT_ID = "enricher:fair-bridge"
_AGENT_LABEL = "FAIR Bridge Enricher v2"


def now_iso() -> str:
    # UTC ISO 8601
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _prefixes(tb_url: str, ckan_url: str) -> dict[str, str]:
    return {
        "prov":   "http://www.w3.org/ns/prov#",
        "xsd":    "http://www.w3.org/2001/XMLSchema#",
        "sosa":   "http://www.w3.org/ns/sosa/",
        "qudt":   "http://qudt.org/schema/qudt/",
        "schema": "https://schema.org/",
        "tb":     tb_url.rstrip("/") + "/",
        "ckan":   ckan_url.rstrip("/") + "/",
    }


def build_prov(
    *,
    dataset_uuid: str,
    device_id: str,
    device_name: str,
    tb_url: str,
    ckan_url: str,
    event_type: str,
) -> dict[str, Any]:
    # Construct a fresh PROV JSON document
    ts = now_iso()
    activity_id = f"enricher:{dataset_uuid}#{ts}"
    entity_id = f"ckan:dataset/{dataset_uuid}"
    device_entity_id = f"tb:device/{device_id or device_name}"

    return {
        "prefix": _prefixes(tb_url, ckan_url),
        "entity": {
            entity_id: {
                "prov:type": "schema:Dataset",
                "schema:name": device_name,
                "tb:thingsboard_device_id": device_id,
            },
            device_entity_id: {
                "prov:type": "sosa:Sensor",
                "schema:name": device_name,
            },
        },
        "activity": {
            activity_id: {
                "prov:type": f"Enrichment/{event_type}",
                "prov:startedAtTime": ts,
                "prov:endedAtTime":   ts,
            },
        },
        "agent": {
            _AGENT_ID: {
                "prov:type": "prov:SoftwareAgent",
                "prov:label": _AGENT_LABEL,
            },
        },
        "wasGeneratedBy": [
            {"prov:entity": entity_id, "prov:activity": activity_id},
        ],
        "wasAttributedTo": [
            {"prov:entity": entity_id, "prov:agent": _AGENT_ID},
        ],
        "wasDerivedFrom": [
            {"prov:generatedEntity": entity_id,
             "prov:usedEntity": device_entity_id},
        ],
        "wasAssociatedWith": [
            {"prov:activity": activity_id, "prov:agent": _AGENT_ID},
        ],
    }


def append_activity(prov: dict[str, Any], *, dataset_uuid: str,
                    event_type: str) -> dict[str, Any]:
    # Append a new Activity to an existing PROV JSON doc, bounded to :data:`MAX_ACTIVITIES`.
    ts = now_iso()
    activity_id = f"enricher:{dataset_uuid}#{ts}"
    activities = prov.setdefault("activity", {})
    activities[activity_id] = {
        "prov:type": f"Enrichment/{event_type}",
        "prov:startedAtTime": ts,
        "prov:endedAtTime":   ts,
    }

    prov.setdefault("wasAssociatedWith", []).append(
        {"prov:activity": activity_id, "prov:agent": _AGENT_ID}
    )

    # Trim if exceed MAX_ACTIVITIES: keep the newest N, fold the rest into
    # a single 'summary' activity that records how many were collapsed.
    if len(activities) > MAX_ACTIVITIES:
        items_sorted = sorted(activities.items(),
                              key=lambda kv: kv[1].get("prov:startedAtTime", ""))
        old = items_sorted[:-MAX_ACTIVITIES]
        kept = dict(items_sorted[-MAX_ACTIVITIES:])
        kept[f"enricher:{dataset_uuid}#summary"] = {
            "prov:type": "Enrichment/summary",
            "prov:label": f"{len(old)} earlier activities collapsed",
            "prov:endedAtTime": old[-1][1].get("prov:endedAtTime", ts),
        }
        prov["activity"] = kept
    return prov


def to_json_str(prov: dict[str, Any]) -> str:
    # Serialise to a compact, sorted JSON string for storage in CKAN extras.
    return json.dumps(prov, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def from_json_str(s: str | None) -> dict[str, Any] | None:
    # Reverse of ： to_json_str.  Returns None on empty/invalid input
    # so the caller can fall back to :func:`build_prov`.
    if not s:
        return None
    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else None
    except (ValueError, TypeError):
        return None
