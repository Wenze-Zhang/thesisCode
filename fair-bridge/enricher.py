#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import re
import sys
import time
import uuid

import requests
from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError, NotFound, ValidationError
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import config
import ontology_mappings
import provenance
import registry


# log configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
log = logging.getLogger("fair-bridge")


# conversion of names that CKAN accepts
def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if len(slug) < 2:
        slug = f"sensor-{slug or 'unnamed'}"
    return slug[:100]


def wait_for_ckan(url: str, timeout: int) -> None:
    deadline = time.time() + timeout
    log.info("Waiting for CKAN at %s ...", url)
    while time.time() < deadline:
        try:
            response = requests.get(f"{url}/api/action/status_show", timeout=3)
            if response.status_code == 200 and response.json().get("success"):
                log.info("CKAN ready.")
                return
        except Exception:
            pass
        time.sleep(4)
    raise RuntimeError(f"CKAN not ready after {timeout}s")


def wait_for_kafka(bootstrap: str, timeout: int) -> KafkaConsumer:
    deadline = time.time() + timeout
    log.info("Waiting for Kafka at %s ...", bootstrap)
    last_exc = None
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_LIFECYCLE,
                bootstrap_servers=bootstrap,
                group_id=config.KAFKA_CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else {},
            )
            log.info("Kafka ready; subscribed to %s", config.KAFKA_TOPIC_LIFECYCLE)
            return consumer
        except NoBrokersAvailable as exc:
            last_exc = exc
            time.sleep(4)
    raise RuntimeError(f"Kafka not ready after {timeout}s ({last_exc})")

# create CKAN organization if not exists
def ensure_organization(ckan: RemoteCKAN, org_name: str, title: str) -> None:
    try:
        ckan.action.organization_show(id=org_name)
        log.info("Organization '%s' exists.", org_name)
    except NotFound:
        log.info("Creating organization '%s' ...", org_name)
        ckan.action.organization_create(name=org_name, title=title)


def _existing_dataset(ckan: RemoteCKAN, slug: str) -> dict | None:
    try:
        return ckan.action.package_show(id=slug)
    except NotFound:
        return None
    except CKANAPIError:
        log.exception("package_show failed for %s", slug)
        return None



def _extract_sampling_interval(event: dict) -> int | None:
    info = event.get("additionalInfo") or {}
    interval = (
        info.get("samplingInterval")
        or info.get("sampling_interval")
        or info.get("sampling_interval_s")
    )
    try:
        return int(float(interval)) if interval is not None else None
    except (TypeError, ValueError):
        return None


def _device_id(event: dict, headers: dict) -> str:
    for value in (event.get("entityId"), event.get("id")):
        if (
            isinstance(value, dict)
            and value.get("entityType") == "DEVICE"
            and value.get("id")
        ):
            return str(value["id"])
    if headers.get("entityType", "DEVICE") == "DEVICE":
        return headers.get("deviceId", "")
    return ""

# from ThingsBoard message to CKAN dataset package dict
# package, not use CKAN API directly
def build_dataset(
    event: dict,
    headers: dict,
    *,
    reg: registry.Registry,
    existing: dict | None = None,
    event_type: str = "ENTITY_CREATED",
    telemetry_keys: list[str] | None = None,
) -> dict | None:
    device_name = event.get("entityName") or headers.get("deviceName") or event.get("name")
    
    if not device_name:
        log.warning("No device name in event; skipping. event=%s headers=%s", event, headers)
        return None

    device_id = _device_id(event, headers)
    description = (
        (event.get("additionalInfo") or {}).get("description")
        or f"Auto-registered from ThingsBoard device {device_name!r}."
    )
    slug = slugify(f"sensor-{device_name}")


    if existing:
        dataset_uuid = existing.get("dataset_uuid") or str(uuid.uuid4())
        created_at = existing.get("created_at_iso") or provenance.now_iso()
        prior_provenance = provenance.from_json_str(existing.get("provenance_json"))
    else:
        dataset_uuid = str(uuid.uuid4())
        created_at = provenance.now_iso()
        prior_provenance = None
        

    metadata = ontology_mappings.infer_metadata(reg, device_name, telemetry_keys)
    sensor_type = metadata.get("sensor_type") or "other"

    pkg = {
        "name": slug,
        "title": device_name,
        "notes": description,
        "owner_org": config.CKAN_ORG,
        "tags": [
            {"name": sensor_type},
            {"name": "sensor"},
            {"name": "thingsboard"},
        ],
        "thingsboard_device_id": device_id,
        "protocol": "mqtt",
        "dataset_uuid": dataset_uuid,
        "created_at_iso": created_at,
        "updated_at_iso": provenance.now_iso(),
    }
    pkg.update(metadata)

    sampling_interval = _extract_sampling_interval(event)
    if sampling_interval is not None:
        pkg["sampling_interval_s"] = sampling_interval

    if prior_provenance is None:
        prov_doc = provenance.build_prov(
            dataset_uuid=dataset_uuid,
            device_id=device_id,
            device_name=device_name,
            tb_url=config.TB_URL,
            ckan_url=config.CKAN_URL,
            event_type=event_type,
        )
    else:
        prov_doc = provenance.append_activity(
            prior_provenance,
            dataset_uuid=dataset_uuid,
            event_type=event_type,
        )
    pkg["provenance_json"] = provenance.to_json_str(prov_doc)
    return pkg


def _patch_dataset(ckan: RemoteCKAN, pkg: dict) -> None:
    slug = pkg["name"]
    ckan.action.package_patch(
        id=slug,
        **{key: value for key, value in pkg.items() if key != "name"},
    )
    log.info("Dataset patched: %s", slug)


def upsert_dataset(ckan: RemoteCKAN, pkg: dict, *, existing: dict | None = None) -> None:
    slug = pkg["name"]
    if existing is not None:
        _patch_dataset(ckan, pkg)
        return

    try:
        ckan.action.package_create(**pkg)
        log.info("Dataset created: %s", slug)
    except ValidationError as exc:
        if "name" in (getattr(exc, "error_dict", {}) or {}):
            log.info("Dataset %s already exists; patching.", slug)
            _patch_dataset(ckan, pkg)
        else:
            log.warning("Validation error for %s: %s", slug, getattr(exc, "error_dict", {}))


def handle_create_or_update(
    ckan: RemoteCKAN,
    reg: registry.Registry,
    event: dict,
    headers: dict,
    event_type: str,
) -> None:
    name = event.get("entityName") or headers.get("deviceName") or event.get("name")
    # name starting from sensor
    slug = slugify(f"sensor-{name}") if name else None
    existing = _existing_dataset(ckan, slug) if slug else None
    pkg = build_dataset(event, headers, reg=reg, existing=existing, event_type=event_type)
    if pkg is not None:
        upsert_dataset(ckan, pkg, existing=existing)


def _entity_type(value: dict, headers: dict) -> str:
    return (
        headers.get("entityType")
        or (value.get("id") or {}).get("entityType")
        or (value.get("entityId") or {}).get("entityType")
        or ""
    )


def handle_message(
    ckan: RemoteCKAN,
    reg: registry.Registry,
    value: dict,
    headers: dict,
) -> None:
    event_type = headers.get("eventType") or value.get("msgType") or ""
    entity_type = _entity_type(value, headers)
    if entity_type and entity_type != "DEVICE":
        log.info(
            "Skipping non-DEVICE lifecycle event (entityType=%s name=%r).",
            entity_type,
            value.get("name") or value.get("entityName"),
        )
        return

    if event_type in ("ENTITY_CREATED", "ADDED"):
        handle_create_or_update(ckan, reg, value, headers, "ENTITY_CREATED")
    elif event_type in ("ENTITY_UPDATED", "UPDATED"):
        handle_create_or_update(ckan, reg, value, headers, "ENTITY_UPDATED")
    elif value.get("entityName") or value.get("name"):
        handle_create_or_update(ckan, reg, value, headers, "ENTITY_UPDATED")
    else:
        log.debug("Ignoring lifecycle event type=%s", event_type)


def main() -> int:
    if not config.CKAN_API_KEY:
        log.error(
            "CKAN_API_KEY is empty. Create a CKAN API token, set CKAN_API_KEY, "
            "and restart fair-bridge."
        )
        while True:
            time.sleep(30)

    reg = registry.load_registry(config.REGISTRY_PATH)
    log.info("Registry ready: %d sensor types.", len(reg.sensor_types))
    wait_for_ckan(config.CKAN_URL, config.CKAN_READY_TIMEOUT_S)
    ckan = RemoteCKAN(
        config.CKAN_URL,
        apikey=config.CKAN_API_KEY,
        user_agent="fair-bridge/2.0",
    )
    ensure_organization(ckan, config.CKAN_ORG, config.CKAN_ORG_TITLE)

    consumer = wait_for_kafka(
        config.KAFKA_BOOTSTRAP_SERVERS,
        config.KAFKA_READY_TIMEOUT_S,
    )

    # listen to lifecycle topic 
    log.info("Listening on topic '%s' ...", config.KAFKA_TOPIC_LIFECYCLE)
    for msg in consumer:
        try:
            headers = {
                key.removeprefix("tb_msg_md_"): value.decode("utf-8", "replace")
                for key, value in (msg.headers or [])
            }
            # Transient CKAN connection failures (uwsgi worker restart under a
            # provisioning burst closes the socket mid-request) must not drop
            # the lifecycle event: a skipped ENTITY_CREATED permanently orphans
            # that device's telemetry (no dataset -> exporter retries forever).
            # Retrying the whole message is idempotent: _existing_dataset
            # re-checks and an already-created dataset is patched, not duped.
            for attempt in range(1, 6):
                try:
                    handle_message(ckan, reg, msg.value or {}, headers)
                    break
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as exc:
                    if attempt == 5:
                        raise
                    wait_s = 2.0 * attempt
                    log.warning(
                        "Transient CKAN error at offset %s (%s); "
                        "retry %d/5 in %.0fs", msg.offset, exc, attempt, wait_s)
                    time.sleep(wait_s)
        except Exception:
            log.exception("Error while processing message at offset %s", msg.offset)

    return 0


if __name__ == "__main__":
    sys.exit(main())
