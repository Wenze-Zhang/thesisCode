from __future__ import annotations

import json


class FakeAction:
    def __init__(self, existing_dataset=None):
        self.existing_dataset = existing_dataset
        self.created = []
        self.patched = []
        self.shown = []

    def package_show(self, id):
        self.shown.append(id)
        return self.existing_dataset

    def package_create(self, **pkg):
        self.created.append(pkg)
        return pkg

    def package_patch(self, **pkg):
        self.patched.append(pkg)
        return pkg


class FakeCKAN:
    def __init__(self, existing_dataset=None):
        self.action = FakeAction(existing_dataset)


def _device_event(name="climate-lab-01", *, entity_type="DEVICE"):
    event = {
        "id": {"entityType": entity_type, "id": "dev-climate-01"},
        "entityName": name,
        "additionalInfo": {"description": "Climate lab sensor"},
    }
    return event


def _headers(event_type="ENTITY_CREATED", *, entity_type="DEVICE", device_name=None):
    headers = {
        "eventType": event_type,
        "entityType": entity_type,
        "deviceId": "dev-climate-01",
    }
    if device_name is not None:
        headers["deviceName"] = device_name
    return headers


def test_build_dataset_from_lifecycle_event(enricher_module, loaded_registry):
    pkg = enricher_module.build_dataset(
        _device_event(),
        _headers(),
        reg=loaded_registry,
        event_type="ENTITY_CREATED",
    )

    assert pkg is not None
    for key in (
        "name",
        "title",
        "owner_org",
        "thingsboard_device_id",
        "dataset_uuid",
        "created_at_iso",
        "updated_at_iso",
        "sensor_type",
        "provenance_json",
    ):
        assert pkg[key]
    assert pkg["name"] == "sensor-climate-lab-01"
    assert pkg["thingsboard_device_id"] == "dev-climate-01"


def test_handle_entity_created_calls_package_create(enricher_module, loaded_registry):
    ckan = FakeCKAN()

    enricher_module.handle_message(
        ckan,
        loaded_registry,
        _device_event(),
        _headers("ENTITY_CREATED"),
    )

    assert len(ckan.action.created) == 1
    assert ckan.action.patched == []


def test_existing_dataset_is_patched_not_duplicated(enricher_module, loaded_registry):
    existing = {
        "name": "sensor-climate-lab-01",
        "dataset_uuid": "existing-dataset-uuid",
        "created_at_iso": "2026-06-01T00:00:00+00:00",
        "provenance_json": "",
    }
    ckan = FakeCKAN(existing)

    enricher_module.handle_message(
        ckan,
        loaded_registry,
        _device_event(),
        _headers("ENTITY_UPDATED"),
    )

    assert ckan.action.created == []
    assert len(ckan.action.patched) == 1
    assert ckan.action.patched[0]["id"] == "sensor-climate-lab-01"
    assert "name" not in ckan.action.patched[0]


def test_non_device_event_is_skipped(enricher_module, loaded_registry):
    ckan = FakeCKAN()

    enricher_module.handle_message(
        ckan,
        loaded_registry,
        _device_event(entity_type="ASSET"),
        _headers(entity_type="ASSET"),
    )

    assert ckan.action.created == []
    assert ckan.action.patched == []


def test_missing_device_name_is_skipped(enricher_module, loaded_registry):
    ckan = FakeCKAN()
    event = {
        "id": {"entityType": "DEVICE", "id": "dev-without-name"},
        "additionalInfo": {},
    }

    enricher_module.handle_message(
        ckan,
        loaded_registry,
        event,
        {"eventType": "ENTITY_CREATED", "entityType": "DEVICE"},
    )

    assert ckan.action.created == []
    assert ckan.action.patched == []


def test_dataset_contains_semantic_metadata(enricher_module, loaded_registry):
    pkg = enricher_module.build_dataset(
        _device_event("climate-lab-01"),
        _headers(),
        reg=loaded_registry,
        event_type="ENTITY_CREATED",
        telemetry_keys=["temperature_c"],
    )

    assert pkg is not None
    assert pkg["sensor_type"] == "climate"
    assert pkg["unit"] == "degC"
    assert pkg["qudt_unit_uri"] == "http://qudt.org/vocab/unit/DEG_C"
    assert pkg["sosa_observable_property_uri"]
    assert pkg["sosa_sensor_uri"]


def test_dataset_contains_provenance_json(enricher_module, loaded_registry):
    pkg = enricher_module.build_dataset(
        _device_event(),
        _headers(),
        reg=loaded_registry,
        event_type="ENTITY_CREATED",
    )

    assert pkg is not None
    provenance = json.loads(pkg["provenance_json"])
    assert provenance["entity"]
    assert provenance["activity"]
    assert provenance["agent"]
