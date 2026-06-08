from __future__ import annotations
from typing import Iterable
from registry import Registry

LICENSE_ID = "CC-BY-4.0" #example, or other license ID


def _canonical(registry: Registry, key: str) -> str:
    return registry.alias_to_canonical.get(str(key).lower(), key)


def _has_semantics(field: dict | None) -> bool:
    if not field:
        return False
    return any(
        field.get(key)
        for key in (
            "measurement_type",
            "unit",
            "unit_uri",
            "observable_property_uri",
        )
    )


def infer_metadata(
    registry: Registry,
    device_name: str,
    telemetry_keys: Iterable[str] | None = None,
) -> dict:
    sensor_type = registry.classify(device_name)
    device_type = registry.get_device_type(sensor_type)
    field_name = None

    for key in telemetry_keys or []:
        candidate = _canonical(registry, key)
        if _has_semantics(registry.get_field(sensor_type, candidate)):
            field_name = candidate
            break

    if field_name is None:
        field_name = registry.get_primary_field(sensor_type)

    field = registry.get_field(sensor_type, field_name or "")

    return {
        "sensor_type": sensor_type,
        "sosa_sensor_uri": device_type.get("sosa_sensor_uri", ""),
        "measurement_type": (field or {}).get("measurement_type", ""),
        "unit": (field or {}).get("unit", ""),
        "qudt_unit_uri": (field or {}).get("unit_uri", ""),
        "sosa_observable_property_uri": (field or {}).get("observable_property_uri", ""),
        "license_id": LICENSE_ID,
    }


def numeric_keys(
    registry: Registry,
    sensor_type: str,
    payload_keys: Iterable[str],
) -> list[str]:
    return registry.numeric_keys(sensor_type, payload_keys)
