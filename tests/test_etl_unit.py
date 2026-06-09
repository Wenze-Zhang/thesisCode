from __future__ import annotations

from datetime import datetime, timezone


def _climate_payload(values, ts="2024-06-01T12:00:00+00:00"):
    return {
        "deviceName": "climate-lab-01",
        "deviceId": "dev-climate-01",
        "ts": ts,
        "values": values,
    }


def test_valid_canonical_telemetry_goes_to_validated_topic(
    etl_module,
    config_module,
    loaded_registry,
):
    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": 21.5, "humidity_pct": 45.0}),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_TELEMETRY_VALIDATED
    assert payload["quality"] == "validated"
    assert "error" not in payload
    assert "canonicalized" not in payload
    assert payload["values"]["temperature_c"] == 21.5


def test_alias_field_is_canonicalized_and_validated(
    etl_module,
    config_module,
    loaded_registry,
):
    target, payload = etl_module.process_message(
        _climate_payload({"temp_c": 22.0, "humidity_pct": 46.0}),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_TELEMETRY_VALIDATED
    assert payload["quality"] == "validated"
    assert "error" not in payload
    assert payload["values"]["temperature_c"] == 22.0
    assert "temp_c" not in payload["values"]
    assert payload["canonicalized"] == {"temp_c": "temperature_c"}


def test_unknown_field_goes_to_dlq(etl_module, config_module, loaded_registry):
    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": 21.5, "mystery_field": 1}),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_DLQ
    assert "quality" not in payload
    assert "canonicalized" not in payload
    assert "unknown fields" in payload["error"]
    assert "mystery_field" in payload["error"]


def test_wrong_datatype_goes_to_dlq_with_schema_error(
    etl_module,
    config_module,
    loaded_registry,
):
    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": "warm"}),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_DLQ
    assert "quality" not in payload
    assert payload["error"].startswith("schema:")
    assert "temperature_c" in payload["error"]


def test_out_of_range_numeric_value_goes_to_dlq_with_schema_error(
    etl_module,
    config_module,
    loaded_registry,
):
    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": 999.0}),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_DLQ
    assert "quality" not in payload
    assert payload["error"].startswith("schema:")
    assert "temperature_c" in payload["error"]


def test_missing_values_goes_to_dlq(etl_module, config_module, loaded_registry):
    target, payload = etl_module.process_message(
        {
            "deviceName": "climate-lab-01",
            "deviceId": "dev-climate-01",
            "ts": "2024-06-01T12:00:00+00:00",
        },
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_DLQ
    assert "quality" not in payload
    assert payload["error"] == "missing or non-dict 'values'"


def test_epoch_millisecond_timestamp_is_converted_to_iso_timestamp(
    etl_module,
    config_module,
    loaded_registry,
):
    epoch_ms = 1_717_244_096_123
    expected = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat()

    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": 21.5}, ts=epoch_ms),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_TELEMETRY_VALIDATED
    assert payload["quality"] == "validated"
    assert "error" not in payload
    assert payload["ts"] == expected
    assert payload["ts"].endswith("+00:00")


def test_iso_timestamp_is_accepted(etl_module, config_module, loaded_registry):
    iso_ts = "2024-06-01T12:34:56Z"

    target, payload = etl_module.process_message(
        _climate_payload({"temperature_c": 21.5}, ts=iso_ts),
        {},
        reg=loaded_registry,
    )

    assert target == config_module.KAFKA_TOPIC_TELEMETRY_VALIDATED
    assert payload["quality"] == "validated"
    assert "error" not in payload
    assert payload["ts"] == iso_ts
