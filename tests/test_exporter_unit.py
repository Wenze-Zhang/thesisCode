from __future__ import annotations

import csv
import json
import re


def _payload(values, *, canonicalized=None, ts="2026-06-01T12:00:00+00:00"):
    payload = {
        "ts": ts,
        "device_name": "Climate Lab 01",
        "device_id": "dev-climate-01",
        "sensor_type": "climate",
        "quality": "validated",
        "values": values,
    }
    if canonicalized is not None:
        payload["canonicalized"] = canonicalized
    return payload


def _read_csv(path):
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return reader.fieldnames or [], list(reader)


def test_append_telemetry_to_csv_creates_expected_dataset_day_path(
    exporter_module,
    tmp_path,
):
    dataset_slug, day, csv_path = exporter_module.append_telemetry_to_csv(
        _payload({"temperature_c": 21.5}),
        tmp_path,
    )

    assert dataset_slug == "sensor-climate-lab-01"
    assert day == "2026-06-01"
    assert csv_path == tmp_path / dataset_slug / "2026-06-01.csv"
    assert csv_path.exists()


def test_csv_includes_base_columns(exporter_module, tmp_path):
    _, _, csv_path = exporter_module.append_telemetry_to_csv(
        _payload({"temperature_c": 21.5}),
        tmp_path,
    )

    fieldnames, _ = _read_csv(csv_path)

    assert fieldnames[:5] == ["ts", "device_name", "device_id", "sensor_type", "quality"]


def test_dynamic_telemetry_value_keys_become_csv_columns(exporter_module, tmp_path):
    _, _, csv_path = exporter_module.append_telemetry_to_csv(
        _payload({"temperature_c": 21.5, "humidity_pct": 45.0}),
        tmp_path,
    )

    fieldnames, rows = _read_csv(csv_path)

    assert "temperature_c" in fieldnames
    assert "humidity_pct" in fieldnames
    assert rows[0]["temperature_c"] == "21.5"
    assert rows[0]["humidity_pct"] == "45.0"


def test_later_new_field_expands_header_without_losing_existing_rows(
    exporter_module,
    tmp_path,
):
    _, _, csv_path = exporter_module.append_telemetry_to_csv(
        _payload({"temperature_c": 21.5}),
        tmp_path,
    )
    exporter_module.append_telemetry_to_csv(
        _payload({"humidity_pct": 46.0}, ts="2026-06-01T12:01:00+00:00"),
        tmp_path,
    )

    fieldnames, rows = _read_csv(csv_path)

    assert "temperature_c" in fieldnames
    assert "humidity_pct" in fieldnames
    assert len(rows) == 2
    assert rows[0]["temperature_c"] == "21.5"
    assert rows[0]["humidity_pct"] == ""
    assert rows[1]["temperature_c"] == ""
    assert rows[1]["humidity_pct"] == "46.0"


def test_canonicalized_column_is_preserved(exporter_module, tmp_path):
    _, _, csv_path = exporter_module.append_telemetry_to_csv(
        _payload(
            {"temperature_c": 22.0},
            canonicalized={"temp_c": "temperature_c"},
        ),
        tmp_path,
    )

    fieldnames, rows = _read_csv(csv_path)

    assert "canonicalized" in fieldnames
    assert json.loads(rows[0]["canonicalized"]) == {"temp_c": "temperature_c"}


def test_discover_pending_csvs_finds_sensor_csvs(exporter_module, tmp_path):
    dataset_slug, day, csv_path = exporter_module.append_telemetry_to_csv(
        _payload({"temperature_c": 21.5}),
        tmp_path,
    )
    ignored_dir = tmp_path / "not-a-sensor"
    ignored_dir.mkdir()
    (ignored_dir / "2026-06-01.csv").write_text("ignored\n", encoding="utf-8")

    pending = exporter_module.discover_pending_csvs(tmp_path)

    assert pending == {(dataset_slug, day, csv_path)}


def test_dataset_slug_generation_is_deterministic_and_safe(exporter_module):
    first = exporter_module.dataset_slug_for_device("Climate Lab 01")
    second = exporter_module.dataset_slug_for_device("Climate Lab 01")

    assert first == second
    assert first == "sensor-climate-lab-01"
    assert re.fullmatch(r"[a-z0-9-]+", first)
    assert exporter_module.slugify("!") == "sensor-unnamed"
