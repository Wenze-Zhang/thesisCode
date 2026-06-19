#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
REGISTRY_PATH = FAIR_BRIDGE_DIR / "registry" / "field_registry.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "tests" / "fixtures" / "labelled_telemetry_cases.jsonl"

if str(FAIR_BRIDGE_DIR) not in sys.path:
    sys.path.insert(0, str(FAIR_BRIDGE_DIR))
os.environ.setdefault("REGISTRY_PATH", str(REGISTRY_PATH))

import config  # noqa: E402
import registry  # noqa: E402


CATEGORIES = [
    "valid_canonical",
    "valid_alias",
    "epoch_timestamp",
    "iso_timestamp",
    "missing_values",
    "non_dict_values",
    "unknown_field",
    "wrong_datatype",
    "out_of_range",
]

DEVICE_NAMES = {
    "climate": "climate-lab-01",
    "energy": "energy-meter-main-01",
    "water": "water-meter-basement-01",
    "air_quality": "air-quality-room-01",
    "ev_charger": "ev-charger-bay-01",
}


def _valid_value(field: dict[str, Any], rng: random.Random) -> Any:
    datatype = field["datatype"]
    if datatype == "number":
        minimum = float(field.get("min", 0.0))
        maximum = float(field.get("max", minimum + 100.0))
        return round(rng.uniform(minimum, maximum), 3)
    if datatype == "integer":
        minimum = int(field.get("min", 0))
        maximum = int(field.get("max", minimum + 100))
        return rng.randint(minimum, maximum)
    if datatype == "string":
        enum = [value for value in field.get("enum", []) if isinstance(value, str)]
        if not enum:
            enum = ["ok"]
        return rng.choice(enum)
    if datatype == "boolean":
        return bool(rng.randint(0, 1))
    raise ValueError(f"Unsupported datatype: {datatype}")


def _wrong_type_value(field: dict[str, Any]) -> Any:
    datatype = field["datatype"]
    if datatype in {"number", "integer"}:
        return "not-a-number"
    if datatype == "string":
        return 123
    if datatype == "boolean":
        return "false"
    return None


def _out_of_range_value(field: dict[str, Any]) -> Any:
    datatype = field["datatype"]
    if datatype not in {"number", "integer"}:
        raise ValueError("Only numeric fields have range labels")
    if "max" in field:
        value = field["max"] + 1
    else:
        value = field.get("min", 0) - 1
    return int(value) if datatype == "integer" else float(value)


def _sensor_type(reg: registry.Registry, rng: random.Random) -> str:
    return rng.choice(sorted(reg.sensor_types.keys()))


def _device_name(sensor_type: str) -> str:
    return DEVICE_NAMES.get(sensor_type, f"{sensor_type}-device-01")


def _field_items(reg: registry.Registry, sensor_type: str) -> list[tuple[str, dict[str, Any]]]:
    return list(reg.sensor_types[sensor_type]["fields"].items())


def _numeric_field(reg: registry.Registry, sensor_type: str) -> tuple[str, dict[str, Any]]:
    for name, field in _field_items(reg, sensor_type):
        if field["datatype"] in {"number", "integer"}:
            return name, field
    raise ValueError(f"Sensor type has no numeric field: {sensor_type}")


def _aliased_field(reg: registry.Registry, sensor_type: str) -> tuple[str, str, dict[str, Any]]:
    for canonical, field in _field_items(reg, sensor_type):
        aliases = field.get("aliases") or []
        if aliases:
            return canonical, str(aliases[0]), field
    raise ValueError(f"Sensor type has no aliased field: {sensor_type}")


def _base_message(sensor_type: str, seq: int, values: Any, ts: Any) -> dict[str, Any]:
    return {
        "deviceName": _device_name(sensor_type),
        "deviceId": f"dev-{sensor_type.replace('_', '-')}-{seq:04d}",
        "ts": ts,
        "values": values,
    }


def _case(
    case_id: str,
    category: str,
    msg: dict[str, Any],
    *,
    expected_topic: str,
    expected_quality: str | None,
    expected_error_contains: str | None,
    expected_canonicalized: bool,
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "category": category,
        "input": msg,
        "expected_topic": expected_topic,
        "expected_quality": expected_quality,
        "expected_error_contains": expected_error_contains,
        "expected_canonicalized": expected_canonicalized,
    }


def build_case(reg: registry.Registry, seq: int, category: str, rng: random.Random) -> dict[str, Any]:
    case_id = f"C{seq:03d}"
    sensor_type = _sensor_type(reg, rng)
    timestamp = "2026-06-01T12:00:00+00:00"

    if category == "valid_canonical":
        name, field = rng.choice(_field_items(reg, sensor_type))
        msg = _base_message(sensor_type, seq, {name: _valid_value(field, rng)}, timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            expected_quality="validated",
            expected_error_contains=None,
            expected_canonicalized=False,
        )

    if category == "valid_alias":
        canonical, alias, field = _aliased_field(reg, sensor_type)
        msg = _base_message(sensor_type, seq, {alias: _valid_value(field, rng)}, timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            expected_quality="validated",
            expected_error_contains=None,
            expected_canonicalized=True,
        )

    if category == "epoch_timestamp":
        name, field = rng.choice(_field_items(reg, sensor_type))
        msg = _base_message(sensor_type, seq, {name: _valid_value(field, rng)}, 1_780_316_096_123)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            expected_quality="validated",
            expected_error_contains=None,
            expected_canonicalized=False,
        )

    if category == "iso_timestamp":
        name, field = rng.choice(_field_items(reg, sensor_type))
        msg = _base_message(sensor_type, seq, {name: _valid_value(field, rng)}, "2026-06-01T12:00:00Z")
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_TELEMETRY_VALIDATED,
            expected_quality="validated",
            expected_error_contains=None,
            expected_canonicalized=False,
        )

    if category == "missing_values":
        msg = {
            "deviceName": _device_name(sensor_type),
            "deviceId": f"dev-{sensor_type.replace('_', '-')}-{seq:04d}",
            "ts": timestamp,
        }
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_DLQ,
            expected_quality=None,
            expected_error_contains="missing or non-dict",
            expected_canonicalized=False,
        )

    if category == "non_dict_values":
        msg = _base_message(sensor_type, seq, ["not", "an", "object"], timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_DLQ,
            expected_quality=None,
            expected_error_contains="missing or non-dict",
            expected_canonicalized=False,
        )

    if category == "unknown_field":
        name, field = rng.choice(_field_items(reg, sensor_type))
        values = {name: _valid_value(field, rng), f"unknown_{seq}": 1}
        msg = _base_message(sensor_type, seq, values, timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_DLQ,
            expected_quality=None,
            expected_error_contains="unknown fields",
            expected_canonicalized=False,
        )

    if category == "wrong_datatype":
        name, field = rng.choice(_field_items(reg, sensor_type))
        msg = _base_message(sensor_type, seq, {name: _wrong_type_value(field)}, timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_DLQ,
            expected_quality=None,
            expected_error_contains="schema:",
            expected_canonicalized=False,
        )

    if category == "out_of_range":
        name, field = _numeric_field(reg, sensor_type)
        msg = _base_message(sensor_type, seq, {name: _out_of_range_value(field)}, timestamp)
        return _case(
            case_id,
            category,
            msg,
            expected_topic=config.KAFKA_TOPIC_DLQ,
            expected_quality=None,
            expected_error_contains="schema:",
            expected_canonicalized=False,
        )

    raise ValueError(f"Unknown category: {category}")


def generate_cases(count: int, seed: int) -> list[dict[str, Any]]:
    reg = registry.load_registry(str(REGISTRY_PATH))
    rng = random.Random(seed)
    cases = []
    for seq in range(1, count + 1):
        category = CATEGORIES[(seq - 1) % len(CATEGORIES)]
        cases.append(build_case(reg, seq, category, rng))
    return cases


def write_jsonl(cases: list[dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as fh:
        for case in cases:
            fh.write(json.dumps(case, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate labelled synthetic telemetry cases.")
    parser.add_argument("--count", type=int, default=180, help="Number of labelled cases to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible values.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output JSONL path.")
    args = parser.parse_args()

    if args.count < len(CATEGORIES):
        raise SystemExit(f"--count must be at least {len(CATEGORIES)} to cover every category")

    cases = generate_cases(args.count, args.seed)
    write_jsonl(cases, args.output)
    print(f"Wrote {len(cases)} labelled cases to {args.output}")
    print(f"Generated at {datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
