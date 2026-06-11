#!/usr/bin/env python3
from __future__ import annotations
import os
from typing import Any, Iterable
import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonValidationError


_JSON_TYPE = {
    "number": "number",
    "integer": "integer",
    "string": "string",
    "boolean": "boolean",
}


class RegistryError(RuntimeError):
    pass


class Registry:
    def __init__(self, raw: dict[str, Any]):
        sensor_types = raw.get("sensor_types")
        
        # ensure the top of yaml isn't empty
        if not isinstance(sensor_types, dict) or not sensor_types:
            raise RegistryError("registry has no sensor_types")

        # derivation from yaml -> validation structures
        self.sensor_types = sensor_types  
        self.alias_to_canonical: dict[str, str] = {}
        self.canonical_keys: set[str] = set()
        self._name_aliases: list[tuple[str, list[str]]] = []

        
        for sensor_type, device_type in sensor_types.items():
            # sensor type must be dict
            if not isinstance(device_type, dict):
                raise RegistryError(f"sensor_type {sensor_type!r} is not a mapping")
            
            # fields must be a non empty dict
            fields = device_type.get("fields")
            if not isinstance(fields, dict) or not fields:
                raise RegistryError(f"sensor_type {sensor_type!r} has no fields")
            
            # primary field can't be empty and must be in fields
            primary = device_type.get("primary_field")
            if primary and primary not in fields:
                raise RegistryError(
                    f"sensor_type {sensor_type!r} primary_field {primary!r} is not in fields"
                )
                
                
            # collect aliases of sensor type names for classification
            self._name_aliases.append((
                sensor_type,
                # lowercase aliases
                [str(alias).lower() for alias in device_type.get("name_aliases") or []],
            ))
            
            
            # validate fields and build alias mapping for field names
            for field_name, field in fields.items():
                # must be a dict
                if not isinstance(field, dict):
                    raise RegistryError(
                        f"field {sensor_type}.{field_name} is not a mapping"
                    )
                # datatype must be supported
                if field.get("datatype") not in _JSON_TYPE:
                    raise RegistryError(
                        f"field {sensor_type}.{field_name} has unsupported datatype"
                    )
                
                self._register_alias(field_name, field_name)
                for alias in field.get("aliases") or []:
                    self._register_alias(str(alias), field_name)


        # build JSON schemas and validators for each sensor type
        self.raw_schemas = self._build_schemas()    # JSON schemas dict for each sensor type
        self.validators = {
            sensor_type: Draft202012Validator(schema)
            for sensor_type, schema in self.raw_schemas.items()
        }

    def _register_alias(self, token: str, canonical: str) -> None:
        
        # lowercase
        key = token.lower()
        existing = self.alias_to_canonical.get(key)
        
        # in case an alias maps to two different canonical keys
        if existing is not None and existing != canonical:
            raise RegistryError(
                f"alias {token!r} maps to both {existing!r} and {canonical!r}"
            )
            
            
        self.alias_to_canonical[key] = canonical
        self.canonical_keys.add(canonical)




    def _build_schemas(self) -> dict[str, dict]:
        schemas = {}
        
        # YAML → JSON Schema
        for sensor_type, device_type in self.sensor_types.items():
            properties = {}
            for field_name, field in device_type["fields"].items():
                # type restriction
                datatype = field["datatype"]
                prop: dict[str, Any] = {"type": _JSON_TYPE[datatype]}
                
                # range restriction
                if datatype in ("number", "integer"):
                    if "min" in field:
                        prop["minimum"] = field["min"]
                    if "max" in field:
                        prop["maximum"] = field["max"]
                # enum restriction
                elif datatype == "string" and field.get("enum"):
                    prop["enum"] = field["enum"]
                properties[field_name] = prop
                
            schemas[sensor_type] = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": f"fair-bridge://registry/{sensor_type}.json",
                "type": "object",
                "additionalProperties": False,  # unknown to dlq
                "properties": properties,
            }
        return schemas


    def canonicalize(self, values: dict) -> tuple[dict, dict, list]:
        # store canonicalized keys and values. temp-> temperature
        canonicalized: dict[str, Any] = {}
        # record renamed mapping for telemetry keys.
        renamed: dict[str, str] = {}
        # record unknown keys 
        unknown: list[str] = []
        
        for key, value in (values or {}).items():
            canonical = self.alias_to_canonical.get(str(key).lower())
            
            if canonical is None:
                canonicalized[key] = value
                unknown.append(key)   # unknown to dlq in etl
                continue
        
            if canonical != key:
                renamed[key] = canonical
            canonicalized[canonical] = value
        return canonicalized, renamed, unknown

    # classification of device type by name
    def classify(self, device_name: str) -> str:
        name = (device_name or "").lower()
        for sensor_type, aliases in self._name_aliases:
            if any(alias and alias in name for alias in aliases):
                return sensor_type
        return "other"


    # validation with JSON schema
    def validate(self, values: dict, sensor_type: str) -> str | None:
        validator = self.validators.get(sensor_type)
        if validator is None:
            return f"no schema for sensor_type={sensor_type!r}"
        try:
            validator.validate(values)
            return None
        except JsonValidationError as exc:
            path = "/".join(str(part) for part in exc.absolute_path) or "<root>"
            return f"{path}: {exc.message}"


    def get_device_type(self, sensor_type: str) -> dict:
        return self.sensor_types.get(sensor_type, {})



    def get_field(self, sensor_type: str, field_name: str) -> dict | None:
        device_type = self.get_device_type(sensor_type)
        fields = device_type.get("fields") or {}
        canonical = self.alias_to_canonical.get(str(field_name).lower(), field_name)
        return fields.get(canonical)


    def get_primary_field(self, sensor_type: str) -> str | None:
        device_type = self.get_device_type(sensor_type)
        primary = device_type.get("primary_field")
        return primary if primary in (device_type.get("fields") or {}) else None


    def numeric_keys(self, sensor_type: str, payload_keys: Iterable[str]) -> list[str]:
        keys = []
        for key in payload_keys:
            canonical = self.alias_to_canonical.get(str(key).lower(), key)
            field = self.get_field(sensor_type, canonical)
            if field and field.get("datatype") in ("number", "integer"):
                keys.append(canonical)
        return keys


def load_registry(path: str) -> Registry:
    if not os.path.isfile(path):
        raise RegistryError(f"registry file not found: {path}")
    with open(path, encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise RegistryError(f"registry file is not a mapping: {path}")
    return Registry(raw)
