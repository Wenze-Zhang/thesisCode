from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
FAIR_BRIDGE_DIR = REPO_ROOT / "fair-bridge"
REGISTRY_PATH = FAIR_BRIDGE_DIR / "registry" / "field_registry.yaml"

os.environ.setdefault("REGISTRY_PATH", str(REGISTRY_PATH))

if str(FAIR_BRIDGE_DIR) not in sys.path:
    sys.path.insert(0, str(FAIR_BRIDGE_DIR))


def _install_optional_dependency_stubs() -> None:
    """Allow importing pure helpers from modules with Kafka/CKAN imports.

    The production Docker image installs these dependencies. Local unit tests in
    this repository only exercise pure functions, so minimal stubs are enough
    when the packages are not installed on the host.
    """
    try:
        import ckanapi  # noqa: F401
    except ImportError:
        ckanapi_module = types.ModuleType("ckanapi")
        ckanapi_errors = types.ModuleType("ckanapi.errors")

        class RemoteCKAN:  # pragma: no cover - only used for importability
            def __init__(self, *args, **kwargs):
                raise RuntimeError("ckanapi is not installed")

        class CKANAPIError(Exception):
            pass

        class NotFound(CKANAPIError):
            pass

        class ValidationError(CKANAPIError):
            pass

        ckanapi_module.RemoteCKAN = RemoteCKAN
        ckanapi_errors.CKANAPIError = CKANAPIError
        ckanapi_errors.NotFound = NotFound
        ckanapi_errors.ValidationError = ValidationError
        sys.modules.setdefault("ckanapi", ckanapi_module)
        sys.modules.setdefault("ckanapi.errors", ckanapi_errors)

    try:
        import kafka  # noqa: F401
    except ImportError:
        kafka_module = types.ModuleType("kafka")
        kafka_errors = types.ModuleType("kafka.errors")
        kafka_structs = types.ModuleType("kafka.structs")

        class KafkaConsumer:  # pragma: no cover - only used for importability
            def __init__(self, *args, **kwargs):
                raise RuntimeError("kafka-python is not installed")

        class NoBrokersAvailable(Exception):
            pass

        class OffsetAndMetadata:
            def __init__(self, offset, metadata):
                self.offset = offset
                self.metadata = metadata

        class TopicPartition:
            def __init__(self, topic, partition):
                self.topic = topic
                self.partition = partition

        kafka_module.KafkaConsumer = KafkaConsumer
        kafka_errors.NoBrokersAvailable = NoBrokersAvailable
        kafka_structs.OffsetAndMetadata = OffsetAndMetadata
        kafka_structs.TopicPartition = TopicPartition
        sys.modules.setdefault("kafka", kafka_module)
        sys.modules.setdefault("kafka.errors", kafka_errors)
        sys.modules.setdefault("kafka.structs", kafka_structs)


def bridge_module(name: str):
    _install_optional_dependency_stubs()
    module = importlib.import_module(name)
    config = sys.modules.get("config")
    if config is not None:
        config.REGISTRY_PATH = str(REGISTRY_PATH)
    return module


@pytest.fixture(scope="session")
def config_module():
    return bridge_module("config")


@pytest.fixture(scope="session")
def registry_module():
    return bridge_module("registry")


@pytest.fixture(scope="session")
def loaded_registry(registry_module):
    return registry_module.load_registry(str(REGISTRY_PATH))


@pytest.fixture(scope="session")
def etl_module():
    return bridge_module("etl")


@pytest.fixture(scope="session")
def exporter_module():
    return bridge_module("telemetry_exporter")


@pytest.fixture(scope="session")
def enricher_module():
    return bridge_module("enricher")
