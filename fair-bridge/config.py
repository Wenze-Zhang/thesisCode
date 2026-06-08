import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_LIFECYCLE = os.getenv("KAFKA_TOPIC_LIFECYCLE", "tb.device.lifecycle")
KAFKA_TOPIC_TELEMETRY_RAW = os.getenv("KAFKA_TOPIC_TELEMETRY_RAW", "tb.telemetry.raw")
KAFKA_TOPIC_TELEMETRY_VALIDATED = os.getenv(
    "KAFKA_TOPIC_TELEMETRY_VALIDATED",
    "tb.telemetry.validated",
)
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "bridge.dlq")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fair-bridge")
KAFKA_CONSUMER_GROUP_ETL = os.getenv("KAFKA_CONSUMER_GROUP_ETL", "fair-bridge-etl")
KAFKA_CONSUMER_GROUP_EXPORTER = os.getenv(
    "KAFKA_CONSUMER_GROUP_EXPORTER",
    "fair-bridge-telemetry-exporter",
)

REGISTRY_PATH = os.getenv("REGISTRY_PATH", "/app/registry/field_registry.yaml")

EXPORT_DIR = os.getenv("EXPORT_DIR", "/app/exports")
EXPORT_FLUSH_INTERVAL_S = int(os.getenv("EXPORT_FLUSH_INTERVAL_S", "30"))
EXPORT_RESOURCE_DESCRIPTION = os.getenv(
    "EXPORT_RESOURCE_DESCRIPTION",
    "Daily validated telemetry exported from tb.telemetry.validated.",
)

CKAN_URL = os.getenv("CKAN_URL", "http://ckan:5000")
CKAN_API_KEY = os.getenv("CKAN_API_KEY", "")
CKAN_ORG = os.getenv("CKAN_ORG", "acs-eonerc")
CKAN_ORG_TITLE = os.getenv("CKAN_ORG_TITLE", "ACS EONERC")

TB_URL = os.getenv("TB_URL", "http://thingsboard:8080")
TB_USERNAME = os.getenv("TB_USERNAME", "tenant@thingsboard.org")
TB_PASSWORD = os.getenv("TB_PASSWORD", "tenant")

CKAN_READY_TIMEOUT_S = int(os.getenv("CKAN_READY_TIMEOUT_S", "300"))
KAFKA_READY_TIMEOUT_S = int(os.getenv("KAFKA_READY_TIMEOUT_S", "120"))
