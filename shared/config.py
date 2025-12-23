"""Configuration settings for CDC consumer."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Get project root (parent of python-consumer directory)
_PROJECT_ROOT = Path(__file__).parent.parent
_DEFAULT_DELTA_PATH = str(_PROJECT_ROOT / "deltalake")


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""

    bootstrap_servers: str = "localhost:29092"
    group_id: str = "cdc-python-consumer"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            group_id=os.getenv("KAFKA_GROUP_ID", "cdc-python-consumer"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower()
            == "true",
        )


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class DatalakeConfig(PostgresConfig):
    """Data Lake PostgreSQL configuration."""

    host: str = "localhost"
    port: int = 5434
    database: str = "datalake_db"
    user: str = "datalake_user"
    password: str = "datalake_password"

    @classmethod
    def from_env(cls) -> "DatalakeConfig":
        return cls(
            host=os.getenv("DATALAKE_HOST", "localhost"),
            port=int(os.getenv("DATALAKE_PORT", "5434")),
            database=os.getenv("DATALAKE_DATABASE", "datalake_db"),
            user=os.getenv("DATALAKE_USER", "datalake_user"),
            password=os.getenv("DATALAKE_PASSWORD", "datalake_password"),
        )


@dataclass
class TargetConfig(PostgresConfig):
    """Target PostgreSQL configuration."""

    host: str = "localhost"
    port: int = 5435
    database: str = "target_db"
    user: str = "target_user"
    password: str = "target_password"

    @classmethod
    def from_env(cls) -> "TargetConfig":
        return cls(
            host=os.getenv("TARGET_HOST", "localhost"),
            port=int(os.getenv("TARGET_PORT", "5435")),
            database=os.getenv("TARGET_DATABASE", "target_db"),
            user=os.getenv("TARGET_USER", "target_user"),
            password=os.getenv("TARGET_PASSWORD", "target_password"),
        )


# Topic mapping: Debezium topic -> table name
TOPIC_TABLE_MAPPING = {
    "cdc.public.customers": "customers",
    "cdc.public.products": "products",
    "cdc.public.orders": "orders",
    "cdc.public.order_items": "order_items",
}

# Topics to subscribe
CDC_TOPICS = list(TOPIC_TABLE_MAPPING.keys())


@dataclass
class DeltaLakeConfig:
    """Delta Lake configuration."""

    base_path: str = _DEFAULT_DELTA_PATH
    enable_cdc_events: bool = True
    enable_table_snapshots: bool = True
    retention_hours: int = 168                      # 7 days
    use_spark: bool = False                         # Enable PySpark mode
    spark_master: str = "local[*]"                  # Spark master URL

    @classmethod
    def from_env(cls) -> "DeltaLakeConfig":
        return cls(
            base_path=os.getenv("DELTA_LAKE_PATH", _DEFAULT_DELTA_PATH),
            enable_cdc_events=os.getenv("DELTA_ENABLE_CDC_EVENTS", "true").lower()
            == "true",
            enable_table_snapshots=os.getenv("DELTA_ENABLE_SNAPSHOTS", "true").lower()
            == "true",
            retention_hours=int(os.getenv("DELTA_RETENTION_HOURS", "168")),
            use_spark=os.getenv("DELTA_USE_SPARK", "false").lower() == "true",
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
        )
