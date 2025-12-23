"""Delta Lake operations for CDC events."""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from models import CDCEvent, CDCKey


# Define explicit schema for CDC events table to avoid Null type issues
CDC_EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("source_table", pa.string()),
    ("operation", pa.string()),
    ("operation_name", pa.string()),
    ("record_id", pa.int64()),
    ("before_data", pa.string()),
    ("after_data", pa.string()),
    ("kafka_topic", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("event_timestamp", pa.timestamp("us")),
    ("processed_at", pa.timestamp("us")),
    ("source_db", pa.string()),
    ("source_schema", pa.string()),
    ("source_lsn", pa.string()),
    ("source_txid", pa.int64()),
])


class DeltaLakeHandler:
    """Handles Delta Lake operations for CDC events."""

    def __init__(self, base_path: str = "../deltalake") -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_table_path(self, table_name: str) -> str:
        """Get the Delta table path for a given table."""
        return str(self.base_path / table_name)

    def _convert_timestamp(self, value: Any) -> Optional[datetime]:
        """Convert Debezium timestamp (microseconds) to datetime."""
        if value is None:
            return None
        if isinstance(value, int):
            return datetime.fromtimestamp(value / 1_000_000)
        return value

    def _safe_str(self, value: Any) -> str:
        """Convert value to string, handling None as empty string."""
        if value is None:
            return ""
        return str(value)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Convert value to int, handling None."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def write_cdc_event(
        self,
        event: CDCEvent,
        key: CDCKey,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Write a CDC event to the raw events Delta table."""
        table_path = self._get_table_path("cdc_events")

        # Create PyArrow table with explicit schema to avoid Null type issues
        data = {
            "event_id": [f"{topic}-{partition}-{offset}"],
            "source_table": [event.full_table_name or ""],
            "operation": [event.operation or ""],
            "operation_name": [event.operation_name or ""],
            "record_id": [self._safe_int(key.id)],
            "before_data": [str(event.before) if event.before else ""],
            "after_data": [str(event.after) if event.after else ""],
            "kafka_topic": [topic or ""],
            "kafka_partition": [partition],
            "kafka_offset": [offset],
            "event_timestamp": [event.event_timestamp or datetime.now()],
            "processed_at": [datetime.now()],
            "source_db": [event.source.db or ""],
            "source_schema": [event.source.schema or ""],
            "source_lsn": [self._safe_str(event.source.lsn)],
            "source_txid": [self._safe_int(event.source.txId)],
        }

        table = pa.table(data, schema=CDC_EVENTS_SCHEMA)

        write_deltalake(
            table_path,
            table,
            mode="append",
            schema_mode="merge",
        )

    def write_table_snapshot(
        self,
        table_name: str,
        event: CDCEvent,
    ) -> None:
        """
        Write/merge CDC event to a table-specific Delta table.
        This maintains current state with full history via Delta versioning.
        """
        if not event.after and event.operation != "d":
            return

        table_path = self._get_table_path(table_name)

        if event.operation == "d":
            self._handle_delete(table_path, event)
        else:
            self._handle_upsert(table_path, table_name, event)

    def _infer_pyarrow_type(self, value: Any) -> pa.DataType:
        """Infer PyArrow type from Python value, defaulting to string for None."""
        if value is None:
            return pa.string()
        if isinstance(value, bool):
            return pa.bool_()
        if isinstance(value, int):
            return pa.int64()
        if isinstance(value, float):
            return pa.float64()
        if isinstance(value, datetime):
            return pa.timestamp("us")
        return pa.string()

    def _convert_value_for_arrow(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Convert a Python value to be compatible with the given Arrow type."""
        if value is None:
            if pa.types.is_string(arrow_type):
                return ""
            if pa.types.is_integer(arrow_type):
                return 0
            if pa.types.is_floating(arrow_type):
                return 0.0
            if pa.types.is_boolean(arrow_type):
                return False
            if pa.types.is_timestamp(arrow_type):
                return datetime.now()
            return ""
        return value

    def _handle_upsert(
        self, table_path: str, table_name: str, event: CDCEvent
    ) -> None:
        """Handle INSERT or UPDATE operation using merge."""
        data = event.after.copy()

        # Convert timestamp fields
        for field in list(data.keys()):
            if field.endswith("_at") or field.endswith("_date"):
                data[field] = self._convert_timestamp(data[field])

        # Add CDC metadata
        data["__cdc_operation"] = event.operation
        data["__cdc_timestamp"] = event.event_timestamp
        data["__cdc_version"] = self._safe_str(event.source.lsn)
        data["__processed_at"] = datetime.now()

        # Build schema from data, inferring types from non-None values
        # CRITICAL: Replace None values with typed defaults BEFORE schema inference
        schema_fields = []
        cleaned_data = {}
        for key, value in data.items():
            arrow_type = self._infer_pyarrow_type(value)
            schema_fields.append((key, arrow_type))
            # Convert to typed default immediately to prevent pa.null() inference
            converted_value = self._convert_value_for_arrow(value, arrow_type)
            cleaned_data[key] = converted_value
        
        schema = pa.schema(schema_fields)

        # Create arrow data with converted values wrapped in lists
        arrow_data = {key: [value] for key, value in cleaned_data.items()}

        new_data = pa.table(arrow_data, schema=schema)

        try:
            # Try to merge if table exists
            dt = DeltaTable(table_path)
            (
                dt.merge(
                    source=new_data,
                    predicate="target.id = source.id",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception:
            # Table doesn't exist, create it
            write_deltalake(table_path, new_data, mode="append")

    def _handle_delete(self, table_path: str, event: CDCEvent) -> None:
        """Handle DELETE operation."""
        if not event.before or "id" not in event.before:
            return

        try:
            dt = DeltaTable(table_path)
            dt.delete(predicate=f"id = {event.before['id']}")
        except Exception:
            # Table doesn't exist, nothing to delete
            pass

    def get_table_history(self, table_name: str) -> list:
        """Get the history of changes for a Delta table."""
        table_path = self._get_table_path(table_name)
        try:
            dt = DeltaTable(table_path)
            return dt.history()
        except Exception:
            return []

    def get_table_at_version(
        self, table_name: str, version: int
    ) -> Optional[pa.Table]:
        """Time travel: get table state at a specific version."""
        table_path = self._get_table_path(table_name)
        try:
            dt = DeltaTable(table_path, version=version)
            return dt.to_pyarrow_table()
        except Exception:
            return None

    def get_table_at_timestamp(
        self, table_name: str, timestamp: str
    ) -> Optional[pa.Table]:
        """Time travel: get table state at a specific timestamp."""
        table_path = self._get_table_path(table_name)
        try:
            dt = DeltaTable(table_path)
            history = dt.history()
            target_ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

            for entry in history:
                if entry.get("timestamp", datetime.max) <= target_ts:
                    return self.get_table_at_version(
                        table_name, entry["version"]
                    )
            return None
        except Exception:
            return None

    def get_current_table(self, table_name: str) -> Optional[pa.Table]:
        """Get current state of a Delta table."""
        table_path = self._get_table_path(table_name)
        try:
            dt = DeltaTable(table_path)
            return dt.to_pyarrow_table()
        except Exception:
            return None

    def vacuum(self, table_name: str, retention_hours: int = 168) -> None:
        """
        Remove old files from Delta table.
        Default retention: 7 days (168 hours)
        """
        table_path = self._get_table_path(table_name)
        try:
            dt = DeltaTable(table_path)
            dt.vacuum(retention_hours=retention_hours)
        except Exception:
            pass
