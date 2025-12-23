"""Database operations for CDC consumer."""

from datetime import datetime
from typing import Any, Optional

import psycopg
from psycopg.types.json import Json

import sys
from pathlib import Path

# Add shared module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from shared import DatalakeConfig, TargetConfig, CDCEvent, CDCKey

# Timestamp field names that Debezium sends as microseconds
TIMESTAMP_FIELDS = {"created_at", "updated_at", "order_date", "source_created_at", "source_updated_at"}


def convert_debezium_timestamp(value: Any) -> Any:
    """Convert Debezium timestamp (microseconds since epoch) to datetime."""
    if value is None:
        return None
    if isinstance(value, int):
        # Debezium sends timestamps as microseconds since epoch
        return datetime.fromtimestamp(value / 1_000_000)
    return value


class DatabaseHandler:
    """Handles database operations for CDC events."""

    def __init__(
        self, datalake_config: DatalakeConfig, target_config: TargetConfig
    ) -> None:
        self.datalake_config = datalake_config
        self.target_config = target_config
        self._datalake_conn: Optional[psycopg.Connection] = None
        self._target_conn: Optional[psycopg.Connection] = None

    def connect_datalake(self) -> psycopg.Connection:
        """Connect to data lake database."""
        if self._datalake_conn is None or self._datalake_conn.closed:
            self._datalake_conn = psycopg.connect(
                host=self.datalake_config.host,
                port=self.datalake_config.port,
                dbname=self.datalake_config.database,
                user=self.datalake_config.user,
                password=self.datalake_config.password,
                autocommit=False,
            )
        return self._datalake_conn

    def connect_target(self) -> psycopg.Connection:
        """Connect to target database."""
        if self._target_conn is None or self._target_conn.closed:
            self._target_conn = psycopg.connect(
                host=self.target_config.host,
                port=self.target_config.port,
                dbname=self.target_config.database,
                user=self.target_config.user,
                password=self.target_config.password,
                autocommit=False,
            )
        return self._target_conn

    def _rollback_datalake(self) -> None:
        """Rollback data lake connection if in failed state."""
        if self._datalake_conn and not self._datalake_conn.closed:
            try:
                self._datalake_conn.rollback()
            except Exception:
                pass

    def _rollback_target(self) -> None:
        """Rollback target connection if in failed state."""
        if self._target_conn and not self._target_conn.closed:
            try:
                self._target_conn.rollback()
            except Exception:
                pass

    def store_raw_event(
        self,
        event: CDCEvent,
        key: CDCKey,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Store raw CDC event in data lake."""
        conn = self.connect_datalake()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdc_raw.cdc_events (
                        source_table, operation, event_key, before_data, after_data,
                        source_metadata, kafka_topic, kafka_partition, kafka_offset,
                        event_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event.full_table_name,
                        event.operation,
                        Json(key.payload),
                        Json(event.before) if event.before else None,
                        Json(event.after) if event.after else None,
                        Json(
                            {
                                "version": event.source.version,
                                "connector": event.source.connector,
                                "name": event.source.name,
                                "db": event.source.db,
                                "schema": event.source.schema,
                                "table": event.source.table,
                                "txId": event.source.txId,
                                "lsn": event.source.lsn,
                            }
                        ),
                        topic,
                        partition,
                        offset,
                        event.event_timestamp,
                    ),
                )
            conn.commit()
        except Exception:
            self._rollback_datalake()
            raise

    def apply_to_target(self, event: CDCEvent) -> None:
        """Apply CDC event to target database."""
        table = event.table_name
        operation = event.operation
        conn = self.connect_target()

        try:
            with conn.cursor() as cur:
                if operation == "d":
                    # DELETE operation
                    if event.before and "id" in event.before:
                        cur.execute(
                            f"DELETE FROM public.{table} WHERE id = %s",
                            (event.before["id"],),
                        )
                elif operation in ("c", "r"):
                    # INSERT operation (create or snapshot)
                    if event.after:
                        self._upsert_record(cur, table, event)
                elif operation == "u":
                    # UPDATE operation
                    if event.after:
                        self._upsert_record(cur, table, event)

            conn.commit()
        except Exception:
            self._rollback_target()
            raise

    def _upsert_record(
        self, cur: psycopg.Cursor, table: str, event: CDCEvent
    ) -> None:
        """Upsert record into target table."""
        data = event.after.copy() if event.after else {}
        if not data:
            return

        # Convert timestamp fields from Debezium format
        for field in list(data.keys()):
            if field in TIMESTAMP_FIELDS or field.endswith("_at"):
                data[field] = convert_debezium_timestamp(data[field])

        # Add CDC metadata
        data["__cdc_operation"] = event.operation
        data["__cdc_timestamp"] = event.event_timestamp

        # Build dynamic upsert query
        columns = list(data.keys())
        placeholders = ["%s"] * len(columns)
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

        query = f"""
            INSERT INTO public.{table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (id) DO UPDATE SET {update_set}, __synced_at = CURRENT_TIMESTAMP
        """

        values = [self._convert_value(data[col]) for col in columns]
        cur.execute(query, values)

    def _convert_value(self, value: Any) -> Any:
        """Convert value for PostgreSQL insertion."""
        if isinstance(value, dict):
            return Json(value)
        return value

    def apply_to_datalake_processed(self, event: CDCEvent) -> None:
        """Apply CDC event to processed tables in data lake."""
        table = event.table_name
        operation = event.operation
        conn = self.connect_datalake()

        try:
            with conn.cursor() as cur:
                if operation == "d":
                    # DELETE operation - remove from processed table
                    if event.before and "id" in event.before:
                        cur.execute(
                            f"DELETE FROM cdc_processed.{table} WHERE id = %s",
                            (event.before["id"],),
                        )
                elif operation in ("c", "r", "u"):
                    # INSERT/UPDATE operation
                    if event.after:
                        self._upsert_processed_record(cur, table, event)

            conn.commit()
        except Exception:
            self._rollback_datalake()
            raise

    def _upsert_processed_record(
        self, cur: psycopg.Cursor, table: str, event: CDCEvent
    ) -> None:
        """Upsert record into processed table in data lake."""
        data = event.after.copy() if event.after else {}
        if not data:
            return

        # Convert timestamp fields and map source timestamps
        data["cdc_operation"] = event.operation
        data["cdc_timestamp"] = event.event_timestamp

        if "created_at" in data:
            data["source_created_at"] = convert_debezium_timestamp(data.pop("created_at"))
        if "updated_at" in data:
            data["source_updated_at"] = convert_debezium_timestamp(data.pop("updated_at"))

        # Convert any remaining timestamp fields
        for field in list(data.keys()):
            if field.endswith("_at") or field.endswith("_date"):
                if isinstance(data[field], int):
                    data[field] = convert_debezium_timestamp(data[field])

        # Build dynamic upsert query
        columns = list(data.keys())
        placeholders = ["%s"] * len(columns)
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

        query = f"""
            INSERT INTO cdc_processed.{table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (id) DO UPDATE SET {update_set}, synced_at = CURRENT_TIMESTAMP
        """

        values = [self._convert_value(data[col]) for col in columns]
        cur.execute(query, values)

    def close(self) -> None:
        """Close database connections."""
        if self._datalake_conn and not self._datalake_conn.closed:
            self._datalake_conn.close()
        if self._target_conn and not self._target_conn.closed:
            self._target_conn.close()
