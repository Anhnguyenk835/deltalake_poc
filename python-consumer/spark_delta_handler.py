"""Simple PySpark Delta Lake handler for CDC events."""

from datetime import datetime
from typing import Any, Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, TimestampType
)
from delta import DeltaTable, configure_spark_with_delta_pip

from models import CDCEvent, CDCKey


# Explicit schema for CDC events table
CDC_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("record_id", LongType(), True),
    StructField("before_data", StringType(), True),
    StructField("after_data", StringType(), True),
    StructField("kafka_topic", StringType(), False),
    StructField("kafka_partition", IntegerType(), False),
    StructField("kafka_offset", LongType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("processed_at", TimestampType(), False),
])


def create_spark_session(
    app_name: str = "CDC-DeltaLake",
    spark_master: str = "local[*]",
    delta_path: str = "./deltalake",
) -> SparkSession:
    """Create SparkSession with Delta Lake support - minimal config."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", delta_path)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class SparkDeltaHandler:
    """Simple PySpark Delta Lake handler."""

    def __init__(
        self,
        base_path: str = "./deltalake",
        spark_master: str = "local[*]",
    ) -> None:
        self.base_path = base_path
        self.spark = create_spark_session(
            spark_master=spark_master,
            delta_path=base_path,
        )

    def _table_path(self, table: str) -> str:
        return f"{self.base_path}/{table}"

    def _convert_ts(self, val: Any) -> Optional[datetime]:
        """Convert Debezium timestamp (microseconds) to datetime."""
        if val is None:
            return None
        if isinstance(val, int):
            return datetime.fromtimestamp(val / 1_000_000)
        return val

    def write_cdc_event(
        self,
        event: CDCEvent,
        key: CDCKey,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Write CDC event to Delta Lake using SQL to avoid Python 3.14 serialization issues."""
        path = self._table_path("cdc_events")
        
        # Escape single quotes in strings for SQL
        def sql_escape(val):
            if val is None:
                return "NULL"
            return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"
        
        event_id = f"{topic}-{partition}-{offset}"
        source_table = str(event.full_table_name) if event.full_table_name else ""
        operation = str(event.operation) if event.operation else ""
        record_id = int(key.id) if key.id is not None else 0
        before_data = str(event.before) if event.before else None
        after_data = str(event.after) if event.after else None
        kafka_topic = str(topic) if topic else ""
        
        # Generate valid table name from path (only alphanumeric and underscore)
        table_name = path.replace("../", "").replace("./", "").replace("/", "_").replace("-", "_").replace(".", "_")
        
        # Create table and insert via SQL - avoids pickling
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_id STRING NOT NULL,
                source_table STRING NOT NULL,
                operation STRING NOT NULL,
                record_id BIGINT,
                before_data STRING,
                after_data STRING,
                kafka_topic STRING NOT NULL,
                kafka_partition INT NOT NULL,
                kafka_offset BIGINT NOT NULL,
                event_timestamp TIMESTAMP NOT NULL,
                processed_at TIMESTAMP NOT NULL
            ) USING DELTA
            LOCATION '{path}'
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        
        self.spark.sql(f"""
            INSERT INTO {table_name} VALUES (
                {sql_escape(event_id)},
                {sql_escape(source_table)},
                {sql_escape(operation)},
                {record_id},
                {sql_escape(before_data)},
                {sql_escape(after_data)},
                {sql_escape(kafka_topic)},
                {partition},
                {offset},
                TIMESTAMP'{event.event_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}',
                TIMESTAMP'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}'
            )
        """)

    def write_table_snapshot(self, table_name: str, event: CDCEvent) -> None:
        """Write/merge CDC event to table-specific Delta table using SQL"""
        if not event.after and event.operation != "d":
            return

        path = self._table_path(table_name)
        
        # Helper to escape SQL values
        def sql_escape(val):
            if val is None:
                return "NULL"
            if isinstance(val, datetime):
                return f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            if isinstance(val, (int, float)):
                return str(val)
            return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"

        if event.operation == "d":
            # DELETE
            if event.before and "id" in event.before:
                try:
                    if DeltaTable.isDeltaTable(self.spark, path):
                        record_id = event.before["id"]
                        # Use delta path syntax instead of converting to table name
                        self.spark.sql(f"DELETE FROM delta.`{path}` WHERE id = {record_id}")
                except Exception:
                    pass
        else:
            # INSERT/UPDATE - use MERGE via SQL
            data = event.after.copy()

            # Convert timestamps
            for field in list(data.keys()):
                if field.endswith("_at") or field.endswith("_date"):
                    data[field] = self._convert_ts(data[field])

            # Add metadata
            data["__cdc_operation"] = event.operation or ""
            data["__cdc_timestamp"] = event.event_timestamp
            data["__processed_at"] = datetime.now()
            
            # Get column names and values
            columns = list(data.keys())
            values = [sql_escape(data[col]) for col in columns]
            
            # Check if table exists
            is_delta = False
            try:
                is_delta = DeltaTable.isDeltaTable(self.spark, path)
            except Exception:
                pass
            
            if not is_delta:
                # Create table with first record - infer types from data
                col_defs = []
                for col in columns:
                    val = data[col]
                    if isinstance(val, datetime):
                        col_type = "TIMESTAMP"
                    elif isinstance(val, int):
                        col_type = "BIGINT"
                    elif isinstance(val, float):
                        col_type = "DOUBLE"
                    else:
                        col_type = "STRING"
                    col_defs.append(f"{col} {col_type}")
                
                # Create table using Delta path syntax
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS delta.`{path}` (
                        {', '.join(col_defs)}
                    ) USING DELTA
                    TBLPROPERTIES (delta.enableChangeDataFeed = true)
                """)
            
            # Use MERGE if possible, otherwise INSERT
            if "id" in data:
                record_id = data["id"]
                update_sets = [f"{col} = {sql_escape(data[col])}" for col in columns if col != "id"]
                
                # only update when data actually changes
                update_conditions = [
                    f"t.{col} != {sql_escape(data[col])} OR (t.{col} IS NULL AND {sql_escape(data[col])} IS NOT NULL) OR (t.{col} IS NOT NULL AND {sql_escape(data[col])} IS NULL)"
                    for col in columns if col != "id"
                ]
                update_condition = " OR ".join(update_conditions)
                
                self.spark.sql(f"""
                    MERGE INTO delta.`{path}` AS t
                    USING (SELECT {record_id} as id) AS s
                    ON t.id = s.id
                    WHEN MATCHED AND ({update_condition}) THEN UPDATE SET {', '.join(update_sets)}
                    WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join(values)})
                """)
            else:
                # No id field, just insert
                self.spark.sql(f"""
                    INSERT INTO delta.`{path}` ({', '.join(columns)})
                    VALUES ({', '.join(values)})
                """)

    def get_table_history(self, table_name: str) -> list:
        """Get Delta table history."""
        path = self._table_path(table_name)
        try:
            dt = DeltaTable.forPath(self.spark, path)
            return dt.history().collect()
        except Exception:
            return []

    def get_table_at_version(self, table_name: str, version: int) -> Optional[DataFrame]:
        """Time travel: read table at specific version."""
        path = self._table_path(table_name)
        try:
            return self.spark.read.format("delta").option("versionAsOf", version).load(path)
        except Exception:
            return None

    def get_current_table(self, table_name: str) -> Optional[DataFrame]:
        """Read current table state."""
        path = self._table_path(table_name)
        try:
            return self.spark.read.format("delta").load(path)
        except Exception:
            return None

    def run_sql(self, query: str) -> DataFrame:
        """Execute SQL query."""
        return self.spark.sql(query)

    def vacuum(self, table_name: str, retention_hours: int = 168) -> None:
        """Remove old files."""
        path = self._table_path(table_name)
        try:
            dt = DeltaTable.forPath(self.spark, path)
            dt.vacuum(retention_hours)
        except Exception:
            pass

    def optimize(self, table_name: str) -> None:
        """Compact small files."""
        path = self._table_path(table_name)
        try:
            dt = DeltaTable.forPath(self.spark, path)
            dt.optimize().executeCompaction()
        except Exception:
            pass

    def close(self) -> None:
        """Stop SparkSession."""
        if self.spark:
            self.spark.stop()
