"""
Spark Structured Streaming: Kafka CDC to Delta Lake.

This module implements real-time streaming from Kafka CDC topics to Delta Lake
using Spark Structured Streaming with exactly-once semantics.
"""

import os
import signal
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    expr,
    lit,
    current_timestamp,
    concat_ws,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# Configuration from environment
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DELTA_PATH = os.getenv("DELTA_LAKE_PATH", "./deltalake")
CHECKPOINT_PATH = os.getenv("SPARK_CHECKPOINT_PATH", "./checkpoints")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000"))
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "earliest")

# Kafka topics to subscribe (Debezium CDC topics)
CDC_TOPICS = "cdc.public.customers,cdc.public.products,cdc.public.orders,cdc.public.order_items"


# Debezium CDC payload schema
DEBEZIUM_SOURCE_SCHEMA = StructType([
    StructField("version", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("snapshot", StringType(), True),
    StructField("db", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("txId", LongType(), True),
    StructField("lsn", LongType(), True),
])

# Generic record schema (fields vary by table, so use MapType or parse dynamically)
DEBEZIUM_PAYLOAD_SCHEMA = StructType([
    StructField("before", StringType(), True),  # JSON string for flexibility
    StructField("after", StringType(), True),   # JSON string for flexibility
    StructField("source", DEBEZIUM_SOURCE_SCHEMA, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
])

DEBEZIUM_MESSAGE_SCHEMA = StructType([
    StructField("payload", DEBEZIUM_PAYLOAD_SCHEMA, True),
])

# CDC events output schema
CDC_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("operation_name", StringType(), False),
    StructField("before_data", StringType(), True),
    StructField("after_data", StringType(), True),
    StructField("kafka_topic", StringType(), False),
    StructField("kafka_partition", IntegerType(), False),
    StructField("kafka_offset", LongType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("processed_at", TimestampType(), False),
    StructField("source_db", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_lsn", LongType(), True),
    StructField("source_txid", LongType(), True),
])

# Table-specific schemas for snapshots
CUSTOMERS_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("__cdc_operation", StringType(), True),
    StructField("__cdc_timestamp", TimestampType(), True),
    StructField("__processed_at", TimestampType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("__cdc_operation", StringType(), True),
    StructField("__cdc_timestamp", TimestampType(), True),
    StructField("__processed_at", TimestampType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("customer_id", LongType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("__cdc_operation", StringType(), True),
    StructField("__cdc_timestamp", TimestampType(), True),
    StructField("__processed_at", TimestampType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("order_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("__cdc_operation", StringType(), True),
    StructField("__cdc_timestamp", TimestampType(), True),
    StructField("__processed_at", TimestampType(), True),
])

TABLE_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
}

# Operation code mapping
OP_NAMES = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT",
}


def create_spark_session(app_name: str = "CDC-Streaming-DeltaLake") -> SparkSession:
    """Create SparkSession with Kafka and Delta Lake support."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", DELTA_PATH)
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"SparkSession created: {spark.sparkContext.appName}")
    print(f"Spark version: {spark.version}")
    print(f"Delta Lake path: {DELTA_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")

    return spark


def read_kafka_stream(spark: SparkSession, topics: str) -> DataFrame:
    """Read CDC events from Kafka topics as a streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topics)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_debezium_cdc(df: DataFrame) -> DataFrame:
    """Parse Debezium CDC JSON payload from Kafka messages."""
    return (
        df
        # Keep Kafka metadata
        .withColumn("kafka_topic", col("topic"))
        .withColumn("kafka_partition", col("partition"))
        .withColumn("kafka_offset", col("offset"))
        .withColumn("kafka_timestamp", col("timestamp"))
        # Parse JSON value
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("parsed", from_json(col("value_str"), DEBEZIUM_MESSAGE_SCHEMA))
        # Extract payload fields
        .withColumn("payload", col("parsed.payload"))
        .withColumn("operation", col("payload.op"))
        .withColumn("before_data", col("payload.before"))
        .withColumn("after_data", col("payload.after"))
        .withColumn("source", col("payload.source"))
        .withColumn("ts_ms", col("payload.ts_ms"))
        # Extract source metadata
        .withColumn("source_table", concat_ws(".", col("source.schema"), col("source.table")))
        .withColumn("table_name", col("source.table"))
        .withColumn("source_db", col("source.db"))
        .withColumn("source_schema", col("source.schema"))
        .withColumn("source_lsn", col("source.lsn"))
        .withColumn("source_txid", col("source.txId"))
        # Create event ID
        .withColumn(
            "event_id",
            concat_ws("-", col("kafka_topic"), col("kafka_partition").cast("string"), col("kafka_offset").cast("string"))
        )
        # Convert timestamp (milliseconds to timestamp)
        .withColumn("event_timestamp", (col("ts_ms") / 1000).cast("timestamp"))
        .withColumn("processed_at", current_timestamp())
        # Map operation to name
        .withColumn(
            "operation_name",
            when(col("operation") == "c", "INSERT")
            .when(col("operation") == "u", "UPDATE")
            .when(col("operation") == "d", "DELETE")
            .when(col("operation") == "r", "SNAPSHOT")
            .otherwise("UNKNOWN")
        )
        # Filter out null payloads (tombstone events)
        .filter(col("payload").isNotNull())
        # Select final columns
        .select(
            "event_id",
            "source_table",
            "table_name",
            "operation",
            "operation_name",
            "before_data",
            "after_data",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "event_timestamp",
            "processed_at",
            "source_db",
            "source_schema",
            "source_lsn",
            "source_txid",
        )
    )


def write_cdc_events_stream(df: DataFrame, spark: SparkSession) -> None:
    """Write raw CDC events to Delta Lake in append mode."""
    output_path = f"{DELTA_PATH}/cdc_events"
    checkpoint_path = f"{CHECKPOINT_PATH}/cdc_events"

    # Select columns for CDC events table
    cdc_events_df = df.select(
        "event_id",
        "source_table",
        "operation",
        "operation_name",
        "before_data",
        "after_data",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "event_timestamp",
        "processed_at",
        "source_db",
        "source_schema",
        "source_lsn",
        "source_txid",
    )

    query = (
        cdc_events_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(output_path)
    )

    print(f"Started CDC events stream: {query.name} -> {output_path}")
    return query


def create_table_snapshot_writer(spark: SparkSession, table_name: str):
    """Create a foreachBatch writer for table snapshots with MERGE support."""
    output_path = f"{DELTA_PATH}/{table_name}"
    checkpoint_path = f"{CHECKPOINT_PATH}/{table_name}"
    schema = TABLE_SCHEMAS.get(table_name)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Process a micro-batch with MERGE operations."""
        if batch_df.isEmpty():
            return

        print(f"Processing batch {batch_id} for {table_name}: {batch_df.count()} records")

        # Parse after_data JSON to extract fields
        after_schema = StructType([
            field for field in schema
            if not field.name.startswith("__")
        ])

        # Process each operation type
        inserts_updates = batch_df.filter(col("operation").isin("c", "u", "r"))
        deletes = batch_df.filter(col("operation") == "d")

        # Handle INSERTs and UPDATEs
        if not inserts_updates.isEmpty():
            # Parse after_data JSON
            records_df = (
                inserts_updates
                .withColumn("record", from_json(col("after_data"), after_schema))
                .select(
                    col("record.*"),
                    col("operation").alias("__cdc_operation"),
                    col("event_timestamp").alias("__cdc_timestamp"),
                    col("processed_at").alias("__processed_at"),
                )
                .filter(col("id").isNotNull())
            )

            if not records_df.isEmpty():
                # Check if Delta table exists
                try:
                    delta_table = DeltaTable.forPath(spark, output_path)

                    # MERGE: update if exists, insert if not
                    (
                        delta_table.alias("target")
                        .merge(
                            records_df.alias("source"),
                            "target.id = source.id"
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    print(f"  Merged {records_df.count()} records into {table_name}")

                except Exception:
                    # Table doesn't exist, create it
                    records_df.write.format("delta").mode("overwrite").save(output_path)
                    print(f"  Created {table_name} with {records_df.count()} records")

        # Handle DELETEs
        if not deletes.isEmpty():
            try:
                delta_table = DeltaTable.forPath(spark, output_path)

                # Parse before_data to get IDs to delete
                before_schema = StructType([StructField("id", LongType(), False)])
                delete_ids_df = (
                    deletes
                    .withColumn("record", from_json(col("before_data"), before_schema))
                    .select(col("record.id").alias("id"))
                    .filter(col("id").isNotNull())
                )

                if not delete_ids_df.isEmpty():
                    # Collect IDs and delete
                    ids_to_delete = [row.id for row in delete_ids_df.collect()]
                    if ids_to_delete:
                        delta_table.delete(col("id").isin(ids_to_delete))
                        print(f"  Deleted {len(ids_to_delete)} records from {table_name}")

            except Exception as e:
                print(f"  Warning: Could not delete from {table_name}: {e}")

    return process_batch, checkpoint_path


def write_table_snapshot_stream(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
) -> None:
    """Write table snapshot stream with MERGE operations."""
    process_batch, checkpoint_path = create_table_snapshot_writer(spark, table_name)

    # Filter for specific table
    table_df = df.filter(col("table_name") == table_name)

    query = (
        table_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"Started {table_name} snapshot stream: {query.name}")
    return query


def main():
    """Main entry point for Spark Structured Streaming CDC pipeline."""
    print("=" * 60)
    print("Spark Structured Streaming: Kafka CDC -> Delta Lake")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session()

    # Track active queries for graceful shutdown
    active_queries = []

    def shutdown_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        print("\nShutdown signal received. Stopping streams...")
        for query in active_queries:
            try:
                query.stop()
                print(f"Stopped query: {query.name}")
            except Exception as e:
                print(f"Error stopping query: {e}")
        spark.stop()
        print("Spark session stopped.")
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Read from Kafka
    print(f"\nConnecting to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Subscribing to topics: {CDC_TOPICS}")
    kafka_df = read_kafka_stream(spark, CDC_TOPICS)

    # Parse Debezium CDC format
    parsed_df = parse_debezium_cdc(kafka_df)

    # Start CDC events stream (append-only audit log)
    print("\nStarting CDC events stream (append-only)...")
    cdc_query = write_cdc_events_stream(parsed_df, spark)
    active_queries.append(cdc_query)

    # Start table snapshot streams (MERGE operations)
    print("\nStarting table snapshot streams (MERGE)...")
    for table_name in TABLE_SCHEMAS.keys():
        query = write_table_snapshot_stream(parsed_df, spark, table_name)
        active_queries.append(query)

    print("\n" + "=" * 60)
    print(f"All streams started. Trigger interval: {TRIGGER_INTERVAL}")
    print("Press Ctrl+C to stop.")
    print("=" * 60 + "\n")

    # Wait for any stream to terminate
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        shutdown_handler(None, None)


if __name__ == "__main__":
    main()
