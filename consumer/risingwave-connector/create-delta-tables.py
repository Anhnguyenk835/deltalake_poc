#!/usr/bin/env python3
"""
RisingWave's Delta Lake sink requires pre-existing Delta tables with proper schema.
This script creates empty Delta tables with the correct schema before RisingWave
can sink data to them.
"""

import os
import pyarrow as pa
from deltalake import DeltaTable

# MinIO/S3 configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password")
BUCKET = "deltalake"

# S3 storage options for delta-rs
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}

# Table schemas matching RisingWave materialized views
TABLES = {
    "customers": pa.schema([
        ("id", pa.int32()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]),
    "products": pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("description", pa.string()),
        ("price", pa.decimal128(10, 2)),
        ("stock_quantity", pa.int32()),
        ("category", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]),
    "orders": pa.schema([
        ("id", pa.int32()),
        ("customer_id", pa.int32()),
        ("order_date", pa.date32()),
        ("status", pa.string()),
        ("total_amount", pa.decimal128(12, 2)),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]),
    "order_items": pa.schema([
        ("id", pa.int32()),
        ("order_id", pa.int32()),
        ("product_id", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.decimal128(10, 2)),
        ("created_at", pa.timestamp("us", tz="UTC")),
    ]),
    # Analytics tables (from materialized views)
    "order_analytics": pa.schema([
        ("order_day", pa.timestamp("us", tz="UTC")),  # DATE_TRUNC returns timestamptz
        ("status", pa.string()),
        ("order_count", pa.int64()),
        ("total_revenue", pa.decimal128(12, 2)),
        ("avg_order_value", pa.decimal128(12, 2)),
    ]),
    "customer_order_summary": pa.schema([
        ("customer_id", pa.int32()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("total_orders", pa.int64()),
        ("lifetime_value", pa.decimal128(12, 2)),
    ]),
    "product_inventory": pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("category", pa.string()),
        ("price", pa.decimal128(10, 2)),
        ("stock_quantity", pa.int32()),
        ("stock_status", pa.string()),
    ]),
}


def create_delta_table(table_name: str, schema: pa.Schema) -> None:
    """Create an empty Delta table with the given schema and Change Data Feed enabled."""
    table_path = f"s3://{BUCKET}/{table_name}"

    # Check if table already exists
    try:
        dt = DeltaTable(table_path, storage_options=STORAGE_OPTIONS)
        print(f"  [EXISTS] {table_name} - Delta table already exists at {table_path}")
        return
    except Exception:
        pass  # Table doesn't exist, create it

    # Create empty Delta table with schema and Change Data Feed enabled
    # CDF allows tracking insert, update, delete changes with _change_type column
    DeltaTable.create(
        table_uri=table_path,
        schema=schema,
        storage_options=STORAGE_OPTIONS,
        configuration={
            "delta.enableChangeDataFeed": "true",
        },
    )

    print(f"  [CREATED] {table_name} - Empty Delta table created at {table_path} (CDF enabled)")


def enable_cdf_on_existing_table(table_name: str) -> bool:
    """Enable Change Data Feed on an existing Delta table if not already enabled."""
    table_path = f"s3://{BUCKET}/{table_name}"
    try:
        dt = DeltaTable(table_path, storage_options=STORAGE_OPTIONS)
        metadata = dt.metadata()
        cdf_enabled = metadata.configuration.get("delta.enableChangeDataFeed", "false")

        if cdf_enabled == "true":
            print(f"  [SKIP] {table_name} - CDF already enabled")
            return True

        # Enable CDF by altering table properties
        dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})
        print(f"  [ENABLED] {table_name} - CDF has been enabled")
        return True
    except Exception as e:
        print(f"  [ERROR] {table_name} - Could not enable CDF: {e}")
        return False


def verify_table(table_name: str) -> bool:
    """Verify a Delta table exists and is readable."""
    table_path = f"s3://{BUCKET}/{table_name}"
    try:
        dt = DeltaTable(table_path, storage_options=STORAGE_OPTIONS)
        version = dt.version()
        schema = dt.schema()
        metadata = dt.metadata()
        cdf_enabled = metadata.configuration.get("delta.enableChangeDataFeed", "false")
        cdf_status = "CDF: ON" if cdf_enabled == "true" else "CDF: OFF"
        print(f"  [OK] {table_name} - version {version}, {len(schema.fields)} columns, {cdf_status}")
        return True
    except Exception as e:
        print(f"  [ERROR] {table_name} - {e}")
        return False


def main():
    # Create tables
    print("Creating Delta tables...")
    for table_name, schema in TABLES.items():
        try:
            create_delta_table(table_name, schema)
        except Exception as e:
            print(f"  [ERROR] {table_name} - {e}")

    print()

    # Enable CDF on existing tables that don't have it
    print("Enabling Change Data Feed on existing tables...")
    for table_name in TABLES:
        try:
            enable_cdf_on_existing_table(table_name)
        except Exception as e:
            print(f"  [ERROR] {table_name} - {e}")

    print()

    # Verify tables
    print("Verifying Delta tables...")
    success_count = 0
    for table_name in TABLES:
        if verify_table(table_name):
            success_count += 1

    print()
    print("=" * 60)
    print(f"  Result: {success_count}/{len(TABLES)} tables ready")
    print("=" * 60)

    if success_count == len(TABLES):
        print("\nAll Delta tables are ready for RisingWave sinks!")
        print("\nNext step: Initialize RisingWave with Delta Lake sinks:")
        print("  psql -h localhost -p 4566 -U root -d dev -f consumer/risingwave-connector/init-risingwave-deltalake.sql")
    else:
        print("\nSome tables failed. Check MinIO connectivity and permissions.")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
