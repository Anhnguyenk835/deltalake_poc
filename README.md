# Architecture

![Architecture](public/arc.svg)

# Project Structure

```
deltalake_poc/
├── docker-compose.yml                    # Docker services configuration
├── connectors/
│   └── postgres-source-connector.json    # Debezium connector config
├── deltalake/                            # Delta Lake storage (created at runtime)
├── init-scripts/
│   ├── source-init.sql                   # Source DB initialization
│   ├── datalake-init.sql                 # Data Lake DB initialization
│   └── target-init.sql                   # Target DB initialization
├── notebooks/
│   └── deltalake-query.ipynb             # Interactive Delta Lake queries
├── consumer/                             # CDC Consumers
│   ├── python-consumer/                  # Python Kafka consumer
│   │   ├── consumer.py                   # Main consumer entry point
│   │   ├── database.py                   # PostgreSQL operations
│   │   ├── delta_handler.py              # Delta Lake handler (delta-rs)
│   │   └── spark_delta_handler.py        # Delta Lake handler (PySpark)
│   ├── spark-streaming/                  # Spark Structured Streaming
│   │   ├── spark_streaming.py            # Main streaming application
│   │   ├── Dockerfile.spark              # Spark Docker image
│   │   └── entrypoint.sh                 # Container entrypoint
│   └── risingwave-connector/             # RisingWave streaming database
│       ├── init-risingwave.sql           # Sources, views, and sinks
│       ├── init-minio.sh                 # MinIO bucket setup
│       └── README.md                     # RisingWave-specific docs
├── shared/                               # Shared configuration and models
│   ├── config.py                         # Configuration classes
│   ├── models.py                         # Data models (CDCEvent, CDCKey)
│   └── __init__.py                       # Module exports
├── scripts/
│   ├── start.sh                          # Start infrastructure services
│   ├── stop.sh                           # Stop all services
│   ├── check-connector.sh                # Check connector status
│   ├── run-consumer.sh                   # Run consumer
│   ├── test-cdc.sh                       # Test CDC operations
│   └── logs.sh                           # View service logs
├── checkpoints/                          # Spark Streaming checkpoints
├── docs/
└── README.md                             # This file
```

# Components

| Component | Description | Port |
|-----------|-------------|------|
| PostgreSQL Source | Source database with sample data | 5433 |
| PostgreSQL Data Lake | Stores raw CDC events | 5434 |
| PostgreSQL Target | Target database for replicated data | 5435 |
| Zookeeper | Kafka coordination service | 2181 |
| Kafka | Message broker for CDC events | 9092 (internal), 29092 (external) |
| Debezium | CDC connector for PostgreSQL | 8083 |
| Kafka UI | Web UI for Kafka monitoring | 8080 |
| **Spark Streaming** | Spark Structured Streaming consumer | 4040 |
| **RisingWave** | SQL streaming database (SQLite metadata) | 4566, 5691 |
| **MinIO** | S3-compatible object storage (for RisingWave) | 9000, 9001 |
| Spark Master | Distributed Spark cluster (optional) | 7077, 8081 |
| Spark Worker | Spark worker node (optional) | 8082 |

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL client (`psql`) for testing
- `jq` for JSON processing (optional)
- Java 8+ (required for PySpark mode)

# Quick Start

## 1. Start the Infrastructure

```bash
# Basic infrastructure (PostgreSQL, Kafka, Debezium)
./scripts/start.sh

# With RisingWave streaming database
./scripts/start.sh --risingwave

# With Spark cluster (optional)
./scripts/start.sh --spark
```

## 2. Choose a Consumer

### Option A: RisingWave (SQL-based streaming) - Recommended

```bash
# Start with RisingWave
./scripts/start.sh --risingwave

# Initialize pipeline (creates Delta tables + sinks automatically)
./scripts/run-consumer.sh --risingwave
```

Features:
- SQL-based streaming processing
- Native Debezium CDC support
- Real-time materialized views with automatic updates
- Delta Lake sink to MinIO (S3-compatible storage)
- Dashboard: http://localhost:5691
- MinIO Console: http://localhost:9001 (admin/password)

### Option B: Spark Streaming

```bash
./scripts/run-consumer.sh --connector
```

Features:
- Spark Structured Streaming
- Direct Delta Lake writes with ACID guarantees
- Auto-recovery and checkpointing
- Spark UI: http://localhost:4040

### Option C: Python Consumer

```bash
# With delta-rs (lightweight)
./scripts/run-consumer.sh

# With PySpark (full SQL support)
./scripts/run-consumer.sh --spark
```

## Technology Comparisons

### Delta Lake Libraries: delta-rs vs PySpark

This project supports two methods for writing to Delta Lake from Python:

| Aspect | delta-rs | PySpark (delta-spark) |
|--------|----------|----------------------|
| **Implementation** | Native Rust library with Python bindings | JVM-based Spark with Delta Lake extension |
| **Dependencies** | `pip install deltalake` (~50MB) | `pip install pyspark delta-spark` (~300MB) + Java 8+ |
| **Startup Time** | Instant | 5-15 seconds (JVM startup) |
| **Memory Usage** | Low (~100MB) | High (~1-2GB for Spark driver) |
| **SQL Support** | Limited (basic reads) | Full SQL support via SparkSQL |
| **Write Modes** | append, overwrite, merge | append, overwrite, merge, update, delete |
| **Time Travel** | Read any version | Read + write (RESTORE command) |
| **Concurrency** | Optimistic concurrency | Full ACID with conflict resolution |
| **Best For** | Lightweight ETL, simple writes, edge devices | Complex transformations, ML pipelines, enterprise |

#### Schema Evolution Capabilities

| Operation | delta-rs | PySpark (delta-spark) |
|-----------|----------|----------------------|
| **Add columns** | ✅ `mergeSchema` option | ✅ `mergeSchema` option |
| **Add columns (explicit)** | ✅ `add_columns()` API | ✅ `ALTER TABLE ADD COLUMNS` |
| **Overwrite schema** | ✅ `overwriteSchema` option | ✅ `overwriteSchema` option |
| **Rename columns** | ❌ Not supported | ✅ Requires Column Mapping mode |
| **Drop columns** | ❌ Not supported | ✅ Requires Column Mapping mode |
| **Reorder columns** | ❌ Not supported | ✅ `ALTER TABLE CHANGE COLUMN` |
| **Change column types** | ❌ Not supported | ✅ Safe type widening only |
| **Column Mapping mode** | ❌ Not supported | ✅ Full support (reader/writer v2+) |

> **Note**: Column Mapping is a Delta Lake feature that allows renaming and dropping columns without rewriting Parquet files. It requires `delta.columnMapping.mode = 'name'` and minimum protocol versions (reader=2, writer=5). See [Delta Lake Column Mapping docs](https://docs.databricks.com/aws/en/delta/column-mapping).

**Recommendation:**
- Use **delta-rs** for: Simple CDC pipelines, containerized deployments, low-latency requirements, adding columns only
- Use **PySpark** for: Complex analytics, SQL-heavy workloads, full schema evolution (rename/drop columns)

### Streaming Consumers: Spark Streaming vs RisingWave

Based on [RisingWave's comparison](https://risingwave.com/risingwave-vs-apache-spark/) and [technical analysis](https://ericfu.me/en/compare-streaming-systems/):

| Aspect | Spark Structured Streaming | RisingWave |
|--------|---------------------------|------------|
| **System Category** | Micro-batch stream processing engine | Streaming database |
| **Architecture** | Batch-first with micro-batch execution | Cloud-native, decoupled compute-storage |
| **Processing Model** | Micro-batch (100ms+ latency) | True streaming with incremental computation |
| **CDC Support** | Manual Debezium parsing required | Native Debezium FORMAT support |
| **Materialized Views** | No (must manage state manually) | Yes (automatically maintained, incrementally updated) |
| **State Management** | In-memory with checkpoints to storage | Native state persisted in S3/object storage |
| **Delta Lake Write** | Native (via delta-spark) | Requires pre-created tables (manually), append-only |
| **Write Modes** | Append, Complete, Update + merge/upsert | Append-only |
| **Storage Backend** | Local filesystem, S3, HDFS | S3-compatible only (MinIO, AWS S3, ...) |
| **Resource Usage** | High (JVM, 2-4GB minimum) | Medium (~500MB-1GB) |
| **Scaling** | Horizontal (add workers) | Horizontal (add compute nodes) |
| **Query Interface** | SparkSQL, DataFrame API | PostgreSQL wire protocol (use psql) |
| **Client Libraries** | Spark client bindings only | Java, Python, Node.js, more |
| **License** | Apache 2.0 | Apache 2.0 |

#### Pros and Cons

**Spark Structured Streaming:**
| Pros | Cons |
|------|------|
| Mature ecosystem (10+ years), battle-tested | Heavy resource usage (JVM overhead) |
| Full Delta Lake integration (merge, upsert, delete) | Complex setup and configuration |
| Rich DataFrame/SQL API for complex transformations | Manual CDC parsing needed |
| Extensive documentation and community | Micro-batch latency (100ms+) |
| Existing enterprise adoption, proven at scale | State management requires careful tuning |

**RisingWave:**
| Pros | Cons |
|------|------|
| SQL-only interface (easy to learn, PostgreSQL-compatible) | Append-only Delta Lake sink (no upsert/merge) |
| Native Debezium CDC support (no parsing code) 
| Real-time materialized views (auto-maintained) | Delta tables must be pre-created before sinks |
| True streaming (lower latency than micro-batch) | Newer project (2022+), smaller community |
| Lower resource usage, cloud-native design | Limited ecosystem integrations vs Spark |
| Fast startup, no JVM overhead | Parquet files may have compatibility issues with some readers |


## References

### Delta Lake
- [Delta Lake Schema Evolution](https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/) - Official guide on schema evolution
- [Delta Lake Column Mapping](https://docs.databricks.com/aws/en/delta/column-mapping) - Rename and drop columns without rewriting data
- [delta-rs Python Documentation](https://delta-io.github.io/delta-rs/) - Native Rust Delta Lake library

### Streaming Comparison
- [RisingWave vs Apache Spark](https://risingwave.com/risingwave-vs-apache-spark/) - Official comparison
- [Technical Comparison of Streaming Systems](https://ericfu.me/en/compare-streaming-systems/) - technical analysis
- [Data Streaming Landscape 2024](https://kai-waehner.medium.com/the-data-streaming-landscape-2024-6e078b1959b5) 

### RisingWave
- [RisingWave Delta Lake Sink](https://docs.risingwave.com/integrations/destinations/delta-lake) - Official documentation
- [RisingWave Debezium CDC](https://docs.risingwave.com/ingest/debezium-cdc) - Native CDC support

### Tutorials
- [Delta Lake with PySpark](https://www.datacamp.com/tutorial/delta-lake) - Hands-on introduction
- [Kafka to Delta Lake](https://delta.io/blog/write-kafka-stream-to-delta-lake/) - Streaming pipeline guide

### More - Kafka-delta-ingest
- [Kafka-delta-ingest](https://brokenco.de/2025/10/30/kafka-delta-ingest-was-fun.html) - Considerably the use case that they drop 
