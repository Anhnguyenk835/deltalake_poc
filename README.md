# Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐
│  PostgreSQL     │     │             │     │             │
│  (Source DB)    │────▶│  Debezium   │────▶│   Kafka     │
│  Port: 5433     │     │  Connector  │     │  Port: 9092 │
└─────────────────┘     └─────────────┘     └──────┬──────┘
                                                    │
                              ┌─────────────────────┼─────────────────────┐
                              │                     │                     │
                              ▼                     ▼                     ▼
                    ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
                    │ Spark Streaming  │  │ Python Consumer  │  │  PostgreSQL      │
                    │   (Connector)    │  │  (delta-rs)      │  │  (Data Lake)     │
                    │   --connector    │  │   (default)      │  │  Port: 5434      │
                    └────────┬─────────┘  └────────┬─────────┘  └──────────────────┘
                             │                     │
                             ▼                     ▼
                    ┌──────────────────┐  ┌──────────────────┐
                    │   Delta Lake     │  │  Delta Lake +    │
                    │   (Parquet +     │  │  PostgreSQL      │
                    │   Transaction    │  │  (Target DB)     │
                    │   Log)           │  │  Port: 5435      │
                    └──────────────────┘  └──────────────────┘
```

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
├── consumer/                             # CDC Consumers sub-directory
│   ├── python-consumer/                  # Traditional Python Kafka consumer
│   │   ├── consumer.py                   # Main Kafka consumer entry point
│   │   ├── database.py                   # PostgreSQL operations
│   │   ├── delta_handler.py              # Delta Lake handler (delta-rs)
│   │   └── spark_delta_handler.py        # Delta Lake handler (PySpark mode)
│   └── spark-streaming/                  # Spark Structured Streaming consumer (recommended)
│       ├── spark_streaming.py            # Main streaming application
│       ├── Dockerfile.spark              # Spark Docker image
│       ├── entrypoint.sh                 # Container entrypoint (Java detection)
│       └── requirements.txt              # Python dependencies
├── shared/                               # Shared configuration and models
│   ├── config.py                         # Configuration classes
│   ├── models.py                         # Data models (CDCEvent, CDCKey)
│   └── __init__.py                       # Module exports
├── scripts/
│   ├── start.sh                          # Start infrastructure services
│   ├── stop.sh                           # Stop all services
│   ├── check-connector.sh                # Check connector status
│   ├── run-consumer.sh                   # Run consumer (default: Python, --connector: Spark)
│   ├── test-cdc.sh                       # Test CDC operations
│   └── logs.sh                           # View service logs
├── checkpoints/                          # Spark Streaming checkpoints (created at runtime)
├── docs/
└── README.md                             # This file
```

# Components

| Component | Description | Port |
|-----------|-------------|------|
| PostgreSQL Source | Source database with sample data | 5433 |
| PostgreSQL Data Lake | Stores raw CDC events and processed data | 5434 |
| PostgreSQL Target | Target database for replicated data | 5435 |
| Zookeeper | Kafka coordination service | 2181 |
| Kafka | Message broker for CDC events | 9092 (internal), 29092 (external) |
| Debezium | CDC connector for PostgreSQL | 8083 |
| Kafka UI | Web UI for Kafka monitoring | 8080 |
| Delta Lake | Versioned data lake storage (file-based) | N/A |
| **Spark Streaming** | **Standalone CDC consumer** | **4040** |
| Spark Master | Distributed Spark cluster (optional, not needed) | 7077, 8081 |
| Spark Worker | Spark worker node (optional, not needed) | 8082 |

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL client (`psql`) for testing
- `jq` for JSON processing (optional, for scripts)
- Java 8+ (required for PySpark)

# Quick Start

## 1. Start the Infrastructure

```bash
# Start infrastructure (PostgreSQL, Kafka, Debezium)
./scripts/start.sh
```

or
```bash
# Start infrastructure with spark cluster
./scripts/start.sh --spark
```

## 2. Start a Consumer (Choose One)

**Option A: Spark Streaming Consumer**
```bash
./scripts/run-consumer.sh --connector
```
- Processes CDC events in real-time with Spark Structured Streaming
- Writes directly to Delta Lake with ACID guarantees
- Auto-recovery and checkpointing
- Spark UI: http://localhost:4040

**Option B: Python Consumer**
```bash
# With delta-rs (pure Python)
./scripts/run-consumer.sh

# With PySpark (for SQL queries)
./scripts/run-consumer.sh --spark
```


### Delta Lake Modes in Python Consumer

| Mode | Library | Best For | Features |
|------|---------|----------|----------|
| **delta-rs** | `deltalake` (Python) | Lightweight operations | Read, write, merge, time travel |
| **PySpark** | `delta-spark` | Full SQL support | SQL queries, complex analytics, distributed processing |

### Delta Lake Directory Structure

```
deltalake/
├── customers/           # Customer table
│   ├── _delta_log/      # Transaction log (JSON)
│   │   ├── 00000000000000000000.json
│   │   ├── 00000000000000000001.json
│   │   └── ...
│   └── part-*.parquet   # Data files
├── products/
├── orders/
├── order_items/
└── cdc_events/          # Raw CDC events with full history
```


https://towardsdatascience.com/hands-on-introduction-to-delta-lake-with-py-spark-b39460a4b1ae/

https://medium.com/@diwasb54/delta-lake-deep-dive-the-complete-guide-to-modern-data-lake-architecture-2c5b5c4c1ecf

https://www.youtube.com/watch?v=6VbRlQ0rL3I