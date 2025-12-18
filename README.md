# CDC Data Pipeline POC with Delta Lake

A complete Change Data Capture (CDC) data pipeline using PostgreSQL, Debezium, Kafka, Delta Lake, and Python.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  PostgreSQL     │     │             │     │             │     │  PostgreSQL      │
│  (Source DB)    │────▶│  Debezium   │────▶│   Kafka     │────▶│  (Data Lake)     │
│  Port: 5433     │     │  Connector  │     │  Port: 9092 │     │  Port: 5434      │
└─────────────────┘     └─────────────┘     └─────────────┘     └──────────────────┘
                                                  │
                                                  │
                                                  ▼
                                          ┌──────────────────┐
                                          │  Python Consumer │
                                          │  (CDC Processor) │
                                          └────────┬─────────┘
                                                   │
                              ┌────────────────────┼────────────────────┐
                              │                    │                    │
                              ▼                    ▼                    ▼
                     ┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
                     │  Delta Lake  │     │  PostgreSQL  │     │  PySpark / SQL   │
                     │  (Parquet +  │     │  (Target DB) │     │  Queries         │
                     │  Transaction │     │  Port: 5435  │     │                  │
                     │  Log)        │     └──────────────┘     └──────────────────┘
                     └──────────────┘
```

## Components

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
| Spark Master | Distributed Spark cluster (optional) | 7077, 8081 |
| Spark Worker | Spark worker node (optional) | 8082 |

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL client (`psql`) for testing
- `jq` for JSON processing (optional, for scripts)
- Java 8+ (required for PySpark)

## Quick Start

### 1. Start the Pipeline

```bash
# Basic mode (without Spark cluster)
./scripts/start.sh

# With Spark cluster
./scripts/start.sh --spark
```

This will:
- Start all Docker containers
- Wait for services to be healthy
- Register the Debezium PostgreSQL connector
- (Optional) Start Spark master and worker

### 2. Verify Connector Status

```bash
./scripts/check-connector.sh
```

### 3. Start the Python Consumer

In a new terminal:

```bash
# With delta-rs (lightweight, pure Python)
./scripts/run-consumer.sh

# With PySpark (full-featured)
./scripts/run-consumer.sh --spark
```


## Delta Lake Integration

Delta Lake provides ACID transactions, time travel, and schema enforcement on top of Parquet files.

### Two Operating Modes

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

### Time Travel Queries

```python
# Query current version
df = load_table("customers")

# Query specific version
df_v1 = load_table("customers", version=1)

# Query by timestamp (PySpark)
df_time = spark.read.format("delta") \
    .option("timestampAsOf", "2025-01-15 10:00:00") \
    .load("./deltalake/customers")
```


## Manual Commands

### Start Services Step by Step

```bash
# Start infrastructure
docker-compose up -d postgres-source postgres-datalake postgres-target zookeeper
sleep 10

# Start Kafka
docker-compose up -d kafka
sleep 15

# Start Debezium
docker-compose up -d debezium
sleep 20

# Start Kafka UI
docker-compose up -d kafka-ui

# (Optional) Start Spark cluster
docker-compose --profile spark up -d

# Register connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://localhost:8083/connectors/ \
    -d @connectors/postgres-source-connector.json
```

### Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Watch messages on a topic
docker exec kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic cdc.public.customers \
    --from-beginning
```

### Check Connector Status

```bash
# List connectors
curl -s http://localhost:8083/connectors | jq

# Get connector status
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq

# Get connector config
curl -s http://localhost:8083/connectors/postgres-source-connector/config | jq
```

### Connect to Databases

```bash
# Source database
PGPASSWORD=source_password psql -h localhost -p 5433 -U source_user -d source_db

# Data Lake database
PGPASSWORD=datalake_password psql -h localhost -p 5434 -U datalake_user -d datalake_db

# Target database
PGPASSWORD=target_password psql -h localhost -p 5435 -U target_user -d target_db
```

### Test CDC Operations

```bash
# INSERT
PGPASSWORD=source_password psql -h localhost -p 5433 -U source_user -d source_db -c \
    "INSERT INTO customers (first_name, last_name, email) VALUES ('New', 'Customer', 'new@example.com');"

# UPDATE
PGPASSWORD=source_password psql -h localhost -p 5433 -U source_user -d source_db -c \
    "UPDATE customers SET email = 'updated@example.com' WHERE email = 'new@example.com';"

# DELETE
PGPASSWORD=source_password psql -h localhost -p 5433 -U source_user -d source_db -c \
    "DELETE FROM customers WHERE email = 'updated@example.com';"
```


### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f debezium
docker-compose logs -f kafka
docker-compose logs -f spark-master
```

### Stop Services

```bash
# Stop services (keep data)
./scripts/stop.sh

# Stop and remove volumes
docker-compose down -v

# Stop including Spark
docker-compose --profile spark down -v
```

## Project Structure

```
deltalake_poc/
├── docker-compose.yml              # Docker services configuration
├── connectors/
│   └── postgres-source-connector.json  # Debezium connector config
├── deltalake/                      # Delta Lake storage (created at runtime)
│   ├── customers/
│   ├── products/
│   ├── orders/
│   ├── order_items/
│   └── cdc_events/
├── init-scripts/
│   ├── source-init.sql             # Source DB initialization
│   ├── datalake-init.sql           # Data Lake DB initialization
│   └── target-init.sql             # Target DB initialization
├── notebooks/
│   └── deltalake-query.ipynb       # Interactive Delta Lake queries
├── python-consumer/
│   ├── config.py                   # Configuration settings
│   ├── consumer.py                 # Main Kafka consumer
│   ├── database.py                 # Database operations
│   ├── delta_handler.py            # Delta Lake handler (delta-rs)
│   ├── spark_delta_handler.py      # Delta Lake handler (PySpark)
│   ├── models.py                   # Data models
│   ├── requirements.txt            # Python dependencies
├── scripts/
│   ├── start.sh                    # Start all services
│   ├── stop.sh                     # Stop all services
│   ├── check-connector.sh          # Check connector status
│   ├── run-consumer.sh             # Run Python consumer
│   ├── test-cdc.sh                 # Test CDC operations
│   └── logs.sh                     # View service logs
├── docs/
└── README.md                       # This file
```

## Database Schemas

### Source Database (source_db)
- `customers` - Customer information
- `products` - Product catalog
- `orders` - Customer orders
- `order_items` - Order line items

### Data Lake Database (datalake_db)
- `cdc_raw.cdc_events` - Raw CDC events storage
- `cdc_processed.customers` - Processed customer data
- `cdc_processed.products` - Processed product data
- `cdc_processed.orders` - Processed order data
- `cdc_processed.order_items` - Processed order items

### Target Database (target_db)
- Mirror tables of source with CDC metadata columns

### Delta Lake Tables
- `customers` - Customer snapshots with version history
- `products` - Product snapshots with version history
- `orders` - Order snapshots with version history
- `order_items` - Order item snapshots with version history
- `cdc_events` - Full CDC event history (all operations)

## Kafka Topics

Topics are automatically created by Debezium:
- `cdc.public.customers` - Customer changes
- `cdc.public.products` - Product changes
- `cdc.public.orders` - Order changes
- `cdc.public.order_items` - Order item changes

## CDC Event Format

Debezium produces events in the following format:

```json
{
  "schema": { ... },
  "payload": {
    "before": { "id": 1, "name": "old_value" },
    "after": { "id": 1, "name": "new_value" },
    "source": {
      "version": "2.5.0.Final",
      "connector": "postgresql",
      "name": "cdc",
      "ts_ms": 1703123456789,
      "db": "source_db",
      "schema": "public",
      "table": "customers"
    },
    "op": "u",
    "ts_ms": 1703123456800
  }
}
```

Operation codes:
- `c` - Create (INSERT)
- `u` - Update (UPDATE)
- `d` - Delete (DELETE)
- `r` - Read (Initial snapshot)

## Troubleshooting

### Debezium connector fails to start

1. Check if PostgreSQL has logical replication enabled:
```sql
SHOW wal_level;  -- Should be 'logical'
```

2. Check connector logs:
```bash
docker-compose logs debezium
```

3. Verify PostgreSQL publication exists:
```sql
SELECT * FROM pg_publication;
```

### No messages in Kafka

1. Check if connector is running:
```bash
curl http://localhost:8083/connectors/postgres-source-connector/status | jq
```

2. Check Kafka topics:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Python consumer not receiving messages

1. Verify Kafka is accessible from host:
```bash
nc -zv localhost 29092
```

2. Check consumer group offset:
```bash
docker exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --group cdc-python-consumer \
    --describe
```

### Delta Lake tables not created

1. Verify the consumer is running with Delta Lake enabled:
```bash
./scripts/run-consumer.sh
```

2. Check if `deltalake/` directory exists and has data:
```bash
ls -la deltalake/
ls -la deltalake/customers/_delta_log/
```

3. Check consumer logs for errors:
```bash
# Look for "Delta Lake" in logs
```

### PySpark errors

1. Ensure Java is installed:
```bash
java -version  # Should be Java 8+
```

2. Check PySpark can find Delta:
```bash
python -c "from delta import configure_spark_with_delta_pip; print('OK')"
```

3. For memory issues, adjust Spark config:
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

## Web Interfaces

- **Kafka UI**: http://localhost:8080
  - View topics, messages, and consumer groups
  - Monitor Debezium connectors
- **Spark Master UI**: http://localhost:8081 (when running with `--spark`)
  - View Spark applications and workers
- **Spark Worker UI**: http://localhost:8082 (when running with `--spark`)
- **Jupyter Notebook**: http://localhost:8888 (when running `start-notebook.sh`)
  - Interactive Delta Lake queries

## Performance Considerations

### Delta Lake Optimization

```python
# Compact small files
from deltalake import DeltaTable
dt = DeltaTable("./deltalake/customers")
dt.optimize().execute_compaction()

# Remove old versions (vacuum)
dt.vacuum(retention_hours=168)  # Keep 7 days
```

### PySpark Tuning

```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

## Cleanup

```bash
# Stop services and remove containers
docker-compose down

# Stop services and remove volumes (full cleanup)
docker-compose down -v

# Also clean Delta Lake data
rm -rf deltalake/
```


https://towardsdatascience.com/hands-on-introduction-to-delta-lake-with-py-spark-b39460a4b1ae/

https://medium.com/@diwasb54/delta-lake-deep-dive-the-complete-guide-to-modern-data-lake-architecture-2c5b5c4c1ecf

https://www.youtube.com/watch?v=6VbRlQ0rL3I