# Architecture

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
                                          │                  │
                                          └────────┬─────────┘
                                                   │
                                                   │                    
                                                   ▼                    
                                            ┌──────────────┐         
                                            │  Delta Lake  │          
                                            │  (Parquet +  │          
                                            │  Log)        │          
                                            └──────────────┘
```

# Project Structure

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
| Spark Master | Distributed Spark cluster (optional) | 7077, 8081 |
| Spark Worker | Spark worker node (optional) | 8082 |

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL client (`psql`) for testing
- `jq` for JSON processing (optional, for scripts)
- Java 8+ (required for PySpark)

# Quick Start

## 1. Start the Pipeline

```bash
# Basic mode (without Spark cluster)
./scripts/start.sh

# With Spark cluster
./scripts/start.sh --spark
```

## 2. Start the Python Consumer

In a new terminal:

```bash
# With delta-rs (lightweight, pure Python)
./scripts/run-consumer.sh

# With PySpark (full-featured)
./scripts/run-consumer.sh --spark
```




### Operating Modes

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

https://news.ycombinator.com/item?id=46098567

https://brokenco.de/2025/10/30/kafka-delta-ingest-was-fun.html