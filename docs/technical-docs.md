# CDC Data Pipeline - Technical Documentation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Component Deep Dive](#component-deep-dive)
   - [PostgreSQL Source Database](#1-postgresql-source-database)
   - [Debezium (Kafka Connect)](#2-debezium-kafka-connect)
   - [Apache Kafka](#3-apache-kafka)
   - [Python Consumer](#4-python-consumer)
   - [Target Databases](#5-target-databases)
4. [Data Flow Examples](#complete-end-to-end-example)
5. [Summary](#summary)

---

## Overview

This document provides a technical deep dive into the Change Data Capture (CDC) pipeline architecture, explaining the role of each component and how data flows through the system.

---

## Architecture

### Overall Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CDC PIPELINE DATA FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘

   SOURCE DATABASE                DEBEZIUM                    KAFKA
   ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
   │ PostgreSQL  │            │   Kafka     │            │   Kafka     │
   │             │            │   Connect   │            │   Broker    │
   │ ┌─────────┐ │            │             │            │             │
   │ │  WAL    │─┼───────────▶│  Debezium   │───────────▶│   Topics    │
   │ │ (Write  │ │  Logical   │  PostgreSQL │  Produce   │             │
   │ │ Ahead   │ │  Replicat. │  Connector  │  Messages  │ ┌─────────┐ │
   │ │  Log)   │ │            │             │            │ │cdc.pub. │ │
   │ └─────────┘ │            └─────────────┘            │ │customers│ │
   │             │                                       │ └─────────┘ │
   │ Tables:     │                                       │ ┌─────────┐ │
   │ - customers │                                       │ │cdc.pub. │ │
   │ - products  │                                       │ │products │ │
   │ - orders    │                                       │ └─────────┘ │
   │ - order_    │                                       │     ...     │
   │   items     │                                       └──────┬──────┘
   └─────────────┘                                              │
                                                                │ Consume
                                                                ▼
                              ┌─────────────────────────────────────────┐
                              │           PYTHON CONSUMER               │
                              │                                         │
                              │  1. Parse Debezium JSON message         │
                              │  2. Extract operation (c/u/d/r)         │
                              │  3. Extract before/after data           │
                              │  4. Store raw event → Data Lake         │
                              │  5. Apply change → Target DB            │
                              │  6. Commit Kafka offset                 │
                              └────────────────┬────────────────────────┘
                                               │
                         ┌─────────────────────┼─────────────────────┐
                         │                     │                     │
                         ▼                     ▼                     ▼
                  ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
                  │  DATA LAKE  │       │  DATA LAKE  │       │   TARGET    │
                  │  (Raw CDC)  │       │ (Processed) │       │  DATABASE   │
                  │             │       │             │       │             │
                  │ cdc_raw.    │       │cdc_processed│       │  Replicated │
                  │ cdc_events  │       │  .customers │       │   Tables    │
                  │             │       │  .products  │       │             │
                  │ Full event  │       │  .orders    │       │  Current    │
                  │ history     │       │             │       │  State      │
                  └─────────────┘       └─────────────┘       └─────────────┘
```

---

## Component Deep Dive

### 1. PostgreSQL Source Database

#### Role

The source of truth for transactional data. All application writes happen here.

#### Technical Mechanism: Write-Ahead Log (WAL)

```
┌────────────────────────────────────────────────────────────────────┐
│                    PostgreSQL WAL Architecture                      │
└────────────────────────────────────────────────────────────────────┘

  Application                PostgreSQL Server
      │                            │
      │  INSERT INTO customers     │
      │  VALUES ('John', ...)      │
      ▼                            ▼
┌──────────┐               ┌──────────────────┐
│  Client  │──────────────▶│  Query Executor  │
└──────────┘               └────────┬─────────┘
                                    │
                                    ▼
                           ┌──────────────────┐
                           │   WAL Writer     │
                           │                  │
                           │ 1. Write change  │
                           │    to WAL buffer │
                           │ 2. Flush to disk │
                           │    (durability)  │
                           └────────┬─────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
             ┌──────────┐   ┌──────────────┐  ┌──────────────┐
             │ WAL File │   │ Data Pages   │  │  Replication │
             │ (pg_wal) │   │ (tablespace) │  │    Slot      │
             └──────────┘   └──────────────┘  └──────┬───────┘
                                                     │
                                                     │ Logical
                                                     │ Decoding
                                                     ▼
                                              ┌──────────────┐
                                              │   Debezium   │
                                              │  Connector   │
                                              └──────────────┘
```

#### Key Configuration

```sql
-- Required PostgreSQL settings for CDC
wal_level = logical          -- Enable logical replication
max_wal_senders = 4          -- Max concurrent replication connections
max_replication_slots = 4    -- Max replication slots

-- Publication (defines which tables to replicate)
CREATE PUBLICATION debezium_publication FOR ALL TABLES;
```

#### What WAL Contains

```
┌─────────────────────────────────────────────────────────────────┐
│                     WAL Record Structure                         │
├─────────────────────────────────────────────────────────────────┤
│  LSN (Log Sequence Number): 0/16B3D48                           │
│  Transaction ID: 758                                             │
│  Timestamp: 2024-12-16 10:30:45.123                             │
│  Operation: INSERT                                               │
│  Relation: public.customers                                      │
│  Tuple Data:                                                     │
│    - id: 6                                                       │
│    - first_name: 'John'                                          │
│    - last_name: 'Doe'                                            │
│    - email: 'john@example.com'                                   │
└─────────────────────────────────────────────────────────────────┘
```

#### Replication Slot

```
┌─────────────────────────────────────────────────────────────────┐
│                    Replication Slot                              │
├─────────────────────────────────────────────────────────────────┤
│  Purpose: Track consumer position in WAL                         │
│                                                                  │
│  - Prevents WAL segments from being deleted before consumed      │
│  - Maintains "confirmed_flush_lsn" (last acknowledged position)  │
│  - Survives connection drops and restarts                        │
│                                                                  │
│  slot_name: debezium_slot                                        │
│  plugin: pgoutput (native logical decoding)                      │
│  confirmed_flush_lsn: 0/16B3D48                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

### 2. Debezium (Kafka Connect)

#### Role

Captures database changes and converts them to Kafka messages.

#### Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                    Debezium Architecture                           │
└────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      Kafka Connect Framework                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Connect Worker                            │   │
│  │                                                              │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │         Debezium PostgreSQL Connector                 │   │   │
│  │  │                                                       │   │   │
│  │  │  ┌─────────────┐   ┌─────────────┐   ┌────────────┐  │   │   │
│  │  │  │   Source    │   │   Change    │   │   Record   │  │   │   │
│  │  │  │   Task      │──▶│   Event     │──▶│  Producer  │  │   │   │
│  │  │  │             │   │  Processor  │   │            │  │   │   │
│  │  │  └──────┬──────┘   └─────────────┘   └─────┬──────┘  │   │   │
│  │  │         │                                   │         │   │   │
│  │  └─────────┼───────────────────────────────────┼─────────┘   │   │
│  │            │                                   │              │   │
│  └────────────┼───────────────────────────────────┼──────────────┘   │
│               │                                   │                   │
│               │ Read WAL via                      │ Produce to        │
│               │ Logical Replication               │ Kafka             │
│               ▼                                   ▼                   │
│        ┌─────────────┐                     ┌─────────────┐           │
│        │ PostgreSQL  │                     │    Kafka    │           │
│        │   Source    │                     │   Broker    │           │
│        └─────────────┘                     └─────────────┘           │
│                                                                       │
│  Internal Topics (offset management):                                │
│  - connect_configs   (connector configurations)                      │
│  - connect_offsets   (source position tracking)                      │
│  - connect_statuses  (connector status)                              │
└───────────────────────────────────────────────────────────────────────┘
```

#### Connector Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│              Debezium Connector Workflow                         │
└─────────────────────────────────────────────────────────────────┘

Phase 1: SNAPSHOT (Initial Load)
─────────────────────────────────
  1. Lock tables (optional, configurable)
  2. Read current LSN position
  3. SELECT * FROM each table
  4. Emit 'r' (read) events for each row
  5. Record LSN as starting point
  6. Release locks

Phase 2: STREAMING (Continuous CDC)
────────────────────────────────────
  1. Connect to replication slot
  2. Receive WAL changes via pgoutput
  3. Decode logical changes
  4. Transform to Debezium event format
  5. Produce to Kafka topic
  6. Commit offset to connect_offsets topic

Event Flow:
───────────
  WAL Change          Debezium Processing           Kafka Message
  ┌─────────┐        ┌─────────────────┐          ┌─────────────┐
  │ INSERT  │───────▶│ 1. Decode WAL   │─────────▶│ Key: {id:1} │
  │ id=1    │        │ 2. Build before │          │ Value: {    │
  │ name=   │        │ 3. Build after  │          │   op: "c",  │
  │ "John"  │        │ 4. Add source   │          │   after:{...│
  └─────────┘        │    metadata     │          │   },source: │
                     │ 5. Serialize    │          │   {...}     │
                     └─────────────────┘          │ }           │
                                                  └─────────────┘
```

#### Debezium Event Structure

```json
{
  "schema": {
    "type": "struct",
    "fields": [...]    // Schema definition for the payload
  },
  "payload": {
    "before": {        // Previous state (null for INSERT)
      "id": 1,
      "first_name": "John",
      "last_name": "Doe",
      "email": "old@example.com"
    },
    "after": {         // New state (null for DELETE)
      "id": 1,
      "first_name": "John",
      "last_name": "Doe",
      "email": "new@example.com"
    },
    "source": {        // Source metadata
      "version": "2.5.0.Final",
      "connector": "postgresql",
      "name": "cdc",
      "ts_ms": 1702720245123,      // DB timestamp
      "snapshot": "false",
      "db": "source_db",
      "schema": "public",
      "table": "customers",
      "txId": 758,                  // Transaction ID
      "lsn": 23456789,             // Log Sequence Number
      "xmin": null
    },
    "op": "u",         // Operation: c=create, u=update, d=delete, r=read
    "ts_ms": 1702720245200,        // Debezium processing timestamp
    "transaction": null
  }
}
```

#### Operation Types

| Op Code | SQL Operation | before field | after field |
|---------|---------------|--------------|-------------|
| `c` | INSERT | null | new row data |
| `u` | UPDATE | old row data | new row data |
| `d` | DELETE | old row data | null |
| `r` | SNAPSHOT | null | row data |

---

### 3. Apache Kafka

#### Role

Distributed message broker that durably stores and distributes CDC events.

#### Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                      Kafka Architecture                             │
└────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                               │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    Kafka Broker                              │  │
│  │                                                              │  │
│  │  Topic: cdc.public.customers                                 │  │
│  │  ┌─────────────────────────────────────────────────────┐    │  │
│  │  │  Partition 0                                         │    │  │
│  │  │  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐        │    │  │
│  │  │  │  0  │  1  │  2  │  3  │  4  │  5  │ ... │        │    │  │
│  │  │  │ INS │ UPD │ INS │ DEL │ INS │ UPD │     │        │    │  │
│  │  │  └─────┴─────┴─────┴─────┴─────┴─────┴─────┘        │    │  │
│  │  │        ▲                         ▲                   │    │  │
│  │  │        │                         │                   │    │  │
│  │  │   Producer               Consumer Offset             │    │  │
│  │  │   (Debezium)            (Python Consumer)            │    │  │
│  │  └─────────────────────────────────────────────────────┘    │  │
│  │                                                              │  │
│  │  Topic: cdc.public.products                                  │  │
│  │  ┌─────────────────────────────────────────────────────┐    │  │
│  │  │  Partition 0                                         │    │  │
│  │  │  ┌─────┬─────┬─────┬─────┐                          │    │  │
│  │  │  │  0  │  1  │  2  │ ... │                          │    │  │
│  │  │  └─────┴─────┴─────┴─────┘                          │    │  │
│  │  └─────────────────────────────────────────────────────┘    │  │
│  │                                                              │  │
│  │  ... (topics for orders, order_items)                        │  │
│  │                                                              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  Zookeeper: Cluster coordination, broker metadata                  │
└────────────────────────────────────────────────────────────────────┘
```

#### Key Concepts

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka Key Concepts                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  TOPIC                                                           │
│  ─────                                                           │
│  - Named stream of messages                                      │
│  - One topic per table: cdc.public.customers                     │
│  - Append-only log                                               │
│                                                                  │
│  PARTITION                                                       │
│  ─────────                                                       │
│  - Ordered, immutable sequence of messages                       │
│  - Messages keyed by record ID → same key = same partition       │
│  - Enables parallel consumption                                  │
│                                                                  │
│  OFFSET                                                          │
│  ──────                                                          │
│  - Unique ID for each message within partition                   │
│  - Sequential, monotonically increasing                          │
│  - Consumer tracks its position via offset                       │
│                                                                  │
│  CONSUMER GROUP                                                  │
│  ──────────────                                                  │
│  - Group of consumers sharing work                               │
│  - Each partition consumed by one consumer in group              │
│  - Enables horizontal scaling                                    │
│                                                                  │
│  MESSAGE KEY                                                     │
│  ───────────                                                     │
│  - Debezium uses record primary key                              │
│  - Ensures ordering: same key → same partition                   │
│  - Example: {"id": 1}                                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Message Flow Through Kafka

```
┌─────────────────────────────────────────────────────────────────┐
│                Message Flow in Kafka                             │
└─────────────────────────────────────────────────────────────────┘

1. PRODUCE (Debezium → Kafka)
   ───────────────────────────

   Debezium                         Kafka Broker
   ┌──────────┐                    ┌──────────────────┐
   │ Message: │                    │ Topic: cdc.pub.  │
   │ key={id:1│───────────────────▶│   customers      │
   │ }        │    ProduceRequest  │                  │
   │ value={..│                    │ Partition 0:     │
   │ }        │                    │ [0][1][2][3]     │
   └──────────┘                    │          ▲      │
                                   │          │      │
                                   │     offset=3    │
                                   └──────────────────┘

2. CONSUME (Kafka → Python Consumer)
   ──────────────────────────────────

   Consumer                         Kafka Broker
   ┌──────────┐                    ┌──────────────────┐
   │ Poll()   │                    │ Partition 0:     │
   │          │◀───────────────────│ [0][1][2][3][4]  │
   │ Process  │    FetchResponse   │     ▲           │
   │ message  │                    │     │           │
   │          │                    │ committed=2     │
   │ Commit   │───────────────────▶│ current=4       │
   │ offset=4 │  OffsetCommit      │                 │
   └──────────┘                    └──────────────────┘

3. OFFSET TRACKING
   ─────────────────

   __consumer_offsets (internal topic):
   ┌────────────────────────────────────────────────┐
   │ group=cdc-python-consumer                      │
   │ topic=cdc.public.customers                     │
   │ partition=0                                    │
   │ committed_offset=4                             │
   │ timestamp=2024-12-16T10:30:45Z                │
   └────────────────────────────────────────────────┘
```

#### Why Kafka for CDC?

| Benefit | Description |
|---------|-------------|
| **Durability** | Messages persisted to disk, configurable retention, survives restarts |
| **Ordering** | Same key → same partition → ordered. Critical for CDC (UPDATE after INSERT) |
| **Replay** | Consumer can reset offset to re-read. Useful for recovery, testing, new consumers |
| **Decoupling** | Producer independent of consumers. Multiple consumers can read same topic |
| **Scalability** | Horizontal scaling via partitions. Multiple consumer instances in a group |
| **Backpressure** | Slow consumer doesn't affect producer. Messages buffer in Kafka |

---

### 4. Python Consumer

#### Role

Processes CDC events and applies changes to target databases.

#### Processing Flow

```
┌────────────────────────────────────────────────────────────────────┐
│                 Python Consumer Processing Flow                     │
└────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   while running:                                                     │
│       │                                                              │
│       ▼                                                              │
│   ┌───────────────┐                                                  │
│   │ consumer.poll │  ◄──── Fetch batch of messages from Kafka       │
│   │ (timeout=1s)  │                                                  │
│   └───────┬───────┘                                                  │
│           │                                                          │
│           ▼                                                          │
│   ┌───────────────┐                                                  │
│   │ Parse JSON    │  ◄──── Deserialize key and value                │
│   │ key & value   │                                                  │
│   └───────┬───────┘                                                  │
│           │                                                          │
│           ▼                                                          │
│   ┌───────────────┐                                                  │
│   │ Create        │  ◄──── Build CDCEvent from payload              │
│   │ CDCEvent      │        Extract: op, before, after, source       │
│   └───────┬───────┘                                                  │
│           │                                                          │
│           ├──────────────────────────────────────────┐               │
│           │                                          │               │
│           ▼                                          ▼               │
│   ┌───────────────┐                          ┌───────────────┐       │
│   │ Store Raw     │                          │ Apply to      │       │
│   │ Event         │                          │ Target DB     │       │
│   │ (Data Lake)   │                          │               │       │
│   └───────┬───────┘                          └───────┬───────┘       │
│           │                                          │               │
│           │  INSERT INTO cdc_raw.cdc_events          │  UPSERT       │
│           │  (full event JSON)                       │  based on op  │
│           │                                          │               │
│           └──────────────────┬───────────────────────┘               │
│                              │                                       │
│                              ▼                                       │
│                      ┌───────────────┐                               │
│                      │ consumer.     │  ◄──── Acknowledge message   │
│                      │ commit()      │        Move offset forward    │
│                      └───────────────┘                               │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

#### Operation Handling

```python
# Pseudocode for operation handling

def apply_to_target(event: CDCEvent):
    match event.operation:
        case 'c' | 'r':  # CREATE or READ (snapshot)
            # INSERT new record
            INSERT INTO {table} (columns...) VALUES (after_data...)
            ON CONFLICT (id) DO UPDATE SET ...  # UPSERT

        case 'u':  # UPDATE
            # UPDATE existing record
            INSERT INTO {table} (columns...) VALUES (after_data...)
            ON CONFLICT (id) DO UPDATE SET ...  # UPSERT

        case 'd':  # DELETE
            # DELETE record
            DELETE FROM {table} WHERE id = before_data.id
```

#### Exactly-Once Semantics

```
┌─────────────────────────────────────────────────────────────────┐
│              Ensuring Exactly-Once Processing                    │
└─────────────────────────────────────────────────────────────────┘

Problem: Consumer crashes after DB write but before offset commit
─────────────────────────────────────────────────────────────────

  Time    Consumer                     Kafka              Target DB
   │
   │      poll() ──────────────────▶ offset=5
   │              ◀────────────────── message
   │
   │      process message
   │      write to DB ───────────────────────────────────▶ INSERT
   │
   │      ❌ CRASH before commit
   │
   │      ... restart ...
   │
   │      poll() ──────────────────▶ offset=5 (same!)
   │              ◀────────────────── same message
   │
   │      process message
   │      write to DB ───────────────────────────────────▶ DUPLICATE!

Solution: Idempotent writes using UPSERT
────────────────────────────────────────

  INSERT INTO customers (id, name, email, ...)
  VALUES (1, 'John', 'john@example.com', ...)
  ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    ...;

  → Same data written twice = same result (idempotent)
```

---

### 5. Target Databases

#### Data Lake (postgres-datalake)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Lake Structure                           │
└─────────────────────────────────────────────────────────────────┘

Schema: cdc_raw (Raw Event Storage)
───────────────────────────────────
┌─────────────────────────────────────────────────────────────────┐
│ Table: cdc_events                                                │
├─────────────────────────────────────────────────────────────────┤
│ id              │ SERIAL PRIMARY KEY                            │
│ source_table    │ 'public.customers'                            │
│ operation       │ 'c', 'u', 'd', 'r'                            │
│ event_key       │ JSONB: {"id": 1}                              │
│ before_data     │ JSONB: previous row state                     │
│ after_data      │ JSONB: new row state                          │
│ source_metadata │ JSONB: Debezium source info                   │
│ kafka_topic     │ 'cdc.public.customers'                        │
│ kafka_partition │ 0                                              │
│ kafka_offset    │ 12345                                          │
│ event_timestamp │ timestamp from source DB                       │
│ processed_at    │ when consumer processed it                     │
└─────────────────────────────────────────────────────────────────┘

Purpose:
- Full audit trail of all changes
- Historical analysis
- Replay capability
- Debugging


Schema: cdc_processed (Current State)
─────────────────────────────────────
┌─────────────────────────────────────────────────────────────────┐
│ Tables: customers, products, orders, order_items                 │
├─────────────────────────────────────────────────────────────────┤
│ - Mirror of source schema                                        │
│ - Always reflects current state                                  │
│ - Additional metadata columns:                                   │
│   - cdc_operation: last operation type                          │
│   - cdc_timestamp: when change occurred                         │
│   - synced_at: when processed by consumer                       │
└─────────────────────────────────────────────────────────────────┘

Purpose:
- Analytics queries on current state
- Reporting
- Data warehouse patterns
```

#### Target Database (postgres-target)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Target Database Structure                     │
└─────────────────────────────────────────────────────────────────┘

Purpose: Read replica for applications
──────────────────────────────────────

┌─────────────────────────────────────────────────────────────────┐
│ Tables: customers, products, orders, order_items                 │
├─────────────────────────────────────────────────────────────────┤
│ - Exact mirror of source tables                                  │
│ - Additional CDC tracking columns:                               │
│   - __cdc_operation: 'c'/'u'/'d'/'r'                            │
│   - __cdc_timestamp: source event time                          │
│   - __synced_at: when replicated                                │
└─────────────────────────────────────────────────────────────────┘

Use Cases:
- Read replicas for scaling reads
- Geographic distribution
- Application isolation
- Disaster recovery
```

---

## Complete End-to-End Example

```
┌─────────────────────────────────────────────────────────────────┐
│           Complete CDC Flow: INSERT Example                      │
└─────────────────────────────────────────────────────────────────┘

1. APPLICATION WRITES TO SOURCE
   ─────────────────────────────

   INSERT INTO customers (first_name, last_name, email)
   VALUES ('Alice', 'Wonder', 'alice@example.com');

   → PostgreSQL writes to WAL
   → Row inserted with id=6


2. POSTGRESQL WAL
   ───────────────

   WAL Record:
   ┌──────────────────────────────────────┐
   │ LSN: 0/16B3D48                       │
   │ TxID: 760                            │
   │ Table: public.customers              │
   │ Operation: INSERT                    │
   │ Data: {id:6, first_name:'Alice',...} │
   └──────────────────────────────────────┘


3. DEBEZIUM CAPTURES CHANGE
   ─────────────────────────

   - Reads from replication slot
   - Decodes WAL record
   - Transforms to Debezium format:

   {
     "payload": {
       "op": "c",
       "before": null,
       "after": {
         "id": 6,
         "first_name": "Alice",
         "last_name": "Wonder",
         "email": "alice@example.com",
         "created_at": 1702720245123000
       },
       "source": {
         "table": "customers",
         "lsn": 23456789,
         "txId": 760
       }
     }
   }


4. KAFKA RECEIVES MESSAGE
   ───────────────────────

   Topic: cdc.public.customers
   Partition: 0 (based on key hash)
   Key: {"id": 6}
   Offset: 47

   Message stored durably, available for consumption


5. PYTHON CONSUMER PROCESSES
   ──────────────────────────

   a) Poll message from Kafka
   b) Parse JSON payload
   c) Extract operation='c' (create)
   d) Store raw event in Data Lake:

      INSERT INTO cdc_raw.cdc_events
      (source_table, operation, after_data, ...)
      VALUES ('public.customers', 'c', '{"id":6,...}', ...);

   e) Apply to Data Lake processed:

      INSERT INTO cdc_processed.customers
      (id, first_name, last_name, email, cdc_operation, ...)
      VALUES (6, 'Alice', 'Wonder', 'alice@example.com', 'c', ...);

   f) Apply to Target:

      INSERT INTO customers
      (id, first_name, last_name, email, __cdc_operation, ...)
      VALUES (6, 'Alice', 'Wonder', 'alice@example.com', 'c', ...)
      ON CONFLICT (id) DO UPDATE SET ...;

   g) Commit Kafka offset=47


6. VERIFICATION
   ─────────────

   Source DB:      SELECT * FROM customers WHERE id=6; → 1 row
   Data Lake Raw:  SELECT * FROM cdc_raw.cdc_events; → event logged
   Data Lake Proc: SELECT * FROM cdc_processed.customers WHERE id=6; → 1 row
   Target DB:      SELECT * FROM customers WHERE id=6; → 1 row (replicated!)
```

### Latency Breakdown (Typical)

| Stage | Latency |
|-------|---------|
| WAL write | ~1ms |
| Debezium capture | ~10-100ms (polling interval) |
| Kafka produce | ~5ms |
| Consumer poll | ~10-100ms (polling interval) |
| DB writes | ~5ms |
| **Total end-to-end** | **~50-300ms** |

---

## Summary

| Component | Role | Key Technology |
|-----------|------|----------------|
| **PostgreSQL Source** | Source of truth, generates WAL | Logical Replication, pgoutput |
| **Debezium** | Captures WAL changes, produces to Kafka | Kafka Connect, PostgreSQL Connector |
| **Kafka** | Durable message broker, decouples producers/consumers | Topics, Partitions, Consumer Groups |
| **Python Consumer** | Processes CDC events, writes to targets | confluent-kafka, psycopg2 |
| **Data Lake** | Stores raw events + processed state | PostgreSQL (could be Delta Lake) |
| **Target DB** | Read replica for applications | PostgreSQL |

---

## Appendix: Quick Reference Commands

### Check Pipeline Status

```bash
# Connector status
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Watch topic messages
docker exec kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic cdc.public.customers \
    --from-beginning
```

### Database Connections

```bash
# Source
PGPASSWORD=source_password psql -h localhost -p 5433 -U source_user -d source_db

# Data Lake
PGPASSWORD=datalake_password psql -h localhost -p 5434 -U datalake_user -d datalake_db

# Target
PGPASSWORD=target_password psql -h localhost -p 5435 -U target_user -d target_db
```

### Useful Queries

```sql
-- Check CDC events in Data Lake
SELECT * FROM cdc_raw.cdc_statistics;

-- Check replication lag
SELECT * FROM cdc_processed.analytics_summary;

-- Check target sync status
SELECT * FROM sync_summary;
```
