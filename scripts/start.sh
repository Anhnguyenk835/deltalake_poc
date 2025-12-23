#!/bin/bash
# Start the CDC Data Pipeline
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
WITH_SPARK=false
WITH_RISINGWAVE=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --spark) WITH_SPARK=true ;;
        --risingwave) WITH_RISINGWAVE=true ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --spark       Include Spark cluster"
            echo "  --risingwave  Include RisingWave streaming database with MinIO"
            echo "  -h, --help    Show this help message"
            exit 0
            ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo "=========================================="
echo "  CDC Data Pipeline - Starting Services  "
echo "=========================================="

if [ "$WITH_SPARK" = true ]; then
    echo "  (Including Spark cluster)              "
fi
if [ "$WITH_RISINGWAVE" = true ]; then
    echo "  (Including RisingWave + MinIO)         "
fi
echo "=========================================="

cd "$PROJECT_DIR"

# Step 1: Start infrastructure services
echo ""
echo "[1/6] Starting infrastructure services (PostgreSQL, Zookeeper)..."
docker-compose up -d postgres-source postgres-datalake postgres-target zookeeper

echo "Waiting for databases to be ready..."
sleep 10

# Step 2: Start Kafka
echo ""
echo "[2/6] Starting Kafka broker..."
docker-compose up -d kafka

echo "Waiting for Kafka to be ready..."
sleep 15

# Step 3: Start Debezium
echo ""
echo "[3/6] Starting Debezium Kafka Connect..."
docker-compose up -d debezium

echo "Waiting for Debezium to be ready..."
sleep 20

# Step 4: Start Kafka UI
echo ""
echo "[4/6] Starting Kafka UI..."
docker-compose up -d kafka-ui

# Step 5: Start Spark (optional)
if [ "$WITH_SPARK" = true ]; then
    echo ""
    echo "[5/7] Starting Spark cluster..."
    docker-compose --profile spark up -d
    echo "Waiting for Spark to be ready..."
    sleep 10
else
    echo ""
    echo "[5/7] Skipping Spark cluster (use --spark to enable)"
fi

# Step 6: Start RisingWave (optional)
if [ "$WITH_RISINGWAVE" = true ]; then
    echo ""
    echo "[6/7] Starting RisingWave with MinIO (SQLite backend)..."
    docker-compose --profile risingwave up -d
    echo "Waiting for RisingWave to be ready..."

    # Wait for RisingWave to be healthy using TCP check
    MAX_RETRIES=60
    RETRY_COUNT=0
    echo "Checking RisingWave health..."
    while ! nc -z localhost 4566 2>/dev/null; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo "Warning: RisingWave is taking longer than expected to start."
            echo "Check logs with: docker-compose logs risingwave"
            break
        fi
        echo "Waiting for RisingWave... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 3
    done

    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
        echo "RisingWave is ready!"
        echo "MinIO buckets 'hummock001' and 'deltalake' are pre-created."
    fi
else
    echo ""
    echo "[6/7] Skipping RisingWave (use --risingwave to enable)"
fi

# Step 7: Register Debezium connector
echo ""
echo "[7/7] Registering PostgreSQL source connector..."
sleep 5

# Check if Debezium is ready
MAX_RETRIES=30
RETRY_COUNT=0
while ! curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "Error: Debezium Connect is not responding after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "Waiting for Debezium Connect to be ready... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

# Check if connector already exists (check for 404 in response)
CONNECTOR_CHECK=$(curl -s http://localhost:8083/connectors/postgres-source-connector)
if echo "$CONNECTOR_CHECK" | grep -q "error_code"; then
    echo "Registering connector 'postgres-source-connector'..."
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
        http://localhost:8083/connectors/ \
        -d @"$PROJECT_DIR/connectors/postgres-source-connector.json"
else
    echo "Connector 'postgres-source-connector' already exists"
fi

echo ""
echo ""
echo "=========================================="
echo "  CDC Pipeline Started Successfully!     "
echo "=========================================="
echo ""
echo "Services running:"
echo "  - PostgreSQL Source:   localhost:5433"
echo "  - PostgreSQL DataLake: localhost:5434"
echo "  - PostgreSQL Target:   localhost:5435"
echo "  - Kafka:               localhost:29092 (external) / kafka:9092 (internal)"
echo "  - Debezium Connect:    http://localhost:8083"
echo "  - Kafka UI:            http://localhost:8080"
if [ "$WITH_SPARK" = true ]; then
    echo "  - Spark Master:        http://localhost:8081"
    echo "  - Spark Worker:        http://localhost:8082"
fi
if [ "$WITH_RISINGWAVE" = true ]; then
    echo "  - RisingWave:          localhost:4566 (psql -h localhost -p 4566 -U root -d dev)"
    echo "  - RisingWave Dashboard: http://localhost:5691"
    echo "  - MinIO Console:       http://localhost:9001 (admin/password)"
    echo "  - MinIO S3 API:        http://localhost:9000"
fi

echo ""
echo "Next steps:"
echo "  1. Check connector status:        ./scripts/check-connector.sh"
echo "  2. Start Python consumer:         ./scripts/run-consumer.sh"
echo "  3. Start Spark connector:         ./scripts/run-consumer.sh --connector"
if [ "$WITH_RISINGWAVE" = true ]; then
    echo "  4. Initialize RisingWave:         psql -h localhost -p 4566 -U root -d dev -f consumer/risingwave-connector/init-risingwave.sql"
    echo "  5. Test CDC:                      ./scripts/test-cdc.sh"
else
    echo "  4. Test CDC:                      ./scripts/test-cdc.sh"
fi
echo ""
