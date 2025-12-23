#!/bin/bash
# Run Spark Structured Streaming CDC Pipeline
# Consumes Kafka CDC events and writes to Delta Lake

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Spark Structured Streaming: Kafka CDC -> Delta Lake${NC}"

# Load environment variables
if [ -f "$PROJECT_DIR/.env.local" ]; then
    echo -e "${YELLOW}Loading environment from .env.local${NC}"
    export $(grep -v '^#' "$PROJECT_DIR/.env.local" | xargs)
fi

# Default configuration
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
export DELTA_LAKE_PATH="${DELTA_LAKE_PATH:-$PROJECT_DIR/deltalake}"
export SPARK_CHECKPOINT_PATH="${SPARK_CHECKPOINT_PATH:-$PROJECT_DIR/checkpoints}"
export SPARK_TRIGGER_INTERVAL="${SPARK_TRIGGER_INTERVAL:-10 seconds}"
export SPARK_MAX_OFFSETS_PER_TRIGGER="${SPARK_MAX_OFFSETS_PER_TRIGGER:-10000}"
export SPARK_STARTING_OFFSETS="${SPARK_STARTING_OFFSETS:-earliest}"

echo ""
echo "Configuration:"
echo "  Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo "  Delta Lake Path: $DELTA_LAKE_PATH"
echo "  Checkpoint Path: $SPARK_CHECKPOINT_PATH"
echo "  Trigger Interval: $SPARK_TRIGGER_INTERVAL"
echo "  Max Offsets/Trigger: $SPARK_MAX_OFFSETS_PER_TRIGGER"
echo "  Starting Offsets: $SPARK_STARTING_OFFSETS"
echo ""

# Create directories if they don't exist
mkdir -p "$DELTA_LAKE_PATH"
mkdir -p "$SPARK_CHECKPOINT_PATH"

# Check if running in Docker mode
if [ "$1" == "--docker" ]; then
    echo -e "${YELLOW}Starting in Docker mode...${NC}"
    cd "$PROJECT_DIR"
    docker-compose --profile spark-streaming up --build spark-streaming
    exit 0
fi

# Check if Kafka is available
echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
if ! nc -z localhost 29092 2>/dev/null; then
    echo -e "${RED}Error: Kafka is not available at localhost:29092${NC}"
    echo "Please start Kafka first: docker-compose up -d kafka"
    exit 1
fi
echo -e "${GREEN}Kafka is available${NC}"

# Run with spark-submit
echo ""
echo -e "${YELLOW}Starting Spark Structured Streaming...${NC}"
echo "Press Ctrl+C to stop"
echo ""

cd "$PROJECT_DIR/python-consumer"

spark-submit \
    --master "local[*]" \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.driver.memory=2g" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.ui.port=4040" \
    spark_streaming.py
