#!/bin/bash
# Run CDC Consumer
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse arguments
USE_CONNECTOR=false
USE_SPARK=false
USE_RISINGWAVE=false
for arg in "$@"; do
    case $arg in
        --connector)
            USE_CONNECTOR=true
            shift
            ;;
        --spark)
            USE_SPARK=true
            shift
            ;;
        --risingwave)
            USE_RISINGWAVE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Consumer modes:"
            echo "  (default)      Python consumer with delta-rs"
            echo "  --spark        Python consumer with PySpark"
            echo "  --connector    Spark Structured Streaming in Docker"
            echo "  --risingwave   RisingWave streaming database"
            echo ""
            echo "Options:"
            echo "  -h, --help     Show this help message"
            exit 0
            ;;
    esac
done

# Mode 1: RisingWave Streaming Database -> Delta Lake
if [ "$USE_RISINGWAVE" = true ]; then
    echo "=========================================="
    echo "  RisingWave -> Delta Lake (MinIO)       "
    echo "=========================================="
    echo -e "${GREEN}Kafka CDC -> RisingWave -> Delta Lake (MinIO)${NC}"
    echo ""

    # Check if RisingWave is running (TCP check - RisingWave container doesn't have psql)
    echo -e "${YELLOW}[1/4] Checking RisingWave connectivity...${NC}"
    if ! nc -z localhost 4566 2>/dev/null; then
        echo -e "${RED}Error: RisingWave is not available at localhost:4566${NC}"
        echo "Please start RisingWave first: ./scripts/start.sh --risingwave"
        exit 1
    fi
    echo -e "${GREEN}RisingWave is available${NC}"

    # Check if MinIO is running
    echo -e "${YELLOW}[2/4] Checking MinIO connectivity...${NC}"
    if ! curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo -e "${RED}Error: MinIO is not available${NC}"
        echo "Please start RisingWave first: ./scripts/start.sh --risingwave"
        exit 1
    fi
    echo -e "${GREEN}MinIO is available${NC}"

    # Create Delta Lake tables (required before sinks)
    echo -e "${YELLOW}[3/4] Creating Delta Lake tables in MinIO...${NC}"
    cd "$PROJECT_DIR/consumer/risingwave-connector"

    # Check Python dependencies
    if ! python3 -c "import deltalake, pyarrow" 2>/dev/null; then
        echo "Installing required packages (deltalake, pyarrow)..."
        pip install deltalake pyarrow --quiet
    fi

    python3 create-delta-tables.py

    echo ""
    echo -e "${YELLOW}[4/4] Initializing RisingWave pipeline with Delta Lake sinks...${NC}"
    echo ""

    # Run the initialization SQL
    cd "$PROJECT_DIR"
    psql -h localhost -p 4566 -U root -d dev -f consumer/risingwave-connector/init-risingwave.sql

    echo ""
    echo "=========================================="
    echo -e "${GREEN}  RisingWave -> Delta Lake Ready!       ${NC}"
    echo "=========================================="
    echo ""
    echo "Data is now streaming to Delta Lake in MinIO!"
    echo ""
    echo "Monitoring:"
    echo "  RisingWave Dashboard: http://localhost:5691"
    echo "  MinIO Console:        http://localhost:9001 (admin/password)"
    echo "  Kafka UI:             http://localhost:8080"
    echo ""
    echo "Query Delta Lake with DuckDB:"
    echo "  python3 -c \""
    echo "  import duckdb"
    echo "  duckdb.sql('''INSTALL httpfs; LOAD httpfs;"
    echo "      SET s3_endpoint='localhost:9000';"
    echo "      SET s3_access_key_id='admin';"
    echo "      SET s3_secret_access_key='password';"
    echo "      SET s3_use_ssl=false; SET s3_url_style='path';''')"
    echo "  duckdb.sql(\\\"SELECT * FROM read_parquet('s3://deltalake/customers/*.parquet')\\\").show()"
    echo "  \""
    echo ""
    echo "Connect to RisingWave:"
    echo "  psql -h localhost -p 4566 -U root -d dev"
    echo ""
    exit 0
fi

# Mode 2: Spark Structured Streaming Connector
if [ "$USE_CONNECTOR" = true ]; then
    echo "=========================================="
    echo "  Spark Streaming Connector Mode        "
    echo "=========================================="
    echo -e "${GREEN}Kafka CDC -> Spark Streaming -> Delta Lake${NC}"

    # Check Kafka connectivity
    echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
    if ! nc -z localhost 29092 2>/dev/null; then
        echo -e "${RED}Error: Kafka is not available at localhost:29092${NC}"
        echo "Please start Kafka first: ./scripts/start.sh"
        exit 1
    fi
    echo -e "${GREEN}Kafka is available${NC}"

    # Create directories
    mkdir -p "$PROJECT_DIR/deltalake"
    mkdir -p "$PROJECT_DIR/checkpoints"

    echo ""
    echo -e "${YELLOW}Starting Spark Streaming in Docker...${NC}"
    echo ""
    echo "Monitoring:"
    echo "  Spark UI:  http://localhost:4040"
    echo "  Kafka UI:  http://localhost:8080"
    echo ""
    echo "Press Ctrl+C to stop"
    echo ""

    cd "$PROJECT_DIR"
    docker-compose --profile spark-streaming up --build spark-streaming
    exit 0
fi

# Mode 3: Python Consumer
echo "  Python CDC Consumer Mode               "
echo "------------------------------------------"

if [ "$USE_SPARK" = true ]; then
    echo "  (Using PySpark for Delta Lake)         "
else
    echo "  (Using delta-rs for Delta Lake)        "
fi
echo "------------------------------------------"

cd "$PROJECT_DIR/consumer/python-consumer"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q -r requirements.txt

# Set environment variable for Spark mode
if [ "$USE_SPARK" = true ]; then
    export DELTA_USE_SPARK=true
    echo ""
    echo "Checking Java installation (required for PySpark)..."
    if ! command -v java &> /dev/null; then
        echo "WARNING: Java is not installed. PySpark requires Java 8+."
        echo "Install Java with: brew install openjdk@11 (macOS) or apt install openjdk-11-jdk (Linux)"
        echo ""
    else
        java -version 2>&1 | head -1
    fi
fi

# Create deltalake directory if it doesn't exist
mkdir -p "$PROJECT_DIR/deltalake"

# Run consumer
echo ""
echo "Starting CDC consumer..."
echo "Delta Lake storage: $PROJECT_DIR/deltalake"
echo ""
python consumer.py
