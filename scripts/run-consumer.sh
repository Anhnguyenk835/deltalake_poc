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
    esac
done

# Mode 1: Spark Structured Streaming Connector
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

# Mode 2: Python Consumer
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
