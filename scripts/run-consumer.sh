#!/bin/bash
# Run the Python CDC consumer
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
USE_SPARK=false
for arg in "$@"; do
    case $arg in
        --spark)
            USE_SPARK=true
            shift
            ;;
    esac
done

echo "=========================================="
echo "  Starting Python CDC Consumer           "
echo "=========================================="

if [ "$USE_SPARK" = true ]; then
    echo "  (Using PySpark for Delta Lake)         "
else
    echo "  (Using delta-rs for Delta Lake)        "
fi
echo "=========================================="

cd "$PROJECT_DIR/python-consumer"

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
echo "Press Ctrl+C to stop"
echo ""
python consumer.py
