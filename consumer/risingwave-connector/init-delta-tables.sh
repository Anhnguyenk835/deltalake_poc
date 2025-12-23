#!/bin/bash
# Create Delta Lake tables in MinIO for RisingWave sinks
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=========================================="
echo "  Delta Lake Table Creator               "
echo "=========================================="
echo ""

# Check if MinIO is running
echo -e "${YELLOW}Checking MinIO connectivity...${NC}"
if ! curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo -e "${RED}Error: MinIO is not available at localhost:9000${NC}"
    echo "Please start RisingWave first: ./scripts/start.sh --risingwave"
    exit 1
fi
echo -e "${GREEN}MinIO is available${NC}"

# Check Python dependencies
echo -e "${YELLOW}Checking Python dependencies...${NC}"
if ! python3 -c "import deltalake, pyarrow" 2>/dev/null; then
    echo "Installing required packages..."
    pip install deltalake pyarrow --quiet
fi
echo -e "${GREEN}Dependencies ready${NC}"

echo ""
echo -e "${YELLOW}Creating Delta tables in MinIO...${NC}"
echo ""

# Run the Python script
cd "$SCRIPT_DIR"
python3 create-delta-tables.py

echo ""
echo -e "${GREEN}Done!${NC}"
echo ""
echo "To initialize RisingWave with Delta Lake sinks, run:"
echo "  psql -h localhost -p 4566 -U root -d dev -f consumer/risingwave-connector/init-risingwave-deltalake.sql"
