#!/bin/bash
# Stop the CDC Data Pipeline
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "  CDC Data Pipeline - Stopping Services  "
echo "=========================================="

cd "$PROJECT_DIR"

# Stop all services
echo "Stopping all services..."
docker-compose down

echo ""
echo "All services stopped."
echo ""
echo "To also remove volumes (data), run:"
echo "  docker-compose down -v"
echo ""
