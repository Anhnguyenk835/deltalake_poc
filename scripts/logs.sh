#!/bin/bash
# View logs from services
set -e

SERVICE=${1:-"all"}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

case $SERVICE in
    "kafka")
        docker-compose logs -f kafka
        ;;
    "debezium")
        docker-compose logs -f debezium
        ;;
    "source")
        docker-compose logs -f postgres-source
        ;;
    "datalake")
        docker-compose logs -f postgres-datalake
        ;;
    "target")
        docker-compose logs -f postgres-target
        ;;
    "all")
        docker-compose logs -f
        ;;
    *)
        echo "Usage: $0 [kafka|debezium|source|datalake|target|all]"
        echo ""
        echo "Available services:"
        echo "  kafka     - Kafka broker logs"
        echo "  debezium  - Debezium Kafka Connect logs"
        echo "  source    - Source PostgreSQL logs"
        echo "  datalake  - Data Lake PostgreSQL logs"
        echo "  target    - Target PostgreSQL logs"
        echo "  all       - All service logs (default)"
        exit 1
        ;;
esac
