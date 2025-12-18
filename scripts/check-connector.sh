#!/bin/bash
# Check Debezium connector status
set -e

echo "=========================================="
echo "  Debezium Connector Status              "
echo "=========================================="

echo ""
echo "[1] Available connectors:"
curl -s http://localhost:8083/connectors | jq '.'

echo ""
echo "[2] PostgreSQL Source Connector Status:"
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq '.'

echo ""
echo "[3] Connector Configuration:"
curl -s http://localhost:8083/connectors/postgres-source-connector/config | jq '.'

echo ""
echo "[4] Kafka Topics:"
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

echo ""
