#!/bin/bash
# Test CDC by making changes to source database
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "  CDC Testing - Making Source DB Changes "
echo "=========================================="

# Source database connection
SOURCE_HOST="localhost"
SOURCE_PORT="5433"
SOURCE_USER="source_user"
SOURCE_DB="source_db"
PGPASSWORD="source_password"

export PGPASSWORD

echo ""
echo "[1] INSERT - Adding new customer..."
psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c \
    "INSERT INTO customers (first_name, last_name, email, phone) VALUES ('Test', 'User', 'test.user@example.com', '+1-555-9999');"

echo ""
echo "[2] UPDATE - Updating customer email..."
psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c \
    "UPDATE customers SET email = 'updated.test@example.com' WHERE email = 'test.user@example.com';"

echo ""
echo "[3] INSERT - Adding new product..."
psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c \
    "INSERT INTO products (name, description, price, stock_quantity, category) VALUES ('Test Product', 'A test product for CDC', 99.99, 10, 'Test');"

echo ""
echo "[4] UPDATE - Updating product stock..."
psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c \
    "UPDATE products SET stock_quantity = 15 WHERE name = 'Test Product';"

echo ""
echo "[5] INSERT - Adding new order..."
psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c \
    "INSERT INTO orders (customer_id, status, total_amount, shipping_address) VALUES (1, 'pending', 199.99, '999 Test St, Test City, TS 99999');"

echo ""
echo "=========================================="
echo "  Test changes completed!                "
echo "=========================================="
echo ""
echo "Check the Python consumer logs to see CDC events being processed."
echo ""
echo "To verify data in target database:"
echo "  ./scripts/verify-data.sh"
echo ""
