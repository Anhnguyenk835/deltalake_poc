-- Creates Kafka tables, materialized views, and Delta Lake sinks
-- for streaming CDC data from PostgreSQL via RisingWave to MinIO.
--
-- IMPORTANT: Delta Lake tables must be pre-created before running this script!
-- Run: ./consumer/risingwave-connector/init-delta-tables.sh
--
-- Usage:
--   psql -h localhost -p 4566 -U root -d dev -f init-risingwave-deltalake.sql
-- ============================================

-- Using CREATE TABLE with FORMAT DEBEZIUM as required in RisingWave v2.5+
-- PRIMARY KEY is required for Debezium format to handle upserts

CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
WITH (
    connector = 'kafka',
    topic = 'cdc.public.customers',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    price DECIMAL,
    stock_quantity INT,
    category VARCHAR,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
WITH (
    connector = 'kafka',
    topic = 'cdc.public.products',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    status VARCHAR,
    total_amount DECIMAL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
WITH (
    connector = 'kafka',
    topic = 'cdc.public.orders',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;

CREATE TABLE IF NOT EXISTS order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL,
    created_at TIMESTAMPTZ
)
WITH (
    connector = 'kafka',
    topic = 'cdc.public.order_items',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;


-- ANALYTICS MATERIALIZED VIEWS
-- These views are incrementally maintained and always up-to-date

-- Order analytics by day and status
CREATE MATERIALIZED VIEW IF NOT EXISTS order_analytics AS
SELECT
    DATE_TRUNC('day', order_date) AS order_day,
    status,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value
FROM orders
GROUP BY DATE_TRUNC('day', order_date), status;

-- Customer order summary
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_order_summary AS
SELECT
    c.id AS customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.first_name, c.last_name, c.email;

-- Product inventory status
CREATE MATERIALIZED VIEW IF NOT EXISTS product_inventory AS
SELECT
    id,
    name,
    category,
    price,
    stock_quantity,
    CASE
        WHEN stock_quantity = 0 THEN 'Out of Stock'
        WHEN stock_quantity < 10 THEN 'Low Stock'
        ELSE 'In Stock'
    END AS stock_status
FROM products;


-- DELTA LAKE SINKS (MinIO)
-- Sinks CDC data and analytics to pre-created Delta tables in MinIO
-- Note: Delta tables must exist before creating these sinks!

-- Sink raw CDC tables to Delta Lake
CREATE SINK IF NOT EXISTS customers_delta_sink FROM customers
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/customers/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

CREATE SINK IF NOT EXISTS products_delta_sink FROM products
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/products/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

CREATE SINK IF NOT EXISTS orders_delta_sink FROM orders
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/orders/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

CREATE SINK IF NOT EXISTS order_items_delta_sink FROM order_items
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/order_items/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

-- Sink analytics materialized views to Delta Lake
CREATE SINK IF NOT EXISTS order_analytics_delta_sink FROM order_analytics
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/order_analytics/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

CREATE SINK IF NOT EXISTS customer_order_summary_delta_sink FROM customer_order_summary
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/customer_order_summary/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);

CREATE SINK IF NOT EXISTS product_inventory_delta_sink FROM product_inventory
WITH (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only = 'true',
    location = 's3a://deltalake/product_inventory/',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'admin',
    s3.secret.key = 'password',
    s3.region = 'us-east-1'
);


-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Run these after setup to verify the pipeline is working:
--
-- Check tables:
--   SELECT name FROM rw_tables WHERE name NOT LIKE 'rw_%';
--
-- Check materialized views:
--   SELECT name FROM rw_materialized_views;
--
-- Check sinks:
--   SELECT name FROM rw_sinks;
--
-- Query real-time CDC data:
--   SELECT * FROM customers LIMIT 10;
--   SELECT * FROM products LIMIT 10;
--   SELECT * FROM orders LIMIT 10;
--
-- Query analytics:
--   SELECT * FROM order_analytics;
--   SELECT * FROM customer_order_summary ORDER BY lifetime_value DESC LIMIT 10;
--   SELECT * FROM product_inventory WHERE stock_status != 'In Stock';
--
-- View Delta Lake data in MinIO Console:
--   http://localhost:9001 (admin/password)
--   Navigate to 'deltalake' bucket
