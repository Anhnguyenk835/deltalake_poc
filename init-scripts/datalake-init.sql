-- Data Lake Database Initialization Script
-- This creates tables to store CDC events and analytics data

-- Create schema for raw CDC events
CREATE SCHEMA IF NOT EXISTS cdc_raw;

-- Create schema for processed/transformed data
CREATE SCHEMA IF NOT EXISTS cdc_processed;

-- Create CDC events log table (stores all raw CDC events)
CREATE TABLE IF NOT EXISTS cdc_raw.cdc_events (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(255) NOT NULL,
    operation VARCHAR(10) NOT NULL, -- 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    event_key JSONB,
    before_data JSONB,
    after_data JSONB,
    source_metadata JSONB,
    kafka_topic VARCHAR(255),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    event_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for efficient querying
CREATE INDEX idx_cdc_events_source_table ON cdc_raw.cdc_events(source_table);
CREATE INDEX idx_cdc_events_operation ON cdc_raw.cdc_events(operation);
CREATE INDEX idx_cdc_events_timestamp ON cdc_raw.cdc_events(event_timestamp);
CREATE INDEX idx_cdc_events_processed_at ON cdc_raw.cdc_events(processed_at);

-- Create customers mirror table (processed/current state)
CREATE TABLE IF NOT EXISTS cdc_processed.customers (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products mirror table (processed/current state)
CREATE TABLE IF NOT EXISTS cdc_processed.products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price DECIMAL(10, 2),
    stock_quantity INTEGER,
    category VARCHAR(100),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders mirror table (processed/current state)
CREATE TABLE IF NOT EXISTS cdc_processed.orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TIMESTAMP,
    status VARCHAR(50),
    total_amount DECIMAL(12, 2),
    shipping_address TEXT,
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items mirror table (processed/current state)
CREATE TABLE IF NOT EXISTS cdc_processed.order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    source_created_at TIMESTAMP,
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create analytics summary view
CREATE OR REPLACE VIEW cdc_processed.analytics_summary AS
SELECT
    'customers' as table_name,
    COUNT(*) as total_records,
    MAX(synced_at) as last_sync
FROM cdc_processed.customers
UNION ALL
SELECT
    'products' as table_name,
    COUNT(*) as total_records,
    MAX(synced_at) as last_sync
FROM cdc_processed.products
UNION ALL
SELECT
    'orders' as table_name,
    COUNT(*) as total_records,
    MAX(synced_at) as last_sync
FROM cdc_processed.orders
UNION ALL
SELECT
    'order_items' as table_name,
    COUNT(*) as total_records,
    MAX(synced_at) as last_sync
FROM cdc_processed.order_items;

-- Create CDC statistics view
CREATE OR REPLACE VIEW cdc_raw.cdc_statistics AS
SELECT
    source_table,
    operation,
    COUNT(*) as event_count,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM cdc_raw.cdc_events
GROUP BY source_table, operation
ORDER BY source_table, operation;

-- Display confirmation
DO $$
BEGIN
    RAISE NOTICE 'Data Lake database initialized successfully!';
    RAISE NOTICE 'Schemas created: cdc_raw, cdc_processed';
    RAISE NOTICE 'Ready to receive CDC events.';
END $$;
