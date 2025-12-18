-- Target Database Initialization Script
-- This creates tables to receive replicated data from CDC

-- Create schema
CREATE SCHEMA IF NOT EXISTS public;

-- Create customers table (mirror of source)
CREATE TABLE IF NOT EXISTS public.customers (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table (mirror of source)
CREATE TABLE IF NOT EXISTS public.products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price DECIMAL(10, 2),
    stock_quantity INTEGER,
    category VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table (mirror of source)
CREATE TABLE IF NOT EXISTS public.orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TIMESTAMP,
    status VARCHAR(50),
    total_amount DECIMAL(12, 2),
    shipping_address TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table (mirror of source)
CREATE TABLE IF NOT EXISTS public.order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    created_at TIMESTAMP,
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CDC sync status table
CREATE TABLE IF NOT EXISTS public.cdc_sync_status (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    last_offset BIGINT,
    last_partition INTEGER,
    records_processed BIGINT DEFAULT 0,
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name)
);

-- Create indexes for performance
CREATE INDEX idx_customers_email ON public.customers(email);
CREATE INDEX idx_products_category ON public.products(category);
CREATE INDEX idx_orders_customer_id ON public.orders(customer_id);
CREATE INDEX idx_orders_status ON public.orders(status);
CREATE INDEX idx_order_items_order_id ON public.order_items(order_id);

-- Create view for sync summary
CREATE OR REPLACE VIEW public.sync_summary AS
SELECT
    'customers' as table_name,
    COUNT(*) as record_count,
    MAX(__synced_at) as last_synced
FROM public.customers
UNION ALL
SELECT
    'products' as table_name,
    COUNT(*) as record_count,
    MAX(__synced_at) as last_synced
FROM public.products
UNION ALL
SELECT
    'orders' as table_name,
    COUNT(*) as record_count,
    MAX(__synced_at) as last_synced
FROM public.orders
UNION ALL
SELECT
    'order_items' as table_name,
    COUNT(*) as record_count,
    MAX(__synced_at) as last_synced
FROM public.order_items;

-- Display confirmation
DO $$
BEGIN
    RAISE NOTICE 'Target database initialized successfully!';
    RAISE NOTICE 'Tables created: customers, products, orders, order_items';
    RAISE NOTICE 'Ready to receive replicated data.';
END $$;
