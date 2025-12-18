-- Source Database Initialization Script
-- This creates sample tables and data for CDC demonstration

-- Create schema
CREATE SCHEMA IF NOT EXISTS public;

-- Create customers table
CREATE TABLE IF NOT EXISTS public.customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(12, 2),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS public.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES public.orders(id),
    product_id INTEGER REFERENCES public.products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON public.customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON public.products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON public.orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Insert sample customers
INSERT INTO public.customers (first_name, last_name, email, phone) VALUES
    ('John', 'Doe', 'john.doe@example.com', '+1-555-0101'),
    ('Jane', 'Smith', 'jane.smith@example.com', '+1-555-0102'),
    ('Bob', 'Johnson', 'bob.johnson@example.com', '+1-555-0103'),
    ('Alice', 'Williams', 'alice.williams@example.com', '+1-555-0104'),
    ('Charlie', 'Brown', 'charlie.brown@example.com', '+1-555-0105');

-- Insert sample products
INSERT INTO public.products (name, description, price, stock_quantity, category) VALUES
    ('Laptop Pro', 'High-performance laptop for professionals', 1299.99, 50, 'Electronics'),
    ('Wireless Mouse', 'Ergonomic wireless mouse', 49.99, 200, 'Electronics'),
    ('USB-C Hub', '7-in-1 USB-C hub with HDMI', 79.99, 150, 'Electronics'),
    ('Mechanical Keyboard', 'RGB mechanical keyboard', 129.99, 100, 'Electronics'),
    ('Monitor Stand', 'Adjustable monitor stand', 59.99, 75, 'Office');

-- Insert sample orders
INSERT INTO public.orders (customer_id, status, total_amount, shipping_address) VALUES
    (1, 'completed', 1349.98, '123 Main St, New York, NY 10001'),
    (2, 'shipped', 79.99, '456 Oak Ave, Los Angeles, CA 90001'),
    (3, 'pending', 259.97, '789 Pine Rd, Chicago, IL 60601');

-- Insert sample order items
INSERT INTO public.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1299.99),
    (1, 2, 1, 49.99),
    (2, 3, 1, 79.99),
    (3, 4, 2, 129.99);

-- Create publication for logical replication (required for Debezium)
-- This allows Debezium to track changes on all tables
CREATE PUBLICATION debezium_publication FOR ALL TABLES;

-- Grant replication permissions
ALTER USER source_user WITH REPLICATION;

-- Display confirmation
DO $$
BEGIN
    RAISE NOTICE 'Source database initialized successfully!';
    RAISE NOTICE 'Tables created: customers, products, orders, order_items';
    RAISE NOTICE 'Sample data inserted.';
END $$;
