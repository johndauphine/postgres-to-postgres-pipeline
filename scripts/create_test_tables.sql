-- Test tables for NTILE partitioning validation
-- Creates tables with different PK types to test various partitioning scenarios
-- Ported from mssql-to-postgres-pipeline for PostgreSQL

-- Drop existing test tables
DROP TABLE IF EXISTS uuid_orders;
DROP TABLE IF EXISTS string_products;
DROP TABLE IF EXISTS composite_order_details;
DROP TABLE IF EXISTS sparse_int_table;

------------------------------------------------------------
-- 1. UUID Primary Key Table (~2M rows)
-- Tests NTILE partitioning with UUID primary keys
------------------------------------------------------------
CREATE TABLE uuid_orders (
    order_id UUID PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP,
    total_amount DECIMAL(18,2),
    status VARCHAR(20)
);

-- Insert 2M rows with UUIDs in batches
DO $$
DECLARE
    batch_size INT := 50000;
    total_rows INT := 2000000;
    i INT := 0;
BEGIN
    RAISE NOTICE 'Inserting uuid_orders (2M rows)...';

    WHILE i < total_rows LOOP
        INSERT INTO uuid_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT
            gen_random_uuid(),
            (random() * 100000)::INT,
            CURRENT_TIMESTAMP - ((random() * 365)::INT || ' days')::INTERVAL,
            (random() * 10000)::DECIMAL(18,2),
            CASE (random() * 4)::INT
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Shipped'
                WHEN 2 THEN 'Delivered'
                ELSE 'Cancelled'
            END
        FROM generate_series(1, batch_size);

        i := i + batch_size;
        IF i % 500000 = 0 THEN
            RAISE NOTICE '  % rows inserted...', i;
        END IF;
    END LOOP;

    RAISE NOTICE 'uuid_orders complete: % rows', total_rows;
END $$;

------------------------------------------------------------
-- 2. String Primary Key Table (~1.5M rows)
-- Tests NTILE partitioning with VARCHAR primary keys
------------------------------------------------------------
CREATE TABLE string_products (
    product_code VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(18,2),
    stock_quantity INT
);

-- Insert 1.5M rows with string PKs in batches
DO $$
DECLARE
    batch_size INT := 50000;
    total_rows INT := 1500000;
    i INT := 0;
BEGIN
    RAISE NOTICE 'Inserting string_products (1.5M rows)...';

    WHILE i < total_rows LOOP
        INSERT INTO string_products (product_code, product_name, category, price, stock_quantity)
        SELECT
            'PRD-' || LPAD((i + row_num)::TEXT, 7, '0'),
            'Product ' || (i + row_num)::TEXT,
            CASE (random() * 5)::INT
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Clothing'
                WHEN 2 THEN 'Food'
                WHEN 3 THEN 'Books'
                ELSE 'Other'
            END,
            (random() * 1000)::DECIMAL(18,2),
            (random() * 1000)::INT
        FROM generate_series(1, batch_size) AS row_num;

        i := i + batch_size;
        IF i % 500000 = 0 THEN
            RAISE NOTICE '  % rows inserted...', i;
        END IF;
    END LOOP;

    RAISE NOTICE 'string_products complete: % rows', total_rows;
END $$;

------------------------------------------------------------
-- 3. Composite Primary Key Table (~2M rows)
-- Tests table handling with composite primary keys
-- Note: Current DAG implementation uses first PK column for NTILE
------------------------------------------------------------
CREATE TABLE composite_order_details (
    order_id INT NOT NULL,
    line_number INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2),
    discount DECIMAL(5,2),
    PRIMARY KEY (order_id, line_number)
);

-- Insert 2M rows with composite PK (5 line items per order)
DO $$
DECLARE
    batch_size INT := 50000;
    total_rows INT := 2000000;
    i INT := 0;
    order_num INT := 1;
BEGIN
    RAISE NOTICE 'Inserting composite_order_details (2M rows)...';

    WHILE i < total_rows LOOP
        INSERT INTO composite_order_details (order_id, line_number, product_id, quantity, unit_price, discount)
        SELECT
            order_num + ((row_num - 1) / 5),
            ((row_num - 1) % 5) + 1,
            (random() * 10000)::INT,
            (random() * 20)::INT + 1,
            (random() * 500)::DECIMAL(18,2),
            (random() * 30)::DECIMAL(5,2)
        FROM generate_series(1, batch_size) AS row_num;

        i := i + batch_size;
        order_num := order_num + (batch_size / 5);
        IF i % 500000 = 0 THEN
            RAISE NOTICE '  % rows inserted...', i;
        END IF;
    END LOOP;

    RAISE NOTICE 'composite_order_details complete: % rows', total_rows;
END $$;

------------------------------------------------------------
-- 4. Sparse Integer PK Table (~1.5M rows with gaps)
-- Tests NTILE partitioning with sparse/gapped integer PKs
-- This demonstrates why NTILE is superior to arithmetic division
------------------------------------------------------------
CREATE TABLE sparse_int_table (
    id INT PRIMARY KEY,
    data VARCHAR(100),
    created_at TIMESTAMP
);

-- Insert 1.5M rows with sparse IDs (gaps between batches)
DO $$
DECLARE
    batch_size INT := 1000;
    total_rows INT := 1500000;
    i INT := 0;
    base_id INT := 1;
BEGIN
    RAISE NOTICE 'Inserting sparse_int_table (1.5M rows with gaps)...';

    WHILE i < total_rows LOOP
        INSERT INTO sparse_int_table (id, data, created_at)
        SELECT
            base_id + row_num - 1,
            'Data row ' || (base_id + row_num - 1)::TEXT,
            CURRENT_TIMESTAMP - ((random() * 86400)::INT || ' seconds')::INTERVAL
        FROM generate_series(1, batch_size) AS row_num;

        i := i + batch_size;
        base_id := base_id + 10000;  -- Jump 10000 between batches = lots of gaps
        IF i % 500000 = 0 THEN
            RAISE NOTICE '  % rows inserted...', i;
        END IF;
    END LOOP;

    RAISE NOTICE 'sparse_int_table complete: % rows', total_rows;
END $$;

------------------------------------------------------------
-- Summary
------------------------------------------------------------
SELECT 'uuid_orders' as table_name, COUNT(*) as rows, 'UUID' as pk_type FROM uuid_orders
UNION ALL
SELECT 'string_products', COUNT(*), 'VARCHAR(20)' FROM string_products
UNION ALL
SELECT 'composite_order_details', COUNT(*), 'INT, INT (composite)' FROM composite_order_details
UNION ALL
SELECT 'sparse_int_table', COUNT(*), 'INT (sparse)' FROM sparse_int_table
ORDER BY rows DESC;
