# PostgreSQL to PostgreSQL Migration Pipeline - Setup Guide

This guide documents the steps to set up and run the local development environment.

## Prerequisites

- Docker Desktop (4GB+ RAM recommended)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

## Quick Start

### 1. Start PostgreSQL Source and Target Databases

```bash
docker-compose up -d
```

This starts two PostgreSQL containers:
- **pg-source**: Source database on port 5434
- **pg-target**: Target database on port 5435

Both use credentials: `postgres` / `PostgresPassword123`

### 2. Start Airflow

```bash
astro dev start
```

This starts 5 Airflow containers:
- API Server (Webserver): http://localhost:8080
- Scheduler
- DAG Processor
- Triggerer
- PostgreSQL (Airflow metadata)

**Note:** On first start, you may see connection errors. This is expected - we'll add the connections next.

### 3. Connect PostgreSQL Databases to Airflow Network

The source and target databases need to be on the same Docker network as Airflow:

```bash
docker network connect postgres-to-postgres-pipeline_886850_airflow pg-source
docker network connect postgres-to-postgres-pipeline_886850_airflow pg-target
```

**Note:** The network name includes a unique ID. Find yours with:
```bash
docker network ls | grep airflow
```

### 4. Add Airflow Connections

```bash
# Get the scheduler container name
SCHEDULER=$(docker ps --format '{{.Names}}' | grep scheduler)

# Add source connection
docker exec $SCHEDULER airflow connections add postgres_source \
  --conn-type postgres \
  --conn-host pg-source \
  --conn-schema source_db \
  --conn-login postgres \
  --conn-password PostgresPassword123 \
  --conn-port 5432

# Add target connection
docker exec $SCHEDULER airflow connections add postgres_target \
  --conn-type postgres \
  --conn-host pg-target \
  --conn-schema target_db \
  --conn-login postgres \
  --conn-password PostgresPassword123 \
  --conn-port 5432
```

### 5. Create Test Data in Source Database

```bash
docker exec pg-source psql -U postgres -d source_db -c "
-- Create sample tables
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12,2),
    status VARCHAR(20) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);

-- Insert sample data
INSERT INTO users (username, email) VALUES
    ('john_doe', 'john@example.com'),
    ('jane_smith', 'jane@example.com'),
    ('bob_wilson', 'bob@example.com'),
    ('alice_jones', 'alice@example.com'),
    ('charlie_brown', 'charlie@example.com')
ON CONFLICT (username) DO NOTHING;

INSERT INTO products (name, description, price, category, stock_quantity) VALUES
    ('Laptop Pro 15', 'High-performance laptop', 1299.99, 'Electronics', 50),
    ('Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 'Electronics', 200),
    ('USB-C Hub', '7-in-1 USB-C adapter', 49.99, 'Electronics', 150),
    ('Standing Desk', 'Adjustable height desk', 599.99, 'Furniture', 30),
    ('Office Chair', 'Ergonomic office chair', 349.99, 'Furniture', 45),
    ('Monitor 27inch', '4K IPS monitor', 449.99, 'Electronics', 75),
    ('Keyboard Mechanical', 'RGB mechanical keyboard', 89.99, 'Electronics', 120),
    ('Webcam HD', '1080p webcam', 79.99, 'Electronics', 90),
    ('Desk Lamp', 'LED desk lamp', 39.99, 'Furniture', 200),
    ('Cable Organizer', 'Desktop cable kit', 19.99, 'Accessories', 300)
ON CONFLICT DO NOTHING;

INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 1329.98, 'completed'),
    (2, 649.98, 'completed'),
    (3, 89.99, 'shipped'),
    (1, 499.98, 'pending'),
    (4, 1749.98, 'completed'),
    (5, 119.98, 'processing')
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1299.99), (1, 2, 1, 29.99),
    (2, 4, 1, 599.99), (2, 3, 1, 49.99),
    (3, 7, 1, 89.99),
    (4, 6, 1, 449.99), (4, 3, 1, 49.99),
    (5, 1, 1, 1299.99), (5, 6, 1, 449.99),
    (6, 2, 1, 29.99), (6, 7, 1, 89.99)
ON CONFLICT DO NOTHING;
"
```

### 6. Run the Migration

**Option A: Using pg_dump (Recommended for quick setup)**

```bash
docker exec pg-source pg_dump -U postgres -d source_db --schema=public --no-owner --no-acl | \
  docker exec -i pg-target psql -U postgres -d target_db
```

**Option B: Using Airflow DAG**

```bash
SCHEDULER=$(docker ps --format '{{.Names}}' | grep scheduler)
docker exec $SCHEDULER airflow dags trigger postgres_to_postgres_migration
```

### 7. Verify Migration

```bash
docker exec pg-target psql -U postgres -d target_db -c "
SELECT 'users' as table_name, COUNT(*) as rows FROM users
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items;
"
```

Expected output:
```
 table_name  | rows
-------------+------
 users       |    5
 products    |   10
 orders      |    6
 order_items |   11
```

## Service Endpoints

| Service | URL/Port | Credentials |
|---------|----------|-------------|
| Airflow UI | http://localhost:8080 | No auth required locally |
| PostgreSQL Source | localhost:5434 | postgres / PostgresPassword123 |
| PostgreSQL Target | localhost:5435 | postgres / PostgresPassword123 |

## Common Commands

```bash
# Stop all services
astro dev stop
docker-compose down

# View Airflow logs
astro dev logs -f

# Restart Airflow
astro dev restart

# Parse/validate DAGs
astro dev parse

# Run tests
astro dev pytest tests/

# Connect to source database
docker exec -it pg-source psql -U postgres -d source_db

# Connect to target database
docker exec -it pg-target psql -U postgres -d target_db
```

## Troubleshooting

### Connection Refused Errors

If Airflow tasks fail with connection errors, ensure the PostgreSQL containers are connected to the Airflow network:

```bash
docker network connect postgres-to-postgres-pipeline_886850_airflow pg-source
docker network connect postgres-to-postgres-pipeline_886850_airflow pg-target
```

### Connections Not Found

If DAGs fail with "connection not defined" errors, re-add the connections:

```bash
SCHEDULER=$(docker ps --format '{{.Names}}' | grep scheduler)
docker exec $SCHEDULER airflow connections delete postgres_source 2>/dev/null
docker exec $SCHEDULER airflow connections delete postgres_target 2>/dev/null
# Then re-add as shown in step 4
```

### Container Name Mismatch

The Astro project name is set in `.astro/config.yaml`. If container names don't match expected patterns, check this file:

```yaml
project:
    name: postgres-to-postgres-pipeline
```

### DAG Task Failures

The `create_target_tables` task may fail due to Airflow 3 compatibility issues with parameter passing. If this happens, use the pg_dump approach (Option A) for migration.

## Code Fixes Applied

The following fixes were needed for Airflow 3 compatibility:

1. **Parameter tuple syntax** in `include/mssql_pg_migration/schema_extractor.py`:
   - Changed `parameters=[value]` to `parameters=(value,)` for all PostgresHook queries

2. **SQL LIKE escape** in `include/mssql_pg_migration/schema_extractor.py`:
   - Changed `LIKE 'nextval%'` to `LIKE 'nextval%%'` to escape the percent sign

3. **Project name** in `.astro/config.yaml`:
   - Updated from `mssql-to-postgres-pipeline` to `postgres-to-postgres-pipeline`

4. **Airflow connections** in `airflow_settings.yaml`:
   - Added `postgres_source` and `postgres_target` connection configurations
