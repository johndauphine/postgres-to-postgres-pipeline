# PostgreSQL to PostgreSQL Migration Pipeline - Setup Guide

This guide documents the steps to set up and run the local development environment.

## Prerequisites

- Docker Desktop (4GB+ RAM recommended)

## Quick Start

### 1. Start All Services

```bash
docker-compose up -d
```

This starts all containers:
- **postgres-source**: Source database on port 5434 (database: `source_db`)
- **postgres-target**: Target database on port 5435 (database: `target_db`)
- **airflow-postgres**: Airflow metadata database
- **airflow-webserver**: http://localhost:8080 (user: `admin`, password: `admin`)
- **airflow-scheduler**: Runs DAGs
- **airflow-triggerer**: For deferrable operators

Both source and target databases use credentials: `postgres` / `PostgresPassword123`

### 2. Load Test Data in Source Database

You have three options for test data:

#### Option A: StackOverflow 2013 Dataset (Recommended for large-scale testing)

This is a real-world dataset with ~106.5 million rows, ideal for testing parallel partitioning and performance at scale.

```bash
# Create data directory
mkdir -p data
cd data

# Download the PostgreSQL dump (~8 GB compressed, ~28 GB uncompressed)
curl -L -o stackoverflow2013.sql.gz \
  "https://ajaydwivedi.com/share-with-others/stackoverflow2013.sql.gz"
```

**Restore to source database:**

```bash
# Copy dump file to container
docker cp stackoverflow2013.sql.gz postgres-source:/tmp/

# Clean existing schema and restore (streams directly, no need to decompress first)
docker exec postgres-source psql -U postgres -d source_db -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

docker exec postgres-source bash -c \
  "gunzip -c /tmp/stackoverflow2013.sql.gz | psql -U postgres -d source_db"
```

**Verify the data:**

```bash
docker exec postgres-source psql -U postgres -d source_db -c "
SELECT 'votes' as table_name, COUNT(*) as rows FROM votes
UNION ALL SELECT 'comments', COUNT(*) FROM comments
UNION ALL SELECT 'posts', COUNT(*) FROM posts
UNION ALL SELECT 'badges', COUNT(*) FROM badges
UNION ALL SELECT 'users', COUNT(*) FROM users
UNION ALL SELECT 'postlinks', COUNT(*) FROM postlinks
ORDER BY rows DESC;
"
```

Expected output (~106.5 million rows):
```
 table_name |   rows
------------+----------
 votes      | 52928720
 comments   | 24534730
 posts      | 17142169
 badges     |  8042005
 users      |  2465713
 postlinks  |  1421208
```

> **Source:** [Ajay Dwivedi - StackOverflow 2013 PostgreSQL Database](https://ajaydwivedi.com/share-with-others/stackoverflow2013.sql.gz)

#### Option B: StackOverflow 2010 Dataset (Smaller alternative)

A smaller real-world dataset with ~17.8 million rows.

**Prerequisites:** Install a BitTorrent client (aria2 recommended)

```bash
# Install aria2 (macOS)
brew install aria2

# Install aria2 (Ubuntu/Debian)
sudo apt-get install -y aria2

# Create data directory
mkdir -p data
cd data

# Download the PostgreSQL dump via torrent (~1.4 GB)
aria2c --seed-time=0 "magnet:?xt=urn:btih:VCX26QHSFMD6ELDZDGBEAHZBJS7Y3633&dn=stackoverflow-postgres-2010-v0.1&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"
```

**Restore to source database:**

```bash
# Copy dump file to container
docker cp stackoverflow-postgres-2010-v0.1/dump-stackoverflow2010-202408101013.sql \
  postgres-source:/tmp/stackoverflow.sql

# Clean existing schema and restore
docker exec postgres-source psql -U postgres -d source_db -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

docker exec postgres-source pg_restore -U postgres -d source_db \
  /tmp/stackoverflow.sql
```

**Verify the data:**

```bash
docker exec postgres-source psql -U postgres -d source_db -c "
SELECT 'votes' as table_name, COUNT(*) as rows FROM votes
UNION ALL SELECT 'posts', COUNT(*) FROM posts
UNION ALL SELECT 'comments', COUNT(*) FROM comments
UNION ALL SELECT 'badges', COUNT(*) FROM badges
UNION ALL SELECT 'users', COUNT(*) FROM users
ORDER BY rows DESC;
"
```

Expected output (~17.8 million rows):
```
 table_name |   rows
------------+---------
 votes      | 9463619
 posts      | 3680688
 comments   | 3353493
 badges     | 1098220
 users      |  298611
```

> **Source:** [Smart Postgres - Stack Overflow Sample Database](https://smartpostgres.com/posts/announcing-early-access-to-the-stack-overflow-sample-database-download-for-postgres/)

#### Option C: Simple Sample Data (Quick setup)

For basic testing with minimal data:

```bash
docker exec postgres-source psql -U postgres -d source_db -c "
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

### 3. Run the Migration

**Option A: Using pg_dump (Recommended for quick setup)**

```bash
docker exec postgres-source pg_dump -U postgres -d source_db --schema=public --no-owner --no-acl | \
  docker exec -i postgres-target psql -U postgres -d target_db
```

**Option B: Using Airflow DAG**

```bash
docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration
```

### 4. Verify Migration

```bash
docker exec postgres-target psql -U postgres -d target_db -c "
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
| Airflow UI | http://localhost:8080 | admin / admin |
| PostgreSQL Source | localhost:5434 | postgres / PostgresPassword123 |
| PostgreSQL Target | localhost:5435 | postgres / PostgresPassword123 |

## Common Commands

```bash
# Stop all services
docker-compose down

# View logs
docker-compose logs -f

# Restart services
docker-compose restart

# Rebuild after code changes
docker-compose down && docker-compose build && docker-compose up -d

# Check for DAG import errors
docker exec airflow-scheduler airflow dags list-import-errors

# Run tests
docker exec airflow-scheduler pytest tests/

# Connect to source database
docker exec -it postgres-source psql -U postgres -d source_db

# Connect to target database
docker exec -it postgres-target psql -U postgres -d target_db
```

## Troubleshooting

### Connection Refused Errors

All services are on the same Docker network, so connection issues should be rare. If tasks fail with connection errors:

1. Check all containers are running:
```bash
docker-compose ps
```

2. Restart the affected service:
```bash
docker-compose restart airflow-scheduler
```

### Containers Not Starting

Check for port conflicts:
```bash
lsof -i :8080  # Airflow webserver
lsof -i :5434  # PostgreSQL source
lsof -i :5435  # PostgreSQL target
```

Stop conflicting services or change ports in docker-compose.yml.

### DAG Not Visible

Wait for the scheduler to pick up new DAGs (up to 30 seconds), or check for import errors:

```bash
docker exec airflow-scheduler airflow dags list-import-errors
```

### Rebuild After Code Changes

If you modify the Dockerfile or requirements.txt:

```bash
docker-compose down
docker-compose build
docker-compose up -d
```
