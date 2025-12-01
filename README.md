# PostgreSQL to PostgreSQL Migration Pipeline

An Apache Airflow pipeline for automated, full-refresh migrations from PostgreSQL source to PostgreSQL target. Built with the Astronomer framework for reliable orchestration and easy deployment.

## Features

- **Schema Discovery**: Automatically extract table structures, columns, indexes, and foreign keys from source PostgreSQL
- **Streaming Data Transfer**: Move data efficiently using server-side cursors, keyset pagination, and PostgreSQL's COPY protocol
- **Validation**: Standalone validation DAG verifies migration success through row count comparisons
- **Parallelization**: Transfer multiple tables concurrently using Airflow's dynamic task mapping
- **Large Table Partitioning**: Automatically partitions tables >1M rows into 8 parallel chunks by primary key range

## Quick Start

### Prerequisites

- Docker Desktop (4GB+ RAM recommended)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### 1. Start Databases

```bash
docker-compose up -d
```

This starts:
- **postgres-source**: Source database on port 5434 (database: `source_db`)
- **postgres-target**: Target database on port 5435 (database: `target_db`)

### 2. Start Airflow

```bash
astro dev start
```

Access the Airflow UI at http://localhost:8080

Connections are auto-configured via `.env` file (environment variables).

### 3. Connect Databases to Airflow Network

```bash
./scripts/connect-databases.sh
```

### 4. Run Migration

**Option A: Using pg_dump (recommended for simple migrations)**

```bash
docker exec postgres-source pg_dump -U postgres -d source_db --schema=public --no-owner --no-acl | \
  docker exec -i postgres-target psql -U postgres -d target_db
```

**Option B: Using Airflow DAG (recommended for large datasets)**

```bash
SCHEDULER=$(docker ps --format '{{.Names}}' | grep scheduler)
docker exec $SCHEDULER airflow dags trigger postgres_to_postgres_migration
```

See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for detailed setup instructions including test data creation.

## How It Works

The pipeline executes as a single Airflow DAG with the following stages:

```
Extract Schema -> Create Target Schema -> Create Tables -> Transfer Data (parallel) -> Create Foreign Keys -> Validate -> Report
```

1. **Schema Extraction**: Queries PostgreSQL system catalogs to discover all tables, columns, data types, indexes, and constraints
2. **DDL Generation**: Generates PostgreSQL DDL statements for target tables
3. **Table Creation**: Creates target tables (drops existing tables first)
4. **Data Transfer**: Streams data using keyset pagination with PostgreSQL COPY protocol
5. **Foreign Key Creation**: Adds foreign key constraints after all data is loaded
6. **Validation**: Triggers standalone validation DAG that compares source and target row counts

## Architecture

### Data Transfer Approach

The pipeline uses a streaming architecture optimized for large datasets:

- **Keyset Pagination**: Uses primary key ordering instead of OFFSET/FETCH for efficient chunking
- **PostgreSQL COPY Protocol**: Bulk loads data for maximum throughput
- **Server-Side Cursors**: Streams rows without loading entire result sets into memory
- **Parallel Partitioning**: Large tables split into parallel partitions by PK range

### Validation DAG

A standalone `validate_migration_env` DAG handles validation separately to avoid XCom serialization issues with large result sets.

## Service Endpoints

| Service | URL/Port | Credentials |
|---------|----------|-------------|
| Airflow UI | http://localhost:8080 | No auth required locally |
| PostgreSQL Source | localhost:5434 | postgres / PostgresPassword123 |
| PostgreSQL Target | localhost:5435 | postgres / PostgresPassword123 |

## Project Structure

```
postgres-to-postgres-pipeline/
├── dags/
│   ├── postgres_to_postgres_migration.py  # Main migration DAG
│   └── validate_migration_env.py          # Standalone validation DAG
├── include/
│   └── mssql_pg_migration/
│       ├── schema_extractor.py            # PostgreSQL schema discovery
│       ├── type_mapping.py                # Data type mapping (identity for PG-to-PG)
│       ├── ddl_generator.py               # PostgreSQL DDL generation
│       ├── data_transfer.py               # Streaming data transfer
│       └── validation.py                  # Migration validation
├── tests/
│   └── dags/
│       └── test_dag_example.py            # DAG validation tests
├── docs/
│   └── SETUP_GUIDE.md                     # Detailed setup instructions
├── docker-compose.yml                     # Source and target PostgreSQL containers
├── Dockerfile                             # Astronomer Runtime image
├── requirements.txt                       # Python dependencies
└── airflow_settings.yaml                  # Local connections/variables
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_conn_id` | `postgres_source` | Airflow connection ID for source PostgreSQL |
| `target_conn_id` | `postgres_target` | Airflow connection ID for target PostgreSQL |
| `source_schema` | `public` | Schema to migrate from source |
| `target_schema` | `public` | Target schema in destination |
| `chunk_size` | `100000` | Rows per batch during transfer |
| `exclude_tables` | `[]` | Table patterns to skip |
| `use_unlogged_tables` | `true` | Create tables as UNLOGGED for faster bulk inserts |
| `drop_existing_tables` | `false` | Drop and recreate existing tables instead of truncating |

### When to Use `drop_existing_tables`

By default, the DAG truncates existing tables before loading data. This is faster but assumes the target table schema matches the source.

Set `drop_existing_tables: true` when:
- The source schema has changed (new/removed/modified columns)
- Switching to a different source database with different table structures
- You encounter errors like `column "X" of relation "Y" does not exist`

**Example: Trigger with clean target**
```bash
astro dev run dags trigger postgres_to_postgres_migration \
  --conf '{"drop_existing_tables": true}'
```

## Development

### Validate DAGs

```bash
astro dev parse
```

### Run Tests

```bash
astro dev pytest tests/
```

### View Logs

```bash
astro dev logs -f
```

### Stop Services

```bash
astro dev stop
docker-compose down
```

## Dependencies

- Astronomer Runtime 3.1
- apache-airflow-providers-postgres >= 5.12.0
- psycopg2-binary
- pg8000 >= 1.30.0

## License

See LICENSE file for details.
