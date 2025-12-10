# PostgreSQL-to-PostgreSQL Migration Pipeline

**Technical Documentation for Senior Data Engineers**

---

## Overview

High-throughput, Airflow-orchestrated data migration pipeline for PostgreSQL-to-PostgreSQL transfers. Designed for full-refresh warehouse migrations with automatic partitioning, parallel execution, and resumable chunked transfers.

**Key Metrics** (Stack Overflow 2013 dataset, 107M rows):
- Throughput: ~220,000 rows/second
- Duration: ~8 minutes for 107M rows across 16 tables
- Parallelism: Up to 40+ concurrent partition tasks

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AIRFLOW SCHEDULER                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  postgres_to_postgres_migration DAG                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                                                                       │   │
│  │  extract_source_schema ──► create_target_schema ──► create_tables    │   │
│  │                                                           │           │   │
│  │                              ┌────────────────────────────┴───┐       │   │
│  │                              ▼                                ▼       │   │
│  │                    prepare_regular_tables      prepare_large_partitions  │
│  │                              │                                │       │   │
│  │                              ▼                                ▼       │   │
│  │                    ┌─────────────────┐          ┌─────────────────┐   │   │
│  │                    │ transfer_table  │          │ transfer_part   │   │   │
│  │                    │ .expand([...])  │          │ .expand([...])  │   │   │
│  │                    │  (N tasks)      │          │  (M tasks)      │   │   │
│  │                    └────────┬────────┘          └────────┬────────┘   │   │
│  │                             │                            │            │   │
│  │                             └──────────┬─────────────────┘            │   │
│  │                                        ▼                              │   │
│  │                            collect_all_results                        │   │
│  │                                        │                              │   │
│  │                    ┌───────────────────┼───────────────────┐          │   │
│  │                    ▼                   ▼                   ▼          │   │
│  │          convert_to_logged    create_primary_keys   trigger_validation│   │
│  │                    │                   │                   │          │   │
│  │                    └───────────────────┴───────────────────┘          │   │
│  │                                        │                              │   │
│  │                                        ▼                              │   │
│  │                          generate_migration_summary                   │   │
│  │                                                                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
         │                                                      │
         ▼                                                      ▼
┌─────────────────┐                                  ┌─────────────────┐
│ postgres-source │◄──── COPY TO STDOUT ────────────►│ postgres-target │
│    (port 5434)  │      Keyset Pagination           │    (port 5435)  │
│                 │      Chunked Streaming           │                 │
│   source_db     │◄──── COPY FROM STDIN ───────────►│   target_db     │
└─────────────────┘                                  └─────────────────┘
```

---

## Core Components

### 1. Schema Extractor (`include/pg_migration/schema_extractor.py`)

Discovers source schema via PostgreSQL system catalogs.

```python
from include.pg_migration.schema_extractor import extract_schema_info

tables = extract_schema_info(
    conn_id='postgres_source',
    schema_name='public',
    exclude_tables=['temp_*', '*_backup']  # Wildcard support
)
# Returns: List[Dict] with table_name, columns, primary_key, indexes, row_count
```

**Extracts**:
- Tables with accurate row counts (via `pg_class.reltuples`)
- Column metadata (types, nullability, defaults, identity)
- Primary keys (including composite)
- Indexes and foreign keys
- Check constraints

### 2. Data Transfer (`include/pg_migration/data_transfer.py`)

Streaming bulk transfer using PostgreSQL COPY protocol with keyset pagination.

```python
from include.pg_migration.data_transfer import transfer_table_data

result = transfer_table_data(
    source_conn_id='postgres_source',
    target_conn_id='postgres_target',
    table_info={
        'table_name': 'votes',
        'source_schema': 'public',
        'target_schema': 'public',
        'columns': ['id', 'post_id', 'vote_type', 'created_at']
    },
    chunk_size=100_000,
    where_clause=('id > %s AND id <= %s', [1_000_000, 2_000_000])  # For partitions
)
```

**Algorithm**: Keyset Pagination
```sql
-- First chunk
SELECT * FROM votes ORDER BY (id, ctid) LIMIT 100000

-- Subsequent chunks
SELECT * FROM votes
WHERE (id, ctid) > (last_id, last_ctid)
ORDER BY (id, ctid) LIMIT 100000
```

**Why Keyset over OFFSET?**
| Approach | Complexity | 10M rows, page 100 |
|----------|------------|---------------------|
| OFFSET/LIMIT | O(n) | Scans 10M rows |
| Keyset | O(log n) | Index seek only |

**Composite PK Support**:
```sql
-- Tables with multi-column PKs
WHERE (pk1, pk2, ctid) > (%s, %s, %s)
ORDER BY (pk1, pk2, ctid)
```

### 3. DDL Generator (`include/pg_migration/ddl_generator.py`)

Generates PostgreSQL DDL with optimization options.

```python
from include.pg_migration.ddl_generator import DDLGenerator

gen = DDLGenerator('postgres_target')

# Create UNLOGGED table (faster bulk loads, no WAL)
ddl = gen.generate_create_table(schema, unlogged=True)
gen.execute_ddl([ddl])

# After data load: convert to durable
gen.execute_ddl([gen.generate_set_logged('votes', 'public')])

# Add PK after data (faster than maintaining during insert)
gen.execute_ddl([gen.generate_primary_key(schema, 'public')])
```

### 4. Validation (`include/pg_migration/validation.py`)

Post-migration row count verification.

```python
from include.pg_migration.validation import validate_migration

results = validate_migration(
    source_conn_id='postgres_source',
    target_conn_id='postgres_target',
    tables=tables_list
)
# Returns: {'passed': 16, 'failed': 0, 'success_rate': 100.0, 'report': '...'}
```

---

## Data Transfer Internals

### Streaming Architecture

```
Source DB                    Airflow Worker                    Target DB
    │                              │                               │
    │   COPY TO STDOUT (CSV)       │                               │
    │ ◄────────────────────────────┤                               │
    │                              │                               │
    │      Chunk Buffer            │      COPY FROM STDIN          │
    │      (in-memory)             ├──────────────────────────────►│
    │                              │                               │
    │   Next chunk via keyset      │                               │
    │ ◄────────────────────────────┤                               │
    │                              │                               │
```

**Memory Efficiency**:
- `_CSVRowStream` class: Lazy generator, buffers one chunk at a time
- No `fetchall()` or pandas DataFrames
- Constant memory regardless of table size

### Type Normalization

Handled during COPY streaming (`_normalize_value()`):

| Source Type | Handling |
|-------------|----------|
| `None` | `\N` (COPY NULL marker) |
| `datetime` | ISO format string |
| `Decimal` | String representation |
| `bool` | `t` / `f` |
| `bytes` | UTF-8 decode or NULL |
| `dict/list` (JSON) | `json.dumps()` |
| `float('nan')`, `float('inf')` | NULL |

### Connection Pooling

```python
# Shared pool per connection ID
ThreadedConnectionPool(
    minconn=int(os.getenv('PG_POOL_MINCONN', '1')),
    maxconn=int(os.getenv('PG_POOL_MAXCONN', '8')),
    dsn=connection_uri
)
```

Session-level optimizations applied per connection:
```sql
SET maintenance_work_mem = '256MB';
SET work_mem = '128MB';
SET synchronous_commit = off;
```

---

## Partitioning Strategy

### When Partitioning Triggers

Tables exceeding `PARTITION_THRESHOLD` (default: 1M rows) are split into parallel tasks.

### Partition Calculation

Uses NTILE window function for balanced distribution:

```sql
WITH boundaries AS (
    SELECT
        NTILE(8) OVER (ORDER BY id) as partition_num,
        id
    FROM public.votes
)
SELECT
    partition_num,
    MIN(id) as min_pk,
    MAX(id) as max_pk,
    COUNT(*) as row_count
FROM boundaries
GROUP BY partition_num
```

**Output** (example for 10M row table):
| Partition | Min PK | Max PK | Rows | WHERE Clause |
|-----------|--------|--------|------|--------------|
| 1 | 1 | 1,250,000 | 1.25M | `id >= 1 AND id <= 1250000` |
| 2 | 1,250,001 | 2,500,000 | 1.25M | `id > 1250000 AND id <= 2500000` |
| ... | ... | ... | ... | ... |
| 8 | 8,750,001 | 10,000,000 | 1.25M | `id > 8750000 AND id <= 10000000` |

**Dynamic Partition Count**:
```python
if row_count >= 10_000_000:
    partitions = min(8, max_partitions)
elif row_count >= 5_000_000:
    partitions = min(6, max_partitions)
elif row_count >= 2_000_000:
    partitions = min(4, max_partitions)
else:
    partitions = 2
```

### UUID/Non-Numeric PK Handling

Falls back to ROW_NUMBER with TEXT casting:

```sql
WITH numbered AS (
    SELECT uuid_pk::TEXT as pk_text,
           ROW_NUMBER() OVER (ORDER BY uuid_pk) as rn
    FROM table_name
)
SELECT MIN(pk_text), MAX(pk_text)
FROM numbered
WHERE rn BETWEEN (partition_num - 1) * chunk_size + 1 AND partition_num * chunk_size
```

---

## Performance Optimizations

### 1. UNLOGGED Tables

```sql
CREATE UNLOGGED TABLE target_schema.votes (...);
-- Load data (no WAL writes)
ALTER TABLE target_schema.votes SET LOGGED;
```

**Impact**: 10-20% faster bulk loads

### 2. Deferred Constraints

```
1. CREATE TABLE (no PK, no indexes)
2. COPY data (bulk load)
3. ALTER TABLE ADD PRIMARY KEY (index built once)
4. CREATE INDEX (if needed)
```

**Why**: Building index once on sorted data is faster than maintaining B-tree during inserts.

### 3. Target Session Tuning

Applied via docker-compose PostgreSQL command:

```yaml
command:
  - "-c"
  - "synchronous_commit=off"      # Don't wait for WAL flush
  - "-c"
  - "maintenance_work_mem=256MB"  # Faster index creation
  - "-c"
  - "max_wal_size=4GB"            # Reduce checkpoint frequency
```

### 4. Keyset vs OFFSET Performance

| Table Size | OFFSET Page 1000 | Keyset Page 1000 |
|------------|------------------|------------------|
| 1M rows | ~500ms | ~5ms |
| 10M rows | ~5s | ~5ms |
| 100M rows | ~50s | ~5ms |

---

## Configuration Reference

### Environment Variables

```bash
# Database Connections
AIRFLOW_CONN_POSTGRES_SOURCE=postgresql://user:pass@host:5432/db
AIRFLOW_CONN_POSTGRES_TARGET=postgresql://user:pass@host:5432/db

# Transfer Settings
CHUNK_SIZE=100000              # Rows per batch
PARTITION_THRESHOLD=1000000    # Min rows to trigger partitioning
MAX_PARTITIONS=8               # Max parallel partitions per table

# Connection Pool
PG_POOL_MINCONN=1
PG_POOL_MAXCONN=8

# Airflow Parallelism
AIRFLOW__CORE__PARALLELISM=16
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=8
```

### Runtime Parameters

Override defaults when triggering DAG:

```bash
airflow dags trigger postgres_to_postgres_migration --conf '{
  "source_schema": "raw",
  "target_schema": "staging",
  "chunk_size": 50000,
  "partition_threshold": 500000,
  "max_partitions": 16,
  "exclude_tables": ["temp_*", "audit_*"],
  "use_unlogged_tables": true,
  "drop_existing_tables": false
}'
```

---

## SQL Injection Prevention

All dynamic SQL uses `psycopg2.sql` module:

```python
from psycopg2 import sql

# Safe identifier quoting
query = sql.SQL('SELECT {columns} FROM {schema}.{table}').format(
    columns=sql.SQL(', ').join([sql.Identifier(c) for c in columns]),
    schema=sql.Identifier(schema_name),
    table=sql.Identifier(table_name)
)

# Safe literal values in WHERE clauses
where = sql.SQL('{col} > {val}').format(
    col=sql.Identifier(pk_column),
    val=sql.Literal(last_value)
)
```

---

## Failure Handling

### Chunk-Level Resumability

Each chunk is idempotent:
1. Target table truncated before first chunk (or partition range)
2. Failed chunk can be retried without data duplication
3. Progress logged per chunk for debugging

### Partition Isolation

Each partition runs as independent Airflow task:
- Failed partition doesn't affect others
- Can be retried individually via Airflow UI
- Parallel execution continues for healthy partitions

### Validation DAG

Separate `validate_migration_env` DAG:
- Triggered automatically after main DAG
- Compares exact row counts source vs target
- Reports pass/fail per table
- Can be run independently for verification

---

## Operational Runbook

### Start Environment

```bash
docker-compose up -d
# Wait for healthy: http://localhost:8080
```

### Trigger Migration

```bash
# Default settings
docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration

# Custom settings
docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration \
  --conf '{"source_schema": "production", "chunk_size": 200000}'
```

### Monitor Progress

```bash
# DAG status
docker exec airflow-scheduler airflow dags list-runs -d postgres_to_postgres_migration

# Task logs
docker exec airflow-scheduler airflow tasks logs postgres_to_postgres_migration \
  transfer_table_data <execution_date> --map-index 0

# Target row counts
docker exec postgres-target psql -U postgres -d target_db \
  -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC"
```

### Validate Results

```bash
# Trigger validation independently
docker exec airflow-scheduler airflow dags trigger validate_migration_env

# Manual count comparison
docker exec postgres-source psql -U postgres -d source_db \
  -c "SELECT COUNT(*) FROM public.votes"
docker exec postgres-target psql -U postgres -d target_db \
  -c "SELECT COUNT(*) FROM public.votes"
```

---

## Limitations

| Limitation | Workaround |
|------------|------------|
| Full-refresh only | Use CDC tools (Debezium) for incremental |
| No cross-database type conversion | Both must be PostgreSQL |
| UNLOGGED tables not crash-safe | Convert to LOGGED immediately after load |
| Foreign keys deferred | Applied after all tables loaded |
| Large single transactions | Chunking prevents long-running txns |

---

## Extension Points

### Adding New Source Types

1. Create new extractor in `include/pg_migration/`
2. Implement `get_tables()`, `get_columns()`, `get_primary_key()` interfaces
3. Add type mapping in `type_mapping.py`
4. Update DAG to use new extractor

### Custom Transformations

Modify `_normalize_value()` in `data_transfer.py`:

```python
def _normalize_value(self, value, column_name, column_type):
    # Add custom transformation logic
    if column_name == 'email':
        return value.lower() if value else None
    return self._default_normalize(value)
```

### Alternative Notification Channels

Extend `notifications.py`:

```python
def send_pagerduty_notification(context, stats):
    # Implement PagerDuty integration
    pass
```

---

## Project Structure

```
postgres-to-postgres-pipeline/
├── dags/
│   ├── postgres_to_postgres_migration.py  # Main DAG (751 lines)
│   └── validate_migration_env.py          # Validation DAG (262 lines)
├── include/pg_migration/
│   ├── schema_extractor.py    # Schema discovery (427 lines)
│   ├── ddl_generator.py       # DDL generation (490 lines)
│   ├── data_transfer.py       # Bulk transfer (750 lines)
│   ├── type_mapping.py        # Type mapping (301 lines)
│   ├── validation.py          # Validation (391 lines)
│   └── notifications.py       # Alerts (1,031 lines)
├── tests/dags/
│   └── test_dag_example.py    # DAG validation tests
├── docs/                       # Documentation
├── docker-compose.yml          # Service orchestration
├── Dockerfile                  # Airflow 3.0.2 image
└── requirements.txt            # Python dependencies
```

---

## Version Compatibility

| Component | Version |
|-----------|---------|
| Apache Airflow | 3.0.2 |
| PostgreSQL | 12+ (tested with 16) |
| Python | 3.12 |
| psycopg2 | 2.9+ |
