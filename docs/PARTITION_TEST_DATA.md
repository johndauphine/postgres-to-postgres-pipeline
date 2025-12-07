# Partition Stress Test Data

Use this script to generate large synthetic tables in the source Postgres instance to exercise universal partitioning and keyset pagination.

## Script
- Path: `scripts/generate_partition_test_data.py`
- Tables created in the chosen schema (default `public`):
  - `partition_int_dense` (dense BIGINT PK)
  - `partition_int_sparse` (gapped BIGINT PK, multiples of 10)
  - `partition_uuid_pk` (UUID PK)
- Default row counts: 1.2M dense, 1.6M sparse, 0.9M UUID.
- The script writes directly into the source database; no files are created.

## Run inside Airflow scheduler container (recommended)
```bash
# Copy script into the container if not already present
docker cp scripts/generate_partition_test_data.py airflow-scheduler:/opt/airflow/scripts/

# Generate data in the source DB using container env (AIRFLOW_CONN_POSTGRES_SOURCE)
docker exec airflow-scheduler python /opt/airflow/scripts/generate_partition_test_data.py --drop-existing
```

## Run from host (requires psycopg2 and DB connectivity)
```bash
# Ensure psycopg2 is available in your environment/venv
python scripts/generate_partition_test_data.py \
  --drop-existing \
  --conn postgresql://postgres:PostgresPassword123@localhost:5434/source_db \
  --schema public
```

## After seeding
- Trigger migration: `docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration`
- Validation will run automatically (`validate_migration_env`). You can also spot-check counts in the target DB for the three tables.***
