# PostgreSQL Migration DAG Task Guide

This summarizes what each task in `dags/postgres_to_postgres_migration.py` does, what it expects, and what it emits. Use it as a quick reference when walking other data engineers through the flow.

## Task Flow at a Glance
```
extract_source_schema
→ create_target_schema
→ create_target_tables
→ prepare_regular_tables + prepare_large_table_partitions
→ transfer_table_data (mapped) + transfer_partition (mapped)
→ collect_all_results
→ convert_tables_to_logged
→ create_primary_keys
→ trigger_validation_dag
→ generate_migration_summary
```

## Task Details
- **extract_source_schema**: Scans the source schema (tables, columns, PKs, indexes) and pushes a table list with row counts into XCom (`extracted_tables`, `total_row_count`). Uses `source_conn_id`, `source_schema`.

- **create_target_schema**: Ensures the target schema exists in Postgres before table creation. Uses `target_conn_id`, `target_schema`.

- **create_target_tables**: Drops or truncates existing target tables (controlled by `drop_existing_tables`), then recreates them (optionally UNLOGGED via `use_unlogged_tables`). Returns table metadata used by downstream tasks.

- **prepare_regular_tables**: Splits tables below `partition_threshold` into a list for single-task transfers. Outputs `regular_tables` (fed to `transfer_table_data.expand`).

- **prepare_large_table_partitions**: For tables at/above `partition_threshold`, computes balanced PK-based partitions (NTILE window). Emits partition dicts with `partition_name`, `where_clause`, and `estimated_rows` for `transfer_partition.expand`.

- **transfer_table_data (mapped)**: Streams a regular table using keyset pagination + COPY. Adjusts `chunk_size` per table, truncates target (already done), and returns transfer stats (row counts, timing, success flag).

- **transfer_partition (mapped)**: Same transfer logic but scoped to a partition `where_clause`; runs in parallel across partitions of a large table.

- **collect_all_results**: Aggregates mapped XCom outputs from both transfer task groups. Builds a single list of per-table results (partition totals rolled up) for downstream consumers.

- **convert_tables_to_logged**: If tables were created UNLOGGED, runs `ALTER TABLE ... SET LOGGED` for every successful transfer result. Skips conversions when `use_unlogged_tables` is false or when a table failed.

- **create_primary_keys**: Adds primary key constraints on successfully transferred tables using the extracted schema metadata.

- **trigger_validation_dag**: Kicks off `validate_migration_env` with `source_schema`/`target_schema` to compare row counts independently. Waits for completion before finishing the DAG.

- **generate_migration_summary**: Final marker task; currently just logs completion and returns a simple status string.
