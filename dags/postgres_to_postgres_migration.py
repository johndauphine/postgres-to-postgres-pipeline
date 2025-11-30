"""
PostgreSQL to PostgreSQL Migration DAG

This DAG performs a complete schema and data migration from PostgreSQL source to PostgreSQL target.
It handles:
1. Schema extraction from PostgreSQL source
2. Table creation in PostgreSQL target
3. Data transfer with chunking and parallelization
4. Primary key creation and validation

The DAG is designed for data warehouse use cases where only primary keys are needed (no foreign keys).
"""

from airflow.sdk import Asset, dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any
import logging
import re

# Import our custom migration modules
from include.mssql_pg_migration import (
    schema_extractor,
    ddl_generator,
    data_transfer,
)

logger = logging.getLogger(__name__)


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.
    """
    if not identifier:
        raise ValueError(f"Invalid {identifier_type}: cannot be empty")

    if len(identifier) > 128:
        raise ValueError(f"Invalid {identifier_type}: exceeds maximum length of 128 characters")

    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid {identifier_type} '{identifier}': must start with letter or underscore "
            "and contain only alphanumeric characters and underscores"
        )

    return identifier


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
        "max_retry_delay": timedelta(minutes=30),
    },
    params={
        "source_conn_id": Param(
            default="postgres_source",
            type="string",
            description="PostgreSQL source connection ID"
        ),
        "target_conn_id": Param(
            default="postgres_target",
            type="string",
            description="PostgreSQL target connection ID"
        ),
        "source_schema": Param(
            default="public",
            type="string",
            description="Source schema in PostgreSQL source"
        ),
        "target_schema": Param(
            default="public",
            type="string",
            description="Target schema in PostgreSQL target"
        ),
        "chunk_size": Param(
            default=100000,
            type="integer",
            minimum=100,
            maximum=500000,
            description="Number of rows to transfer per batch"
        ),
        "exclude_tables": Param(
            default=[],
            type="array",
            description="List of table patterns to exclude (supports wildcards)"
        ),
        "use_unlogged_tables": Param(
            default=True,
            type="boolean",
            description="Create tables as UNLOGGED during load for faster bulk inserts (converts to LOGGED after)"
        ),
    },
    tags=["migration", "postgres", "etl", "full-refresh"],
)
def postgres_to_postgres_migration():
    """
    Main DAG for PostgreSQL to PostgreSQL migration.
    """

    @task(outlets=[Asset("postgres_schema_extracted")])
    def extract_source_schema(**context) -> List[Dict[str, Any]]:
        """Extract complete schema information from PostgreSQL source."""
        params = context["params"]
        logger.info(f"Extracting schema from {params['source_schema']} in PostgreSQL source")

        tables = schema_extractor.extract_schema_info(
            postgres_conn_id=params["source_conn_id"],
            schema_name=params["source_schema"],
            exclude_tables=params.get("exclude_tables", [])
        )

        logger.info(f"Extracted schema for {len(tables)} tables")

        context["ti"].xcom_push(key="extracted_tables", value=[t["table_name"] for t in tables])
        context["ti"].xcom_push(key="total_row_count", value=sum(t.get("row_count", 0) for t in tables))

        return tables

    @task
    def create_target_schema(schema_name: str, **context) -> str:
        """Create target schema in PostgreSQL if it doesn't exist."""
        params = context["params"]
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])
        postgres_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        logger.info(f"Ensured schema {schema_name} exists in PostgreSQL target")
        return f"Schema {schema_name} ready"

    @task
    def create_target_tables(
        tables_schema: List[Dict[str, Any]],
        schema_status: str,
        **context
    ) -> List[Dict[str, Any]]:
        """
        Create or truncate tables in PostgreSQL target.

        For existing tables: truncate data (preserve structure)
        For new tables: create with proper data types
        """
        params = context["params"]
        target_schema = params["target_schema"]
        use_unlogged = params.get("use_unlogged_tables", True)

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])
        prepared_tables = []

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]

            try:
                if generator.table_exists(table_name, target_schema):
                    logger.info(f"Truncating existing table {target_schema}.{table_name}")
                    truncate_stmt = generator.generate_truncate_table(table_name, target_schema, cascade=False)
                    generator.execute_ddl([truncate_stmt], transaction=False)
                    logger.info(f"✓ Truncated table {table_name}")
                else:
                    unlogged_msg = " (UNLOGGED)" if use_unlogged else ""
                    logger.info(f"Creating new table {target_schema}.{table_name}{unlogged_msg}")

                    ddl_statements = [generator.generate_create_table(
                        table_schema,
                        target_schema,
                        include_constraints=False,
                        unlogged=use_unlogged
                    )]

                    generator.execute_ddl(ddl_statements, transaction=False)
                    logger.info(f"✓ Created table {table_name}")

                table_info = {
                    "table_name": table_name,
                    "source_schema": params["source_schema"],
                    "target_schema": target_schema,
                    "target_table": table_name,
                    "row_count": table_schema.get("row_count", 0),
                    "columns": [col["column_name"] for col in table_schema["columns"]],
                }
                prepared_tables.append(table_info)

            except Exception as e:
                logger.error(f"✗ Failed to prepare table {table_name}: {str(e)}")
                raise

        logger.info(f"Successfully prepared {len(prepared_tables)} tables for data transfer")
        return prepared_tables

    # Threshold for partitioning large tables (rows)
    LARGE_TABLE_THRESHOLD = 5_000_000
    PARTITION_COUNT = 4

    @task
    def prepare_regular_tables(created_tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """Filter out tables that are small enough to transfer without partitioning."""
        regular_tables = []

        for table_info in created_tables:
            row_count = table_info.get('row_count', 0)
            if row_count < LARGE_TABLE_THRESHOLD:
                regular_tables.append(table_info)
            else:
                logger.info(f"Table {table_info['table_name']} ({row_count:,} rows) will be partitioned")

        logger.info(f"Prepared {len(regular_tables)} regular tables for transfer")
        return regular_tables

    @task
    def prepare_large_table_partitions(created_tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """Create partitions for large tables (>5M rows) using primary key ranges."""
        params = context["params"]
        partitions = []

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        source_hook = PostgresHook(postgres_conn_id=params["source_conn_id"])

        for table_info in created_tables:
            row_count = table_info.get('row_count', 0)

            if row_count < LARGE_TABLE_THRESHOLD:
                continue

            table_name = table_info['table_name']
            source_schema = table_info.get('source_schema', params.get('source_schema', 'public'))

            try:
                safe_table_name = validate_sql_identifier(table_name, "table name")
                safe_source_schema = validate_sql_identifier(source_schema, "schema name")
            except ValueError as e:
                logger.error(f"Invalid SQL identifier: {e}")
                continue

            pk_column = table_info.get('primary_key')
            if not pk_column:
                pk_query = """
                    SELECT a.attname
                    FROM pg_catalog.pg_constraint con
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
                    JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s AND con.contype = 'p'
                    ORDER BY array_position(con.conkey, a.attnum)
                    LIMIT 1
                """
                pk_result = source_hook.get_first(pk_query, parameters=[safe_source_schema, safe_table_name])
                pk_column = pk_result[0] if pk_result else 'id'

            try:
                safe_pk_column = validate_sql_identifier(pk_column, "primary key column")
            except ValueError as e:
                logger.error(f"Invalid primary key column for table {safe_table_name}: {e}")
                continue

            logger.info(f"Partitioning {safe_table_name} by \"{safe_pk_column}\" ({row_count:,} rows)")

            range_query = f'SELECT MIN("{safe_pk_column}"), MAX("{safe_pk_column}") FROM {safe_source_schema}."{safe_table_name}"'
            min_max = source_hook.get_first(range_query)

            if not min_max or min_max[0] is None:
                logger.warning(f"Could not get PK range for {safe_table_name}, skipping partitioning")
                continue

            min_id, max_id = min_max[0], min_max[1]

            if not isinstance(min_id, int) or not isinstance(max_id, int):
                logger.error(f"Primary key range for {safe_table_name} must be integer")
                continue

            if max_id < min_id:
                logger.warning(f"Invalid PK range for {safe_table_name}, skipping partitioning")
                continue

            id_range = max_id - min_id + 1
            chunk_size = id_range // PARTITION_COUNT

            logger.info(f"  PK range: {min_id:,} to {max_id:,} (chunk size: {chunk_size:,})")

            for i in range(PARTITION_COUNT):
                start_id = min_id + (i * chunk_size)

                if i == PARTITION_COUNT - 1:
                    where_clause = f'"{safe_pk_column}" >= {start_id}'
                else:
                    end_id = min_id + ((i + 1) * chunk_size) - 1
                    where_clause = f'"{safe_pk_column}" >= {start_id} AND "{safe_pk_column}" <= {end_id}'

                partition_info = {
                    **table_info,
                    'partition_name': f'partition_{i + 1}',
                    'partition_index': i,
                    'where_clause': where_clause,
                    'pk_column': safe_pk_column,
                    'estimated_rows': row_count // PARTITION_COUNT,
                    'truncate_first': i == 0
                }
                partitions.append(partition_info)

            logger.info(f"  Created {PARTITION_COUNT} partitions for {safe_table_name}")

        logger.info(f"Total: {len(partitions)} partitions")
        return partitions

    @task
    def transfer_table_data(table_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Transfer data for a single table from source to target."""
        params = context["params"]

        logger.info(f"Starting data transfer for {table_info['table_name']} ({table_info.get('row_count', 0):,} rows)")

        result = data_transfer.transfer_table_data(
            source_conn_id=params["source_conn_id"],
            target_conn_id=params["target_conn_id"],
            table_info=table_info,
            chunk_size=params["chunk_size"],
            truncate=False
        )

        result["table_name"] = table_info["table_name"]

        if result["success"]:
            logger.info(
                f"✓ {table_info['table_name']}: Transferred {result['rows_transferred']:,} rows "
                f"in {result['elapsed_time_seconds']:.2f}s "
                f"({result['avg_rows_per_second']:,.0f} rows/sec)"
            )
        else:
            logger.error(f"✗ {table_info['table_name']}: Transfer failed. Errors: {result.get('errors', [])}")

        return result

    @task
    def transfer_partition(partition_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Transfer a partition of a large table in parallel."""
        params = context["params"]
        table_name = partition_info['table_name']
        partition_name = partition_info['partition_name']

        logger.info(f"Starting {table_name} {partition_name} transfer (estimated {partition_info.get('estimated_rows', 0):,} rows)")

        result = data_transfer.transfer_table_data(
            source_conn_id=params["source_conn_id"],
            target_conn_id=params["target_conn_id"],
            table_info=partition_info,
            chunk_size=params["chunk_size"],
            truncate=partition_info.get('truncate_first', False),
            where_clause=partition_info.get('where_clause')
        )

        result["table_name"] = table_name
        result["partition_name"] = partition_name
        result["is_partition"] = True

        if result["success"]:
            logger.info(
                f"✓ {table_name} {partition_name}: Transferred {result['rows_transferred']:,} rows "
                f"in {result['elapsed_time_seconds']:.2f}s"
            )
        else:
            logger.error(f"✗ {table_name} {partition_name}: Transfer failed.")

        return result

    @task
    def convert_tables_to_logged(transfer_results: List[Dict[str, Any]], **context) -> str:
        """Convert UNLOGGED tables to LOGGED after data transfer."""
        params = context["params"]

        if not params.get("use_unlogged_tables", True):
            logger.info("Tables were created as LOGGED, no conversion needed")
            return "Tables already logged"

        target_schema = params["target_schema"]
        generator = ddl_generator.DDLGenerator(params["target_conn_id"])

        successful_tables = [r["table_name"] for r in transfer_results if r.get("success", False)]
        converted_count = 0

        for table_name in successful_tables:
            try:
                set_logged_ddl = generator.generate_set_logged(table_name, target_schema)
                generator.execute_ddl([set_logged_ddl], transaction=False)
                converted_count += 1
                logger.info(f"✓ Converted {table_name} to LOGGED")
            except Exception as e:
                logger.warning(f"Could not convert {table_name} to LOGGED: {str(e)}")

        logger.info(f"Converted {converted_count} tables to LOGGED")
        return f"Converted {converted_count} tables to LOGGED"

    @task
    def create_primary_keys(
        tables_schema: List[Dict[str, Any]],
        transfer_results: List[Dict[str, Any]],
        **context
    ) -> str:
        """Create primary key constraints after data transfer."""
        params = context["params"]
        target_schema = params["target_schema"]

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])

        successful_tables = {r["table_name"] for r in transfer_results if r.get("success", False)}
        pk_count = 0

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]
            if table_name not in successful_tables:
                continue

            pk_ddl = generator.generate_primary_key(table_schema, target_schema)
            if pk_ddl:
                try:
                    generator.execute_ddl([pk_ddl], transaction=False)
                    pk_count += 1
                    logger.info(f"✓ Created primary key for {table_name}")
                except Exception as e:
                    logger.warning(f"Could not create primary key for {table_name}: {str(e)}")

        logger.info(f"Created {pk_count} primary key constraints")
        return f"Created {pk_count} primary keys"

    # Define the task flow
    schema_data = extract_source_schema()
    schema_status = create_target_schema(schema_name="{{ params.target_schema }}")
    created_tables = create_target_tables(schema_data, schema_status)

    # Prepare transfer tasks
    regular_tables = prepare_regular_tables(created_tables)
    large_table_partitions = prepare_large_table_partitions(created_tables)

    @task
    def split_partitions(partitions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Split partitions into first (truncate) and remaining (no truncate)."""
        first_partitions = []
        remaining_partitions = []

        for partition in partitions:
            if partition.get('truncate_first', False):
                first_partitions.append(partition)
            else:
                remaining_partitions.append(partition)

        return {'first': first_partitions, 'remaining': remaining_partitions}

    partition_groups = split_partitions(large_table_partitions)

    # Transfer regular tables in parallel
    regular_transfer_results = transfer_table_data.expand(table_info=regular_tables)

    @task
    def get_first_partitions(groups: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return groups.get('first', [])

    @task
    def get_remaining_partitions(groups: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return groups.get('remaining', [])

    first_partitions_list = get_first_partitions(partition_groups)
    remaining_partitions_list = get_remaining_partitions(partition_groups)

    # Transfer partitions
    first_partition_results = transfer_partition.expand(partition_info=first_partitions_list)
    remaining_partition_results = transfer_partition.expand(partition_info=remaining_partitions_list)
    first_partition_results >> remaining_partition_results

    # Collect all transfer results
    @task(trigger_rule="all_done")
    def collect_all_results(**context) -> List[Dict[str, Any]]:
        """Collect and aggregate results from all transfer tasks."""
        ti = context['ti']
        all_results = []

        try:
            regular = ti.xcom_pull(task_ids='transfer_table_data', map_indexes=None)
            if regular:
                if isinstance(regular, list):
                    all_results.extend([r for r in regular if r])
                else:
                    all_results.append(regular)
        except Exception as e:
            logger.warning(f"Failed to retrieve regular table results: {e}")

        from collections import defaultdict
        table_partitions = defaultdict(list)

        try:
            partitions = ti.xcom_pull(task_ids='transfer_partition', map_indexes=None)
            if partitions:
                if not isinstance(partitions, list):
                    partitions = [partitions]
                for p in partitions:
                    if p:
                        table_partitions[p.get('table_name', 'Unknown')].append(p)
        except:
            pass

        for table_name, parts in table_partitions.items():
            total_rows = sum(p.get('rows_transferred', 0) for p in parts)
            success = all(p.get('success', False) for p in parts)
            all_results.append({
                'table_name': table_name,
                'rows_transferred': total_rows,
                'success': success,
                'partitions_processed': len(parts)
            })
            logger.info(f"Aggregated {len(parts)} partitions for {table_name}: {total_rows:,} total rows")

        logger.info(f"Collected results for {len(all_results)} tables")
        return all_results

    transfer_results = collect_all_results()
    [regular_transfer_results, first_partition_results, remaining_partition_results] >> transfer_results

    # Post-transfer tasks
    logged_status = convert_tables_to_logged(transfer_results)
    pk_status = create_primary_keys(schema_data, transfer_results)
    logged_status >> pk_status

    # Trigger validation DAG
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_validation_dag",
        trigger_dag_id="validate_migration_env",
        wait_for_completion=True,
        poke_interval=30,
        conf={
            "source_schema": "{{ params.source_schema }}",
            "target_schema": "{{ params.target_schema }}",
        },
    )

    pk_status >> trigger_validation

    @task
    def generate_migration_summary(**context):
        """Generate a summary of the migration."""
        logger.info("Migration completed successfully!")
        return "Migration complete"

    final_status = generate_migration_summary()
    trigger_validation >> final_status


# Instantiate the DAG
postgres_to_postgres_migration()
