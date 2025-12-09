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

from airflow.decorators import dag, task
from airflow.sdk.definitions.asset import Asset
from airflow.models.param import Param
from airflow.models.xcom_arg import XCOM_RETURN_KEY
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any
import logging
import os

# Import our custom migration modules
from include.pg_migration import (
    schema_extractor,
    ddl_generator,
    data_transfer,
)
from include.pg_migration.notifications import (
    on_task_failure,
    send_success_notification,
    capture_exceptions,
)
from include.pg_migration.utils import validate_sql_identifier, quote_sql_literal
from include.pg_migration.partitioning import get_partition_count, prepare_partitions

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=64,  # Allow up to 64 concurrent tasks within this DAG
    is_paused_upon_creation=False,
    doc_md=__doc__,
    # Note: DAG-level callbacks don't work reliably in Airflow 3.0
    # Success notifications are sent from generate_migration_summary task instead
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
        "max_retry_delay": timedelta(minutes=30),
        "pool": "default_pool",  # Use default pool for all tasks
        "on_failure_callback": on_task_failure,
    },
    params={
        "source_conn_id": Param(
            default=os.environ.get('SOURCE_CONN_ID', 'postgres_source'),
            type="string",
            description="PostgreSQL source connection ID (env: SOURCE_CONN_ID)"
        ),
        "target_conn_id": Param(
            default=os.environ.get('TARGET_CONN_ID', 'postgres_target'),
            type="string",
            description="PostgreSQL target connection ID (env: TARGET_CONN_ID)"
        ),
        "source_schema": Param(
            default=os.environ.get('SOURCE_SCHEMA', 'public'),
            type="string",
            description="Source schema in PostgreSQL source (env: SOURCE_SCHEMA)"
        ),
        "target_schema": Param(
            default=os.environ.get('TARGET_SCHEMA', 'public'),
            type="string",
            description="Target schema in PostgreSQL target (env: TARGET_SCHEMA)"
        ),
        "chunk_size": Param(
            default=int(os.environ.get('CHUNK_SIZE', '100000')),
            type="integer",
            minimum=100,
            maximum=500000,
            description="Number of rows to transfer per batch (env: CHUNK_SIZE)"
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
        "drop_existing_tables": Param(
            default=False,
            type="boolean",
            description="Drop and recreate existing tables instead of truncating. Use when source schema has changed."
        ),
        "partition_threshold": Param(
            default=int(os.environ.get('PARTITION_THRESHOLD', '1000000')),
            type="integer",
            minimum=100_000,
            description="Row count threshold for automatic table partitioning (env: PARTITION_THRESHOLD)"
        ),
        "max_partitions": Param(
            default=int(os.environ.get('MAX_PARTITIONS', '8')),
            type="integer",
            minimum=2,
            maximum=16,
            description="Maximum partitions per large table (env: MAX_PARTITIONS)"
        ),
    },
    tags=["migration", "postgres", "etl", "full-refresh"],
)
def postgres_to_postgres_migration():
    """
    Main DAG for PostgreSQL to PostgreSQL migration.
    """

    @task(outlets=[Asset("postgres_schema_extracted")])
    @capture_exceptions
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
    @capture_exceptions
    def create_target_schema(schema_name: str, **context) -> str:
        """Create target schema in PostgreSQL if it doesn't exist."""
        params = context["params"]
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id=params["target_conn_id"])
        postgres_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        logger.info(f"Ensured schema {schema_name} exists in PostgreSQL target")
        return f"Schema {schema_name} ready"

    @task
    @capture_exceptions
    def create_target_tables(
        tables_schema: List[Dict[str, Any]],
        schema_status: str,
        **context
    ) -> List[Dict[str, Any]]:
        """
        Create or truncate tables in PostgreSQL target.

        For existing tables:
          - If drop_existing_tables=True: drop and recreate (use when schema changed)
          - If drop_existing_tables=False: truncate data (preserve structure)
        For new tables: create with proper data types
        """
        params = context["params"]
        target_schema = params["target_schema"]
        use_unlogged = params.get("use_unlogged_tables", True)
        drop_existing = params.get("drop_existing_tables", False)

        generator = ddl_generator.DDLGenerator(params["target_conn_id"])
        prepared_tables = []

        for table_schema in tables_schema:
            table_name = table_schema["table_name"]

            try:
                table_exists = generator.table_exists(table_name, target_schema)

                if table_exists and drop_existing:
                    # Drop and recreate when schema may have changed
                    logger.info(f"Dropping existing table {target_schema}.{table_name}")
                    drop_stmt = f'DROP TABLE IF EXISTS {target_schema}."{table_name}" CASCADE'
                    generator.execute_ddl([drop_stmt], transaction=False)
                    logger.info(f"✓ Dropped table {table_name}")
                    table_exists = False  # Will be recreated below

                if table_exists:
                    # Truncate existing table (schema unchanged)
                    logger.info(f"Truncating existing table {target_schema}.{table_name}")
                    truncate_stmt = generator.generate_truncate_table(table_name, target_schema, cascade=False)
                    generator.execute_ddl([truncate_stmt], transaction=False)
                    logger.info(f"✓ Truncated table {table_name}")
                else:
                    # Create new table
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

    @task
    def prepare_regular_tables(created_tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """Filter out tables that are small enough to transfer without partitioning."""
        params = context["params"]
        partition_threshold = params.get("partition_threshold", 1_000_000)
        regular_tables = []

        for table_info in created_tables:
            row_count = table_info.get('row_count', 0)
            if row_count < partition_threshold:
                regular_tables.append(table_info)
            else:
                logger.info(f"Table {table_info['table_name']} ({row_count:,} rows) will be partitioned")

        logger.info(f"Prepared {len(regular_tables)} regular tables for transfer")
        return regular_tables

    @task
    def prepare_large_table_partitions(created_tables: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
        """Create partitions for large tables using NTILE-based boundaries.

        Delegates to the partitioning module for the actual partition calculation.
        """
        params = context["params"]
        return prepare_partitions(
            tables_info=created_tables,
            source_conn_id=params["source_conn_id"],
            source_schema=params.get("source_schema", "public"),
            partition_threshold=params.get("partition_threshold", 1_000_000),
            max_partitions=params.get("max_partitions", int(os.environ.get('MAX_PARTITIONS', '8')))
        )

    @task
    @capture_exceptions
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
    @capture_exceptions
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
            truncate=False,  # truncate already done in create_target_tables
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

    # Transfer regular tables in parallel
    regular_transfer_results = transfer_table_data.expand(table_info=regular_tables)

    # Transfer all partitions in parallel (no sequential dependency needed since
    # truncate already happened in create_target_tables)
    partition_transfer_results = transfer_partition.expand(partition_info=large_table_partitions)

    # Collect all transfer results
    @task(trigger_rule="all_done")
    def collect_all_results(
        regular_tables: List[Dict[str, Any]],
        large_table_partitions: List[Dict[str, Any]],
        **context,
    ) -> List[Dict[str, Any]]:
        """Collect and aggregate results from all transfer tasks.

        Fixed: Improved XCom retrieval with proper error logging for Airflow 3.0.2
        dynamic task mapping compatibility.
        """
        ti = context['ti']
        all_results = []

        def _pull_mapped_xcoms(task_id: str, total: int) -> List[Any]:
            """Fetch mapped XCom return values ordered by map index."""
            if total <= 0:
                logger.info(f"No mapped instances found for {task_id}; skipping pull.")
                return []

            map_indexes = list(range(total))
            logger.info(f"{task_id} map indexes detected: {map_indexes}")
            return ti.xcom_pull(task_ids=task_id, map_indexes=map_indexes, key=XCOM_RETURN_KEY)

        # Collect results from regular table transfers
        logger.info("Collecting results from transfer_table_data tasks...")
        try:
            regular = _pull_mapped_xcoms('transfer_table_data', len(regular_tables or []))
            logger.info(f"transfer_table_data XCom pull returned: type={type(regular)}, value={regular}")
            if regular:
                if isinstance(regular, list):
                    all_results.extend([r for r in regular if r])
                elif isinstance(regular, dict):
                    all_results.append(regular)
            logger.info(f"Collected {len(all_results)} regular table results")
        except Exception as e:
            logger.warning(f"Failed to retrieve regular table results: {e}")

        from collections import defaultdict
        table_partitions = defaultdict(list)

        # Collect results from partition transfers
        logger.info("Collecting results from transfer_partition tasks...")
        try:
            partitions = _pull_mapped_xcoms('transfer_partition', len(large_table_partitions or []))
            logger.info(f"transfer_partition XCom pull returned: type={type(partitions)}, value={partitions}")
            if partitions:
                if not isinstance(partitions, list):
                    partitions = [partitions]
                for p in partitions:
                    if p and isinstance(p, dict):
                        table_partitions[p.get('table_name', 'Unknown')].append(p)
                logger.info(f"Collected {len(partitions)} partition results across {len(table_partitions)} tables")
        except Exception as e:
            logger.warning(f"Failed to retrieve partition results: {e}")

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

        logger.info(f"Collected results for {len(all_results)} tables: {[r.get('table_name') for r in all_results]}")
        return all_results

    transfer_results = collect_all_results(regular_tables, large_table_partitions)
    [regular_transfer_results, partition_transfer_results] >> transfer_results

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
        """Generate a summary of the migration and send notification."""
        logger.info("Migration completed successfully!")

        # Send success notification with migration stats
        # Since DAG callbacks don't work reliably in Airflow 3.0, send from final task
        ti = context.get('ti')
        dag_run = context.get('dag_run')

        # Get migration stats from XCom
        tables_list = ti.xcom_pull(task_ids='extract_source_schema', key='extracted_tables') or []
        total_rows = ti.xcom_pull(task_ids='extract_source_schema', key='total_row_count') or 0

        # Calculate duration
        duration_seconds = 0
        if dag_run and dag_run.start_date:
            from datetime import datetime, timezone
            end_time = datetime.now(timezone.utc)
            duration_seconds = (end_time - dag_run.start_date).total_seconds()

        stats = {
            'tables_migrated': len(tables_list),
            'total_rows': total_rows,
            'tables_list': tables_list,
            'rows_per_second': int(total_rows / duration_seconds) if duration_seconds > 0 else 0,
        }

        send_success_notification(
            dag_id=dag_run.dag_id if dag_run else 'postgres_to_postgres_migration',
            run_id=dag_run.run_id if dag_run else 'unknown',
            start_date=dag_run.start_date if dag_run else None,
            duration_seconds=duration_seconds,
            stats=stats,
        )

        return "Migration complete"

    final_status = generate_migration_summary()
    trigger_validation >> final_status


# Instantiate the DAG
postgres_to_postgres_migration()
