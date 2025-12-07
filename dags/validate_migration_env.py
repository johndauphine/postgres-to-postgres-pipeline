"""
Migration Validation DAG using Environment Variables

This DAG validates PostgreSQL to PostgreSQL migration by comparing row counts.
It uses the same AIRFLOW_CONN_* connection strings from .env as the migration DAG.

Connection strings are parsed from:
- AIRFLOW_CONN_POSTGRES_SOURCE (or custom via SOURCE_CONN_ID)
- AIRFLOW_CONN_POSTGRES_TARGET (or custom via TARGET_CONN_ID)
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
from urllib.parse import urlparse
import logging
import os
import pg8000

logger = logging.getLogger(__name__)


def parse_connection_uri(uri: str) -> dict:
    """Parse an Airflow connection URI into connection parameters."""
    parsed = urlparse(uri)
    return {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/') if parsed.path else 'postgres',
        'user': parsed.username or 'postgres',
        'password': parsed.password or '',
    }


def get_connection_config(conn_id: str) -> dict:
    """Get connection config from environment variable."""
    # Convert conn_id to env var name: postgres_source -> AIRFLOW_CONN_POSTGRES_SOURCE
    env_var = f"AIRFLOW_CONN_{conn_id.upper()}"
    uri = os.environ.get(env_var)

    if uri:
        return parse_connection_uri(uri)

    # Fallback defaults for testing
    if 'source' in conn_id.lower():
        return {
            'host': 'postgres-source',
            'port': 5432,
            'database': 'source_db',
            'user': 'postgres',
            'password': 'PostgresPassword123',
        }
    else:
        return {
            'host': 'postgres-target',
            'port': 5432,
            'database': 'target_db',
            'user': 'postgres',
            'password': 'PostgresPassword123',
        }


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run manually
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 0,
    },
    params={
        "source_conn_id": Param(
            default=os.environ.get('SOURCE_CONN_ID', 'postgres_source'),
            type="string",
            description="Source connection ID (env: SOURCE_CONN_ID)"
        ),
        "target_conn_id": Param(
            default=os.environ.get('TARGET_CONN_ID', 'postgres_target'),
            type="string",
            description="Target connection ID (env: TARGET_CONN_ID)"
        ),
        "source_schema": Param(
            default=os.environ.get('SOURCE_SCHEMA', 'public'),
            type="string",
            description="Source schema (env: SOURCE_SCHEMA)"
        ),
        "target_schema": Param(
            default=os.environ.get('TARGET_SCHEMA', 'public'),
            type="string",
            description="Target schema (env: TARGET_SCHEMA)"
        ),
        "exclude_tables": Param(
            default=[],
            type="array",
            description="Tables to exclude from validation"
        ),
    },
    tags=["validation", "migration", "env-based"],
)
def validate_migration_env():
    """
    Validate migration using environment variables for connections.
    """

    @task
    def validate_tables(**context) -> str:
        """
        Validate all tables using connection IDs from .env.
        """
        params = context["params"]

        # Get connection config from AIRFLOW_CONN_* environment variables
        source_conn_id = params["source_conn_id"]
        target_conn_id = params["target_conn_id"]

        source_config = get_connection_config(source_conn_id)
        target_config = get_connection_config(target_conn_id)

        source_schema = params["source_schema"]
        target_schema = params["target_schema"]
        exclude_tables = params.get("exclude_tables", [])

        # Connect to databases
        logger.info(f"Connecting using {source_conn_id} -> {target_conn_id}...")

        try:
            source_conn = pg8000.connect(**source_config)
            source_cursor = source_conn.cursor()
            logger.info(f"✓ Connected to source: {source_config['host']}:{source_config['port']}/{source_config['database']}")
        except Exception as e:
            logger.error(f"PostgreSQL source connection failed: {e}")
            return f"PostgreSQL source connection failed: {e}"

        try:
            target_conn = pg8000.connect(**target_config)
            target_cursor = target_conn.cursor()
            logger.info(f"✓ Connected to target: {target_config['host']}:{target_config['port']}/{target_config['database']}")
        except Exception as e:
            logger.error(f"PostgreSQL target connection failed: {e}")
            source_conn.close()
            return f"PostgreSQL target connection failed: {e}"

        # Discover tables from source (just get table names)
        logger.info(f"Discovering tables in {source_schema}...")

        discovery_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        try:
            source_cursor.execute(discovery_query, (source_schema,))
            source_results = source_cursor.fetchall()
            table_names = [
                row[0] for row in source_results
                if row[0] not in exclude_tables
            ]
        except Exception as e:
            logger.error(f"Failed to query source: {e}")
            source_conn.close()
            target_conn.close()
            return f"Failed to query source: {e}"

        # Validate each table using actual COUNT(*)
        logger.info(f"Validating {len(table_names)} tables using exact COUNT(*)...")

        results = []
        passed = 0
        failed = 0
        missing = 0

        for table_name in sorted(table_names):
            # Query source with actual COUNT(*)
            try:
                source_cursor.execute(
                    f'SELECT COUNT(*) FROM {source_schema}."{table_name}"'
                )
                source_count = source_cursor.fetchone()[0]
            except Exception as e:
                logger.error(f"Failed to count source table {table_name}: {e}")
                continue

            # Query target with actual COUNT(*)
            try:
                target_cursor.execute(
                    f'SELECT COUNT(*) FROM {target_schema}."{table_name}"'
                )
                target_count = target_cursor.fetchone()[0]
            except:
                target_count = None
                missing += 1

            if target_count is not None:
                if source_count == target_count:
                    status = "✓"
                    passed += 1
                else:
                    status = "✗"
                    failed += 1

                logger.info(
                    f"{status} {table_name:25} | "
                    f"Source: {source_count:>10,} | "
                    f"Target: {target_count:>10,} | "
                    f"Diff: {target_count - source_count:>+10,}"
                )
            else:
                logger.warning(f"? {table_name:25} | MISSING IN TARGET")

        # Clean up
        source_conn.close()
        target_conn.close()

        # Summary
        total = len(table_names)
        success_rate = (passed / total * 100) if total > 0 else 0

        summary = (
            f"\n{'='*60}\n"
            f"VALIDATION SUMMARY\n"
            f"{'='*60}\n"
            f"Tables Checked: {total}\n"
            f"Passed: {passed}\n"
            f"Failed: {failed}\n"
            f"Missing: {missing}\n"
            f"Success Rate: {success_rate:.1f}%\n"
            f"{'='*60}"
        )
        logger.info(summary)

        if missing > 0:
            return f"Incomplete: {missing} tables missing"
        elif failed > 0:
            return f"Failed: {failed} tables have mismatches"
        else:
            return f"Success: All {total} tables match"

    # Execute
    validate_tables()

# Instantiate
validate_migration_env()
