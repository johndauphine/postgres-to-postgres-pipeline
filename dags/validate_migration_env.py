"""
Migration Validation DAG using Environment Variables

This DAG validates PostgreSQL to PostgreSQL migration by comparing row counts.
It uses environment variables for connection details to avoid hardcoding.

Set these environment variables before running:
- POSTGRES_SOURCE_HOST, POSTGRES_SOURCE_PORT, POSTGRES_SOURCE_DATABASE, POSTGRES_SOURCE_USERNAME, POSTGRES_SOURCE_PASSWORD
- POSTGRES_TARGET_HOST, POSTGRES_TARGET_PORT, POSTGRES_TARGET_DATABASE, POSTGRES_TARGET_USERNAME, POSTGRES_TARGET_PASSWORD

Or use the default test values if not set.
"""

from airflow.sdk import dag, task
from airflow.models.param import Param
from pendulum import datetime
from datetime import timedelta
import logging
import os
import pg8000

logger = logging.getLogger(__name__)


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
        "source_schema": Param(default="public", type="string"),
        "target_schema": Param(default="public", type="string"),
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
        Validate all tables using environment variable connections.
        """
        params = context["params"]

        # Get connection details from environment with defaults for testing
        source_config = {
            'host': os.environ.get('POSTGRES_SOURCE_HOST', 'postgres-source'),
            'port': int(os.environ.get('POSTGRES_SOURCE_PORT', '5432')),
            'database': os.environ.get('POSTGRES_SOURCE_DATABASE', 'source_db'),
            'user': os.environ.get('POSTGRES_SOURCE_USERNAME', 'postgres'),
            'password': os.environ.get('POSTGRES_SOURCE_PASSWORD', 'PostgresPassword123'),
        }

        target_config = {
            'host': os.environ.get('POSTGRES_TARGET_HOST', 'postgres-target'),
            'port': int(os.environ.get('POSTGRES_TARGET_PORT', '5432')),
            'database': os.environ.get('POSTGRES_TARGET_DATABASE', 'target_db'),
            'user': os.environ.get('POSTGRES_TARGET_USERNAME', 'postgres'),
            'password': os.environ.get('POSTGRES_TARGET_PASSWORD', 'PostgresPassword123'),
        }

        source_schema = params["source_schema"]
        target_schema = params["target_schema"]
        exclude_tables = params.get("exclude_tables", [])

        # Connect to databases
        logger.info("Connecting to databases...")

        try:
            source_conn = pg8000.connect(**source_config)
            source_cursor = source_conn.cursor()
            logger.info(f"✓ Connected to PostgreSQL source: {source_config['host']}")
        except Exception as e:
            logger.error(f"PostgreSQL source connection failed: {e}")
            return f"PostgreSQL source connection failed: {e}"

        try:
            target_conn = pg8000.connect(**target_config)
            target_cursor = target_conn.cursor()
            logger.info(f"✓ Connected to PostgreSQL target: {target_config['host']}")
        except Exception as e:
            logger.error(f"PostgreSQL target connection failed: {e}")
            source_conn.close()
            return f"PostgreSQL target connection failed: {e}"

        # Discover tables from source
        logger.info(f"Discovering tables in {source_schema}...")

        discovery_query = """
            SELECT
                t.table_name,
                COALESCE(s.n_live_tup, 0) AS row_count
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                ON s.schemaname = t.table_schema
                AND s.relname = t.table_name
            WHERE t.table_schema = %s
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """

        try:
            source_cursor.execute(discovery_query, (source_schema,))
            source_results = source_cursor.fetchall()
            source_counts = {
                row[0]: row[1]
                for row in source_results
                if row[0] not in exclude_tables
            }
        except Exception as e:
            logger.error(f"Failed to query source: {e}")
            source_conn.close()
            target_conn.close()
            return f"Failed to query source: {e}"

        # Get target counts
        logger.info(f"Validating {len(source_counts)} tables...")

        results = []
        passed = 0
        failed = 0
        missing = 0

        for table_name in sorted(source_counts.keys()):
            source_count = source_counts[table_name]

            # Query target
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
        total = len(source_counts)
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
