"""
Partitioning logic for large table transfers.

This module provides functions for splitting large tables into balanced partitions
for parallel data transfer using PostgreSQL's NTILE window function.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.pg_migration.utils import validate_sql_identifier, quote_sql_literal

logger = logging.getLogger(__name__)


def get_partition_count(row_count: int, max_partitions: int = 8) -> int:
    """Determine partition count based on table size.

    Respects MAX_PARTITIONS environment variable for hardware-specific tuning.
    Conservative defaults to minimize I/O contention on Docker Desktop / WSL2.

    Args:
        row_count: Number of rows in the table
        max_partitions: Maximum partitions allowed (from DAG params or env var)

    Returns:
        Partition count scaled to table size, capped at max_partitions
    """
    if row_count >= 10_000_000:  # 10M+ rows
        return max_partitions
    elif row_count >= 5_000_000:  # 5-10M rows
        return min(6, max_partitions)
    elif row_count >= 2_000_000:  # 2-5M rows
        return min(4, max_partitions)
    else:  # 1-2M rows
        return min(2, max_partitions)


def get_pk_column(
    source_hook: PostgresHook,
    schema_name: str,
    table_name: str,
    table_info: Dict[str, Any]
) -> Optional[str]:
    """Get the primary key column for a table.

    Args:
        source_hook: PostgreSQL hook for source database
        schema_name: Schema containing the table
        table_name: Name of the table
        table_info: Dictionary with table metadata, may contain 'primary_key'

    Returns:
        Primary key column name, or None if not found
    """
    pk_column = table_info.get('primary_key')
    if pk_column:
        return pk_column

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
    pk_result = source_hook.get_first(pk_query, parameters=[schema_name, table_name])
    return pk_result[0] if pk_result else 'id'


def get_pk_data_type(
    source_hook: PostgresHook,
    schema_name: str,
    table_name: str,
    pk_column: str
) -> str:
    """Get the data type of the primary key column.

    Args:
        source_hook: PostgreSQL hook for source database
        schema_name: Schema containing the table
        table_name: Name of the table
        pk_column: Primary key column name

    Returns:
        Data type string (lowercase), or 'unknown' if not found
    """
    pk_type_query = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """
    pk_type_result = source_hook.get_first(
        pk_type_query,
        parameters=[schema_name, table_name, pk_column]
    )
    return pk_type_result[0].lower() if pk_type_result else 'unknown'


def calculate_ntile_boundaries(
    source_hook: PostgresHook,
    schema_name: str,
    table_name: str,
    pk_column: str,
    partition_count: int,
    is_uuid_pk: bool
) -> List[Tuple[int, Any, Any, int]]:
    """Calculate partition boundaries using PostgreSQL NTILE function.

    Args:
        source_hook: PostgreSQL hook for source database
        schema_name: Schema containing the table (validated)
        table_name: Name of the table (validated)
        pk_column: Primary key column name (validated)
        partition_count: Number of partitions to create
        is_uuid_pk: Whether the PK is UUID type

    Returns:
        List of tuples: (partition_id, min_pk, max_pk, row_count)
    """
    if is_uuid_pk:
        ntile_query = f"""
            WITH numbered AS (
                SELECT "{pk_column}",
                       NTILE({partition_count}) OVER (ORDER BY "{pk_column}") as partition_id
                FROM {schema_name}."{table_name}"
            )
            SELECT partition_id,
                   MIN("{pk_column}"::TEXT)::UUID as min_pk,
                   MAX("{pk_column}"::TEXT)::UUID as max_pk,
                   COUNT(*) as row_count
            FROM numbered
            GROUP BY partition_id
            ORDER BY partition_id
        """
    else:
        ntile_query = f"""
            WITH numbered AS (
                SELECT "{pk_column}",
                       NTILE({partition_count}) OVER (ORDER BY "{pk_column}") as partition_id
                FROM {schema_name}."{table_name}"
            )
            SELECT partition_id,
                   MIN("{pk_column}") as min_pk,
                   MAX("{pk_column}") as max_pk,
                   COUNT(*) as row_count
            FROM numbered
            GROUP BY partition_id
            ORDER BY partition_id
        """

    return source_hook.get_records(ntile_query)


def calculate_row_number_boundaries(
    source_hook: PostgresHook,
    schema_name: str,
    table_name: str,
    pk_column: str,
    partition_count: int,
    row_count: int
) -> List[Tuple[int, Any, Any, int]]:
    """Fallback: Calculate partition boundaries using ROW_NUMBER.

    Used when NTILE query fails (e.g., for certain edge cases).

    Args:
        source_hook: PostgreSQL hook for source database
        schema_name: Schema containing the table (validated)
        table_name: Name of the table (validated)
        pk_column: Primary key column name (validated)
        partition_count: Number of partitions to create
        row_count: Total number of rows in table

    Returns:
        List of tuples: (partition_id, min_pk, max_pk, row_count)
    """
    rows_per_partition = row_count // partition_count
    boundaries = []

    for i in range(partition_count):
        start_row = i * rows_per_partition + 1
        # Last partition takes all remaining rows
        end_row = row_count if i == partition_count - 1 else (i + 1) * rows_per_partition
        partition_rows = end_row - start_row + 1

        # Get boundary PK values using ROW_NUMBER window function
        boundary_query = f"""
            WITH numbered AS (
                SELECT "{pk_column}",
                       ROW_NUMBER() OVER (ORDER BY "{pk_column}") as rn
                FROM {schema_name}."{table_name}"
            )
            SELECT
                (SELECT "{pk_column}" FROM numbered WHERE rn = {start_row}) as min_pk,
                (SELECT "{pk_column}" FROM numbered WHERE rn = {end_row}) as max_pk
        """
        try:
            result = source_hook.get_first(boundary_query)
            if result and result[0] is not None:
                boundaries.append((i + 1, result[0], result[1], partition_rows))
        except Exception as inner_e:
            logger.error(f"Failed to get boundary for partition {i + 1}: {inner_e}")

    return boundaries


def build_partition_configs(
    table_info: Dict[str, Any],
    boundaries: List[Tuple[int, Any, Any, int]],
    pk_column: str
) -> List[Dict[str, Any]]:
    """Build partition configuration dictionaries from boundaries.

    Args:
        table_info: Base table information dictionary
        boundaries: List of (partition_id, min_pk, max_pk, row_count) tuples
        pk_column: Primary key column name (validated)

    Returns:
        List of partition info dictionaries ready for transfer tasks
    """
    partitions = []

    for partition_id, min_pk, max_pk, part_rows in boundaries:
        # Build WHERE clause using actual boundary values
        # Use quote_sql_literal to properly handle UUID/VARCHAR/string PKs
        quoted_min = quote_sql_literal(min_pk)
        quoted_max = quote_sql_literal(max_pk)

        if partition_id == 1:
            # First partition: use <= max to ensure no gaps
            where_clause = f'"{pk_column}" <= {quoted_max}'
        elif partition_id == len(boundaries):
            # Last partition: use > previous max to avoid overlap
            prev_max = boundaries[partition_id - 2][2]  # boundaries is 0-indexed
            quoted_prev_max = quote_sql_literal(prev_max)
            where_clause = f'"{pk_column}" > {quoted_prev_max}'
        else:
            # Middle partitions: use > prev_max AND <= current_max
            prev_max = boundaries[partition_id - 2][2]
            quoted_prev_max = quote_sql_literal(prev_max)
            where_clause = f'"{pk_column}" > {quoted_prev_max} AND "{pk_column}" <= {quoted_max}'

        partition_info = {
            **table_info,
            'partition_name': f'partition_{partition_id}',
            'partition_index': partition_id - 1,
            'where_clause': where_clause,
            'pk_column': pk_column,
            'estimated_rows': part_rows,
        }
        partitions.append(partition_info)

    return partitions


def prepare_partitions(
    tables_info: List[Dict[str, Any]],
    source_conn_id: str,
    source_schema: str,
    partition_threshold: int,
    max_partitions: int
) -> List[Dict[str, Any]]:
    """Prepare partition configurations for all large tables.

    Main entry point that orchestrates the partitioning process.

    Args:
        tables_info: List of table info dictionaries from create_tables task
        source_conn_id: Airflow connection ID for source PostgreSQL
        source_schema: Default source schema name
        partition_threshold: Row count threshold for partitioning
        max_partitions: Maximum number of partitions per table

    Returns:
        List of partition configurations ready for parallel transfer
    """
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    partitions = []

    for table_info in tables_info:
        row_count = table_info.get('row_count', 0)

        if row_count < partition_threshold:
            continue

        table_name = table_info['table_name']
        schema_name = table_info.get('source_schema', source_schema)

        try:
            safe_table_name = validate_sql_identifier(table_name, "table name")
            safe_source_schema = validate_sql_identifier(schema_name, "schema name")
        except ValueError as e:
            logger.error(f"Invalid SQL identifier: {e}")
            continue

        # Get primary key column
        pk_column = get_pk_column(source_hook, safe_source_schema, safe_table_name, table_info)
        if not pk_column:
            logger.warning(f"No primary key found for {safe_table_name}")
            continue

        try:
            safe_pk_column = validate_sql_identifier(pk_column, "primary key column")
        except ValueError as e:
            logger.error(f"Invalid primary key column for table {safe_table_name}: {e}")
            continue

        # Get PK data type
        pk_data_type = get_pk_data_type(source_hook, safe_source_schema, safe_table_name, pk_column)
        is_uuid_pk = pk_data_type == 'uuid'

        # Calculate partition count
        partition_count = get_partition_count(row_count, max_partitions)

        logger.info(
            f"Partitioning {safe_table_name} by \"{safe_pk_column}\" "
            f"({row_count:,} rows, {partition_count} partitions, pk_type={pk_data_type})"
        )

        # Calculate boundaries using NTILE
        try:
            boundaries = calculate_ntile_boundaries(
                source_hook, safe_source_schema, safe_table_name,
                safe_pk_column, partition_count, is_uuid_pk
            )
        except Exception as e:
            logger.warning(f"NTILE query failed for {safe_table_name}: {e}")
            logger.info(f"Falling back to ROW_NUMBER-based partitioning for {safe_table_name}")
            boundaries = calculate_row_number_boundaries(
                source_hook, safe_source_schema, safe_table_name,
                safe_pk_column, partition_count, row_count
            )
            if not boundaries:
                logger.warning(f"ROW_NUMBER fallback failed for {safe_table_name}")
                continue
            logger.info(f"  Created {len(boundaries)} ROW_NUMBER-based partitions for {safe_table_name}")

        if not boundaries:
            logger.warning(f"Could not determine partition boundaries for {safe_table_name}")
            continue

        # Build partition configs
        table_partitions = build_partition_configs(table_info, boundaries, safe_pk_column)
        partitions.extend(table_partitions)

        logger.info(f"  Created {len(boundaries)} NTILE-based partitions for {safe_table_name}")

    logger.info(f"Total: {len(partitions)} partitions")
    return partitions
