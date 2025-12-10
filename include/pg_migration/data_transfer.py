"""
Data Transfer Module

This module handles the actual data migration from PostgreSQL source to PostgreSQL target,
including chunked reading, bulk loading, and progress tracking.

Uses psycopg2 connections for keyset pagination and COPY for efficient bulk loading.
"""

from typing import Dict, Any, Optional, List, Tuple, Iterable, Sequence
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date, time as dt_time
from decimal import Decimal
from io import StringIO, TextIOBase
import contextlib
import json
import logging
import os
import threading
import time
import csv
import math
from psycopg2 import pool as pg_pool
from psycopg2 import sql

logger = logging.getLogger(__name__)


class DataTransfer:
    """Handle data transfer from PostgreSQL source to PostgreSQL target."""

    _source_pools: Dict[str, pg_pool.ThreadedConnectionPool] = {}
    _target_pools: Dict[str, pg_pool.ThreadedConnectionPool] = {}
    _pool_lock = threading.Lock()

    def __init__(self, source_conn_id: str, target_conn_id: str):
        """
        Initialize the data transfer handler.

        Args:
            source_conn_id: Airflow connection ID for PostgreSQL source
            target_conn_id: Airflow connection ID for PostgreSQL target
        """
        self.source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        self.target_hook = PostgresHook(postgres_conn_id=target_conn_id)
        self._source_conn_id = source_conn_id
        self._target_conn_id = target_conn_id

        # Initialize shared PostgreSQL connection pools
        self._init_pool(source_conn_id, self.source_hook, DataTransfer._source_pools)
        self._init_pool(target_conn_id, self.target_hook, DataTransfer._target_pools)

    def _init_pool(self, conn_id: str, hook: PostgresHook, pool_dict: Dict[str, pg_pool.ThreadedConnectionPool]):
        """Initialize a connection pool if not already exists.

        Pool size is configurable via environment variables:
        - PG_POOL_MINCONN: Minimum connections (default: 1)
        - PG_POOL_MAXCONN: Maximum connections (default: 8)

        Adjust based on hardware: lower for memory-constrained systems,
        higher for 64GB+ systems with dedicated database servers.
        """
        if conn_id not in pool_dict:
            with DataTransfer._pool_lock:
                if conn_id not in pool_dict:
                    pg_conn = hook.get_connection(conn_id)
                    minconn = int(os.environ.get('PG_POOL_MINCONN', '1'))
                    maxconn = int(os.environ.get('PG_POOL_MAXCONN', '8'))
                    pool_dict[conn_id] = pg_pool.ThreadedConnectionPool(
                        minconn=minconn,
                        maxconn=maxconn,
                        host=pg_conn.host,
                        port=pg_conn.port or 5432,
                        database=pg_conn.schema or pg_conn.login,
                        user=pg_conn.login,
                        password=pg_conn.password,
                    )

    def _acquire_connection(self, pool_dict: Dict[str, pg_pool.ThreadedConnectionPool], conn_id: str, hook: PostgresHook):
        pool = pool_dict.get(conn_id)
        if pool:
            return pool.getconn()
        return hook.get_conn()

    def _release_connection(self, conn, pool_dict: Dict[str, pg_pool.ThreadedConnectionPool], conn_id: str) -> None:
        if conn is None:
            return
        pool = pool_dict.get(conn_id)
        if pool:
            pool.putconn(conn)
        else:
            conn.close()

    @contextlib.contextmanager
    def _source_connection(self):
        conn = self._acquire_connection(DataTransfer._source_pools, self._source_conn_id, self.source_hook)
        try:
            yield conn
        finally:
            if conn and getattr(conn, "autocommit", False) is False:
                try:
                    conn.rollback()
                except Exception as e:
                    logger.exception("Exception occurred during source PostgreSQL connection rollback")
            self._release_connection(conn, DataTransfer._source_pools, self._source_conn_id)

    @contextlib.contextmanager
    def _target_connection(self):
        conn = self._acquire_connection(DataTransfer._target_pools, self._target_conn_id, self.target_hook)
        try:
            # Apply session-level performance tuning for bulk loads
            self._apply_bulk_load_settings(conn)
            yield conn
        finally:
            if conn and getattr(conn, "autocommit", False) is False:
                try:
                    conn.rollback()
                except Exception as e:
                    logger.exception("Exception occurred during target PostgreSQL connection rollback")
            self._release_connection(conn, DataTransfer._target_pools, self._target_conn_id)

    def _apply_bulk_load_settings(self, conn) -> None:
        """Apply PostgreSQL session settings optimized for bulk data loading."""
        settings = [
            # Increase memory for maintenance operations (sorting, index creation)
            ("maintenance_work_mem", "'256MB'"),
            # Increase memory for complex operations
            ("work_mem", "'128MB'"),
            # Disable synchronous commit for faster writes (data is still durable after commit)
            ("synchronous_commit", "off"),
        ]
        try:
            with conn.cursor() as cursor:
                for param, value in settings:
                    try:
                        cursor.execute(f"SET {param} = {value}")
                    except Exception as e:
                        logger.debug(f"Could not set {param}: {e}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Could not apply bulk load settings: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    def transfer_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        chunk_size: int = 10000,
        truncate_target: bool = True,
        columns: Optional[List[str]] = None,
        where_clause: Optional[Tuple[str, Sequence[Any]]] = None
    ) -> Dict[str, Any]:
        """
        Transfer data from PostgreSQL source table to PostgreSQL target table.

        Args:
            source_schema: Source schema name in PostgreSQL source
            source_table: Source table name in PostgreSQL source
            target_schema: Target schema name in PostgreSQL target
            target_table: Target table name in PostgreSQL target
            chunk_size: Number of rows to transfer per batch
            truncate_target: Whether to truncate target table before transfer
            columns: Specific columns to transfer (None for all columns)
            where_clause: Optional WHERE clause and parameters for filtering source data

        Returns:
            Transfer result dictionary with statistics
        """
        start_time = time.time()
        logger.info(f"Starting transfer: {source_schema}.{source_table} -> {target_schema}.{target_table}")

        # Get source row count
        source_row_count = self._get_row_count(source_schema, source_table, is_source=True, where_clause=where_clause)
        logger.info(f"Source table has {source_row_count:,} rows{' (filtered)' if where_clause else ''}")

        # Truncate target if requested
        if truncate_target:
            self._truncate_table(target_schema, target_table)
            logger.info(f"Truncated target table {target_schema}.{target_table}")

        # Get column list if not specified
        if not columns:
            columns = self._get_table_columns(source_schema, source_table)
            logger.info(f"Transferring {len(columns)} columns")

        # Right-size chunk size for given table
        optimal_chunk_size = self._calculate_optimal_chunk_size(source_row_count, chunk_size)
        if optimal_chunk_size != chunk_size:
            logger.info(
                "Adjusted chunk size from %s to %s rows based on table volume",
                chunk_size,
                optimal_chunk_size,
            )
            chunk_size = optimal_chunk_size

        rows_transferred = 0
        chunks_processed = 0
        errors = []

        pk_columns = self._get_primary_key_columns(source_schema, source_table, columns)
        if pk_columns:
            logger.info(f"Using PK columns {pk_columns} for keyset pagination")
        else:
            logger.warning(
                f"No primary key found for {source_schema}.{source_table}. "
                "Using ctid-only pagination (slower for large tables)."
            )
        # Get indices of PK columns in the column list
        pk_indices = [columns.index(pk) for pk in pk_columns if pk in columns]

        try:
            with self._source_connection() as source_conn, self._target_connection() as target_conn:
                last_key_value = None  # Will be tuple of (pk_values..., ctid) or just (ctid,)
                while rows_transferred < source_row_count:
                    chunk_start = time.time()

                    rows, last_key_value = self._read_chunk_keyset(
                        source_conn,
                        source_schema,
                        source_table,
                        columns,
                        pk_columns,
                        last_key_value,
                        chunk_size,
                        pk_indices,
                        where_clause,
                    )

                    if not rows:
                        break

                    rows_written = self._write_chunk(
                        rows,
                        target_schema,
                        target_table,
                        columns,
                        target_conn
                    )

                    rows_transferred += rows_written
                    chunks_processed += 1

                    chunk_time = time.time() - chunk_start
                    rows_per_second = rows_written / chunk_time if chunk_time > 0 else 0

                    logger.info(
                        f"Chunk {chunks_processed}: Transferred {rows_written:,} rows "
                        f"({rows_transferred:,}/{source_row_count:,} total) "
                        f"at {rows_per_second:,.0f} rows/sec"
                    )

                    target_conn.commit()

        except Exception as e:
            error_msg = f"Error transferring data: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

        # Get final row count in target (match partition filter when provided)
        target_row_count = self._get_row_count(
            target_schema,
            target_table,
            is_source=False,
            where_clause=where_clause,
        )

        elapsed_time = time.time() - start_time
        avg_rows_per_second = rows_transferred / elapsed_time if elapsed_time > 0 else 0

        result = {
            'source_table': f"{source_schema}.{source_table}",
            'target_table': f"{target_schema}.{target_table}",
            'source_row_count': source_row_count,
            'target_row_count': target_row_count,
            'rows_transferred': rows_transferred,
            'chunks_processed': chunks_processed,
            'chunk_size': chunk_size,
            'elapsed_time_seconds': elapsed_time,
            'avg_rows_per_second': avg_rows_per_second,
            'success': len(errors) == 0 and target_row_count == source_row_count,
            'errors': errors,
            'timestamp': datetime.now().isoformat(),
        }

        if result['success']:
            logger.info(
                f"Successfully transferred {rows_transferred:,} rows in {elapsed_time:.2f} seconds "
                f"({avg_rows_per_second:,.0f} rows/sec average)"
            )
        else:
            logger.warning(
                f"Transfer completed with issues. Source: {source_row_count:,}, "
                f"Target: {target_row_count:,}, Transferred: {rows_transferred:,}"
            )

        return result

    def _get_row_count(
        self,
        schema_name: str,
        table_name: str,
        is_source: bool = True,
        where_clause: Optional[Tuple[str, Sequence[Any]]] = None
    ) -> int:
        """
        Get row count from a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            is_source: Whether this is the source or target PostgreSQL
            where_clause: Optional tuple of (WHERE sql, params) for filtering (applied to source or target)

        Returns:
            Row count
        """
        # Use sql.Identifier for safe quoting of schema/table names
        base_query = sql.SQL('SELECT COUNT(*) FROM {}.{}').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )

        params: Sequence[Any] = ()
        if where_clause:
            clause_sql, clause_params = where_clause
            full_query = sql.SQL('{} WHERE {}').format(base_query, sql.SQL(clause_sql))
            params = clause_params
        else:
            full_query = base_query

        # Execute using hook's connection
        hook = self.source_hook if is_source else self.target_hook
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(full_query, params)
                result = cursor.fetchone()
                return result[0] if result else 0

    def _truncate_table(self, schema_name: str, table_name: str) -> None:
        """
        Truncate a PostgreSQL table.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        query = sql.SQL('TRUNCATE TABLE {}.{} CASCADE').format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )
        with self.target_hook.get_conn() as conn:
            query_str = query.as_string(conn)
            with conn.cursor() as cursor:
                cursor.execute(query_str)
            conn.commit()

    def _get_table_columns(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get column names from PostgreSQL source table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column names
        """
        query = """
        SELECT a.attname
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """
        columns = self.source_hook.get_records(query, parameters=(schema_name, table_name))
        return [col[0] for col in columns]

    def _get_primary_key_columns(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str]
    ) -> List[str]:
        """
        Get all primary key columns for keyset pagination (supports composite PKs).

        Priority order:
        1. Actual primary key columns from database (ordered by key position)
        2. 'id' column as fallback (common convention)
        3. Empty list if no PK found (will use ctid-only pagination)

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: List of available columns

        Returns:
            List of column names forming the primary key, empty if none found
        """
        # 1. First try to get actual primary key columns from database
        query = """
        SELECT a.attname
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND con.contype = 'p'
        ORDER BY array_position(con.conkey, a.attnum)
        """
        try:
            pk_cols = self.source_hook.get_records(query, parameters=(schema_name, table_name))
            if pk_cols:
                return [col[0] for col in pk_cols]
        except Exception:
            pass

        # 2. Fall back to 'id' column if it exists (case-insensitive)
        for col in columns:
            if col.lower() == 'id':
                return [col]

        # 3. No PK found - return empty list (will use ctid-only pagination)
        return []

    def _calculate_optimal_chunk_size(self, row_count: int, requested_chunk: int) -> int:
        """Determine an appropriate chunk size based on table volume."""
        if row_count <= 0:
            return requested_chunk

        if row_count < 100_000:
            target = min(requested_chunk, 10_000)
        elif row_count < 1_000_000:
            target = max(requested_chunk, 20_000)
        elif row_count < 5_000_000:
            target = max(requested_chunk, 50_000)
        else:
            target = max(requested_chunk, 100_000)

        return min(max(target, 5_000), 200_000)

    def _read_chunk_keyset(
        self,
        conn,
        schema_name: str,
        table_name: str,
        columns: List[str],
        pk_columns: List[str],
        last_key_value: Optional[Tuple[Any, ...]],
        limit: int,
        pk_indices: List[int],
        where_clause: Optional[Tuple[str, Sequence[Any]]] = None,
    ) -> Tuple[List[Tuple[Any, ...]], Optional[Tuple[Any, ...]]]:
        """Read rows using keyset pagination with deterministic ordering.

        Supports composite primary keys by using tuple comparison:
        (pk1, pk2, ..., ctid) > (last_pk1, last_pk2, ..., last_ctid)

        If no primary key exists, falls back to ctid-only ordering.

        Args:
            conn: Active database connection
            schema_name: Source schema name
            table_name: Source table name
            columns: List of columns to select
            pk_columns: List of primary key column names (can be empty)
            last_key_value: Tuple of (pk_values..., ctid) from previous chunk, or None
            limit: Maximum rows to fetch
            pk_indices: Indices of PK columns in the columns list
            where_clause: Optional tuple of (WHERE sql, params) for partition filtering

        Returns:
            Tuple of (rows, next_key_value) where next_key_value is (pk_values..., ctid)
        """

        # Build column list with proper quoting using psycopg2.sql
        columns_sql = sql.SQL(', ').join([sql.Identifier(col) for col in columns])

        # Build base SELECT with properly quoted identifiers
        base_query = sql.SQL('SELECT {columns}, ctid FROM {schema}.{table}').format(
            columns=columns_sql,
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )

        # Build ORDER BY clause: (pk1, pk2, ..., ctid) or just (ctid) if no PK
        if pk_columns:
            order_cols = [sql.Identifier(pk) for pk in pk_columns] + [sql.SQL('ctid')]
        else:
            order_cols = [sql.SQL('ctid')]
        order_by = sql.SQL('ORDER BY {}').format(sql.SQL(', ').join(order_cols))
        limit_sql = sql.SQL('LIMIT {}').format(sql.Literal(limit))

        # Build WHERE clause combining filter and pagination
        where_conditions = []
        params: List[Any] = []

        if where_clause:
            clause_sql, clause_params = where_clause
            where_conditions.append(sql.SQL('({})').format(sql.SQL(clause_sql)))
            params.extend(clause_params)

        if last_key_value is not None:
            # Build tuple comparison: (pk1, pk2, ..., ctid) > (%s, %s, ..., %s)
            if pk_columns:
                tuple_cols = [sql.Identifier(pk) for pk in pk_columns] + [sql.SQL('ctid')]
            else:
                tuple_cols = [sql.SQL('ctid')]

            placeholders = sql.SQL(', ').join([sql.SQL('%s')] * len(last_key_value))
            tuple_comparison = sql.SQL('({cols}) > ({placeholders})').format(
                cols=sql.SQL(', ').join(tuple_cols),
                placeholders=placeholders
            )
            where_conditions.append(tuple_comparison)
            params.extend(list(last_key_value))

        if where_conditions:
            where_part = sql.SQL('WHERE {}').format(sql.SQL(' AND ').join(where_conditions))
            query = sql.SQL('{base} {where} {order} {limit}').format(
                base=base_query,
                where=where_part,
                order=order_by,
                limit=limit_sql
            )
        else:
            query = sql.SQL('{base} {order} {limit}').format(
                base=base_query,
                order=order_by,
                limit=limit_sql
            )

        try:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    return [], last_key_value

                # Last row: extract pk values and ctid for next pagination
                last_row = rows[-1]
                ctid = last_row[-1]  # ctid is always last column

                # Build next_key_value tuple: (pk_values..., ctid)
                if pk_indices:
                    pk_values = tuple(last_row[idx] for idx in pk_indices)
                    next_key_value = pk_values + (ctid,)
                else:
                    next_key_value = (ctid,)

                # Remove ctid from returned rows (it's only for pagination)
                cleaned_rows = [row[:-1] for row in rows]

                return cleaned_rows, next_key_value
        except Exception as e:
            logger.error(f"Error reading chunk after key {last_key_value}: {str(e)}")
            raise

    def _write_chunk(
        self,
        rows: List[Tuple[Any, ...]],
        schema_name: str,
        table_name: str,
        columns: List[str],
        target_conn
    ) -> int:
        """
        Stream rows to PostgreSQL using COPY.

        Args:
            rows: Sequence of rows to write
            schema_name: Target schema name
            table_name: Target table name
            columns: List of column names
            target_conn: Active PostgreSQL connection

        Returns:
            Number of rows written
        """
        if not rows:
            return 0

        # Build COPY statement with properly quoted identifiers
        columns_sql = sql.SQL(', ').join([sql.Identifier(col) for col in columns])
        copy_sql = sql.SQL(
            'COPY {schema}.{table} ({columns}) '
            "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL E'\\\\N')"
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=columns_sql
        )

        stream = _CSVRowStream(rows, self._normalize_value)
        with target_conn.cursor() as cursor:
            cursor.copy_expert(copy_sql.as_string(target_conn), stream)

        return len(rows)

    def _normalize_value(self, value: Any) -> Any:
        """Normalize Python values for COPY consumption.

        Uses \\N as the NULL marker (distinct from empty string '') to prevent
        data loss where empty strings would otherwise become NULL.
        """
        if value is None:
            return '\\N'  # Distinct NULL marker, not empty string

        if isinstance(value, datetime):
            return value.isoformat(sep=' ')
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dt_time):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bool):
            return 't' if value else 'f'
        if isinstance(value, (dict, list)):
            # JSONB/JSON columns: serialize as proper JSON string
            return json.dumps(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                return bytes(value).decode('utf-8', 'ignore')
            except Exception:
                return '\\N'  # NULL for decode failures
        if isinstance(value, float) and not math.isfinite(value):
            return '\\N'  # NULL for non-finite floats (NaN, Inf)

        return value


def transfer_table_data(
    source_conn_id: str,
    target_conn_id: str,
    table_info: Dict[str, Any],
    chunk_size: int = 10000,
    truncate: bool = True,
    where_clause: Optional[Tuple[str, Sequence[Any]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to transfer a single table.

    Args:
        source_conn_id: PostgreSQL source connection ID
        target_conn_id: PostgreSQL target connection ID
        table_info: Table information dictionary with schema and table names
        chunk_size: Rows per chunk
        truncate: Whether to truncate target before transfer
    where_clause: Optional tuple of (WHERE sql, params) for filtering source data

    Returns:
        Transfer result dictionary
    """
    transfer = DataTransfer(source_conn_id, target_conn_id)

    source_schema = table_info.get('source_schema', table_info.get('schema_name', 'public'))
    source_table = table_info['table_name']
    target_schema = table_info.get('target_schema', 'public')
    target_table = table_info.get('target_table', source_table)

    return transfer.transfer_table(
        source_schema=source_schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        chunk_size=chunk_size,
        truncate_target=truncate,
        columns=table_info.get('columns'),
        where_clause=where_clause
    )


def parallel_transfer_tables(
    source_conn_id: str,
    target_conn_id: str,
    tables: List[Dict[str, Any]],
    chunk_size: int = 10000,
    truncate: bool = True
) -> List[Dict[str, Any]]:
    """
    Transfer multiple tables (designed for use with Airflow's expand operator).

    Args:
        source_conn_id: PostgreSQL source connection ID
        target_conn_id: PostgreSQL target connection ID
        tables: List of table information dictionaries
        chunk_size: Rows per chunk
        truncate: Whether to truncate targets before transfer

    Returns:
        List of transfer result dictionaries
    """
    results = []
    for table_info in tables:
        result = transfer_table_data(
            source_conn_id,
            target_conn_id,
            table_info,
            chunk_size,
            truncate
        )
        results.append(result)

    return results


class _CSVRowStream(TextIOBase):
    """Lazy text stream that feeds COPY FROM without large buffers."""

    def __init__(self, rows: Iterable[Tuple[Any, ...]], normalizer):
        self._iterator = iter(rows)
        self._normalizer = normalizer
        self._buffer = ''
        self._exhausted = False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> str:
        while (size < 0 or len(self._buffer) < size) and not self._exhausted:
            try:
                row = next(self._iterator)
            except StopIteration:
                self._exhausted = True
                break
            self._buffer += self._format_row(row)

        if size < 0:
            data = self._buffer
            self._buffer = ''
            return data

        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return data

    def _format_row(self, row: Tuple[Any, ...]) -> str:
        buffer = StringIO()
        writer = csv.writer(
            buffer,
            delimiter='\t',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n',
        )
        writer.writerow([self._normalizer(value) for value in row])
        return buffer.getvalue()
