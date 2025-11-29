"""
PostgreSQL Schema Extraction Module

This module provides functions to extract schema metadata from PostgreSQL databases
using information_schema and pg_catalog views.
"""

from typing import List, Dict, Any, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """Extract schema information from PostgreSQL databases."""

    def __init__(self, postgres_conn_id: str):
        """
        Initialize the schema extractor.

        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL
        """
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_tables(self, schema_name: str = 'public', exclude_patterns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get all tables from a specific schema.

        Args:
            schema_name: Schema name to extract tables from
            exclude_patterns: List of table name patterns to exclude (supports wildcards)

        Returns:
            List of table information dictionaries
        """
        query = """
        SELECT
            t.table_schema AS schema_name,
            t.table_name,
            c.oid AS object_id,
            NULL AS create_date,
            NULL AS modify_date,
            COALESCE(s.n_live_tup, 0) AS row_count
        FROM information_schema.tables t
        JOIN pg_catalog.pg_class c ON c.relname = t.table_name
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
        LEFT JOIN pg_stat_user_tables s ON s.schemaname = t.table_schema AND s.relname = t.table_name
        WHERE t.table_schema = %s
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
        """

        tables = self.postgres_hook.get_records(query, parameters=[schema_name])

        result = []
        for table in tables:
            table_dict = {
                'schema_name': table[0],
                'table_name': table[1],
                'object_id': table[2],
                'create_date': table[3],
                'modify_date': table[4],
                'row_count': table[5] or 0,
            }

            # Apply exclusion patterns
            if exclude_patterns:
                skip = False
                for pattern in exclude_patterns:
                    if self._matches_pattern(table_dict['table_name'], pattern):
                        logger.info(f"Excluding table {table_dict['table_name']} (matches pattern '{pattern}')")
                        skip = True
                        break
                if skip:
                    continue

            result.append(table_dict)

        logger.info(f"Found {len(result)} tables in schema '{schema_name}'")
        return result

    def get_columns(self, table_oid: int) -> List[Dict[str, Any]]:
        """
        Get all columns for a specific table.

        Args:
            table_oid: PostgreSQL OID of the table

        Returns:
            List of column information dictionaries
        """
        query = """
        SELECT
            a.attnum AS column_id,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            CASE
                WHEN a.atttypmod > 0 AND t.typname IN ('varchar', 'char', 'bpchar')
                THEN a.atttypmod - 4
                ELSE NULL
            END AS max_length,
            CASE
                WHEN t.typname = 'numeric' AND a.atttypmod > 0
                THEN ((a.atttypmod - 4) >> 16) & 65535
                ELSE NULL
            END AS precision,
            CASE
                WHEN t.typname = 'numeric' AND a.atttypmod > 0
                THEN (a.atttypmod - 4) & 65535
                ELSE NULL
            END AS scale,
            NOT a.attnotnull AS is_nullable,
            COALESCE(
                (SELECT TRUE FROM pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                 AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%'),
                FALSE
            ) AS is_identity,
            COALESCE(a.attgenerated != '', FALSE) AS is_computed,
            NULL AS seed_value,
            NULL AS increment_value,
            pg_get_expr(d.adbin, d.adrelid) AS default_value,
            CASE WHEN a.attgenerated != '' THEN pg_get_expr(d.adbin, d.adrelid) ELSE NULL END AS computed_definition,
            FALSE AS is_max
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """

        columns = self.postgres_hook.get_records(query, parameters=[table_oid])

        result = []
        for col in columns:
            column_dict = {
                'column_id': col[0],
                'column_name': col[1],
                'data_type': col[2],
                'max_length': col[3],
                'precision': col[4],
                'scale': col[5],
                'is_nullable': bool(col[6]),
                'is_identity': bool(col[7]),
                'is_computed': bool(col[8]),
                'seed_value': col[9],
                'increment_value': col[10],
                'default_value': col[11],
                'computed_definition': col[12],
                'is_max': bool(col[13]),
            }
            result.append(column_dict)

        return result

    def get_primary_key(self, table_oid: int) -> Optional[Dict[str, Any]]:
        """
        Get primary key information for a table.

        Args:
            table_oid: PostgreSQL OID of the table

        Returns:
            Primary key information or None if no primary key exists
        """
        query = """
        SELECT
            con.conname AS constraint_name,
            array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS columns
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
        WHERE con.conrelid = %s
          AND con.contype = 'p'
        GROUP BY con.conname
        """

        result = self.postgres_hook.get_first(query, parameters=[table_oid])

        if result:
            return {
                'constraint_name': result[0],
                'columns': list(result[1]) if result[1] else []
            }
        return None

    def get_indexes(self, table_oid: int) -> List[Dict[str, Any]]:
        """
        Get all indexes for a table (excluding primary key).

        Args:
            table_oid: PostgreSQL OID of the table

        Returns:
            List of index information dictionaries
        """
        query = """
        SELECT
            i.relname AS index_name,
            ix.indisunique AS is_unique,
            am.amname AS type_desc,
            array_agg(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum)) AS columns
        FROM pg_catalog.pg_index ix
        JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = i.relam
        JOIN pg_catalog.pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
        WHERE ix.indrelid = %s
          AND NOT ix.indisprimary
        GROUP BY i.relname, ix.indisunique, am.amname
        """

        indexes = self.postgres_hook.get_records(query, parameters=[table_oid])

        result = []
        for idx in indexes:
            columns = list(idx[3]) if idx[3] else []
            index_dict = {
                'index_name': idx[0],
                'is_unique': bool(idx[1]),
                'type_desc': idx[2].upper() if idx[2] else 'BTREE',
                'columns': columns,
                'columns_with_order': ', '.join(columns),
            }
            result.append(index_dict)

        return result

    def get_foreign_keys(self, table_oid: int) -> List[Dict[str, Any]]:
        """
        Get all foreign keys for a table.

        Args:
            table_oid: PostgreSQL OID of the table

        Returns:
            List of foreign key information dictionaries
        """
        query = """
        SELECT
            con.conname AS constraint_name,
            array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS columns,
            nf.nspname AS referenced_schema,
            cf.relname AS referenced_table,
            array_agg(af.attname ORDER BY array_position(con.confkey, af.attnum)) AS referenced_columns,
            CASE con.confdeltype
                WHEN 'a' THEN 'NO ACTION'
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
                ELSE 'NO ACTION'
            END AS on_delete,
            CASE con.confupdtype
                WHEN 'a' THEN 'NO ACTION'
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
                ELSE 'NO ACTION'
            END AS on_update
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
        JOIN pg_catalog.pg_class cf ON cf.oid = con.confrelid
        JOIN pg_catalog.pg_namespace nf ON nf.oid = cf.relnamespace
        JOIN pg_catalog.pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
        WHERE con.conrelid = %s
          AND con.contype = 'f'
        GROUP BY con.conname, nf.nspname, cf.relname, con.confdeltype, con.confupdtype
        """

        foreign_keys = self.postgres_hook.get_records(query, parameters=[table_oid])

        result = []
        for fk in foreign_keys:
            fk_dict = {
                'constraint_name': fk[0],
                'columns': list(fk[1]) if fk[1] else [],
                'referenced_schema': fk[2],
                'referenced_table': fk[3],
                'referenced_columns': list(fk[4]) if fk[4] else [],
                'on_delete': fk[5],
                'on_update': fk[6],
            }
            result.append(fk_dict)

        return result

    def get_check_constraints(self, table_oid: int) -> List[Dict[str, Any]]:
        """
        Get all check constraints for a table.

        Args:
            table_oid: PostgreSQL OID of the table

        Returns:
            List of check constraint information dictionaries
        """
        query = """
        SELECT
            con.conname AS constraint_name,
            pg_get_constraintdef(con.oid) AS definition,
            FALSE AS is_disabled
        FROM pg_catalog.pg_constraint con
        WHERE con.conrelid = %s
          AND con.contype = 'c'
        """

        constraints = self.postgres_hook.get_records(query, parameters=[table_oid])

        result = []
        for con in constraints:
            constraint_dict = {
                'constraint_name': con[0],
                'definition': con[1],
                'is_disabled': bool(con[2]),
            }
            result.append(constraint_dict)

        return result

    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get complete schema information for a single table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Complete table schema information
        """
        # Get table OID
        query = """
        SELECT c.oid
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
        """

        result = self.postgres_hook.get_first(query, parameters=[schema_name, table_name])

        if not result:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        table_oid = result[0]

        # Get all schema components
        schema_info = {
            'schema_name': schema_name,
            'table_name': table_name,
            'object_id': table_oid,
            'columns': self.get_columns(table_oid),
            'primary_key': self.get_primary_key(table_oid),
            'indexes': self.get_indexes(table_oid),
            'foreign_keys': self.get_foreign_keys(table_oid),
            'check_constraints': self.get_check_constraints(table_oid),
        }

        return schema_info

    def get_all_tables_schema(self, schema_name: str = 'public', exclude_patterns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get complete schema information for all tables in a schema.

        Args:
            schema_name: Schema name to extract from
            exclude_patterns: List of table name patterns to exclude

        Returns:
            List of complete table schema information
        """
        tables = self.get_tables(schema_name, exclude_patterns)
        result = []

        for table in tables:
            logger.info(f"Extracting schema for table {table['table_name']}")
            table_schema = {
                'schema_name': table['schema_name'],
                'table_name': table['table_name'],
                'object_id': table['object_id'],
                'row_count': table['row_count'],
                'columns': self.get_columns(table['object_id']),
                'primary_key': self.get_primary_key(table['object_id']),
                'indexes': self.get_indexes(table['object_id']),
                'foreign_keys': self.get_foreign_keys(table['object_id']),
                'check_constraints': self.get_check_constraints(table['object_id']),
            }
            result.append(table_schema)

        logger.info(f"Extracted schema for {len(result)} tables")
        return result

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """
        Check if a name matches a pattern with wildcard support.

        Args:
            name: Name to check
            pattern: Pattern with optional wildcards (*)

        Returns:
            True if name matches pattern
        """
        import fnmatch
        return fnmatch.fnmatch(name.lower(), pattern.lower())


def extract_schema_info(
    postgres_conn_id: str,
    schema_name: str = 'public',
    exclude_tables: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Convenience function to extract complete schema information.

    Args:
        postgres_conn_id: Airflow connection ID for PostgreSQL source
        schema_name: Schema name to extract from
        exclude_tables: List of table patterns to exclude

    Returns:
        List of complete table schema information
    """
    extractor = SchemaExtractor(postgres_conn_id)
    return extractor.get_all_tables_schema(schema_name, exclude_tables)
