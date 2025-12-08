"""
PostgreSQL to PostgreSQL Type Mapping Module

This module provides data type mapping from PostgreSQL source to PostgreSQL target.
Since both databases are PostgreSQL, the mapping is straightforward - types are
preserved as-is. This module exists for API compatibility with the migration framework.
"""

from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


# PostgreSQL type mapping - identity mapping since both source and target are PostgreSQL
# This preserves all PostgreSQL types exactly as they are
TYPE_MAPPING = {
    # Numeric Types
    "smallint": "SMALLINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "decimal": "DECIMAL({precision},{scale})",
    "numeric": "NUMERIC({precision},{scale})",
    "real": "REAL",
    "double precision": "DOUBLE PRECISION",
    "smallserial": "SMALLSERIAL",
    "serial": "SERIAL",
    "bigserial": "BIGSERIAL",

    # Monetary Type
    "money": "MONEY",

    # Character Types
    "character varying": "VARCHAR({length})",
    "varchar": "VARCHAR({length})",
    "character": "CHAR({length})",
    "char": "CHAR({length})",
    "bpchar": "CHAR({length})",
    "text": "TEXT",

    # Binary Data Types
    "bytea": "BYTEA",

    # Date/Time Types
    "timestamp without time zone": "TIMESTAMP({precision})",
    "timestamp with time zone": "TIMESTAMP({precision}) WITH TIME ZONE",
    "timestamp": "TIMESTAMP({precision})",
    "timestamptz": "TIMESTAMP({precision}) WITH TIME ZONE",
    "date": "DATE",
    "time without time zone": "TIME({precision})",
    "time with time zone": "TIME({precision}) WITH TIME ZONE",
    "time": "TIME({precision})",
    "timetz": "TIME({precision}) WITH TIME ZONE",
    "interval": "INTERVAL",

    # Boolean Type
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",

    # Geometric Types
    "point": "POINT",
    "line": "LINE",
    "lseg": "LSEG",
    "box": "BOX",
    "path": "PATH",
    "polygon": "POLYGON",
    "circle": "CIRCLE",

    # Network Address Types
    "cidr": "CIDR",
    "inet": "INET",
    "macaddr": "MACADDR",
    "macaddr8": "MACADDR8",

    # Bit String Types
    "bit": "BIT({length})",
    "bit varying": "VARBIT({length})",
    "varbit": "VARBIT({length})",

    # Text Search Types
    "tsvector": "TSVECTOR",
    "tsquery": "TSQUERY",

    # UUID Type
    "uuid": "UUID",

    # XML Type
    "xml": "XML",

    # JSON Types
    "json": "JSON",
    "jsonb": "JSONB",

    # Array Types are handled dynamically

    # Range Types
    "int4range": "INT4RANGE",
    "int8range": "INT8RANGE",
    "numrange": "NUMRANGE",
    "tsrange": "TSRANGE",
    "tstzrange": "TSTZRANGE",
    "daterange": "DATERANGE",

    # Other Types
    "oid": "OID",
    "regclass": "REGCLASS",
    "regproc": "REGPROC",
    "regtype": "REGTYPE",
}


def map_type(
    pg_type: str,
    max_length: Optional[int] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    is_max: bool = False
) -> str:
    """
    Map a PostgreSQL data type (identity mapping for PostgreSQL-to-PostgreSQL).

    Args:
        pg_type: The PostgreSQL data type name
        max_length: Maximum length for character/binary types
        precision: Precision for numeric/datetime types
        scale: Scale for numeric types
        is_max: Whether the type is unbounded (not applicable for PostgreSQL)

    Returns:
        The PostgreSQL data type (same as input with proper formatting)
    """
    # Normalize type name to lowercase for comparison
    base_type = pg_type.lower().strip()

    # Handle array types
    if base_type.endswith('[]'):
        element_type = base_type[:-2]
        mapped_element = map_type(element_type, max_length, precision, scale)
        return f"{mapped_element}[]"

    # Check if we have a mapping
    if base_type not in TYPE_MAPPING:
        # For unknown types, return as-is (PostgreSQL is flexible)
        logger.debug(f"Type '{pg_type}' not in mapping, using as-is")
        return pg_type

    mapped_type = TYPE_MAPPING[base_type]

    # Replace placeholders with actual values
    if "{length}" in mapped_type:
        if max_length and max_length > 0:
            mapped_type = mapped_type.replace("{length}", str(max_length))
        else:
            # Remove length specification for unbounded
            mapped_type = mapped_type.replace("({length})", "")

    if "{precision}" in mapped_type:
        if precision is not None:
            mapped_type = mapped_type.replace("{precision}", str(precision))
        else:
            # Default precision for timestamp types
            if "timestamp" in mapped_type.lower() or "time" in mapped_type.lower():
                mapped_type = mapped_type.replace("{precision}", "6")
            else:
                mapped_type = mapped_type.replace("{precision}", "18")

    if "{scale}" in mapped_type:
        if scale is not None:
            mapped_type = mapped_type.replace("{scale}", str(scale))
        else:
            mapped_type = mapped_type.replace("{scale}", "0")

    return mapped_type


def map_column(column_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a complete column definition (identity mapping for PostgreSQL).

    Args:
        column_info: Dictionary containing column metadata

    Returns:
        Dictionary with PostgreSQL column definition
    """
    result = {
        'column_name': column_info['column_name'],
        'is_nullable': column_info.get('is_nullable', True),
        'default_value': column_info.get('default_value'),
        'is_identity': column_info.get('is_identity', False),
    }

    # Map the data type (identity mapping)
    result['data_type'] = map_type(
        column_info['data_type'],
        column_info.get('max_length'),
        column_info.get('precision'),
        column_info.get('scale'),
        column_info.get('is_max', False)
    )

    # Handle identity/serial columns
    if result['is_identity']:
        base_type = column_info['data_type'].lower()
        if 'bigint' in base_type or 'bigserial' in base_type:
            result['data_type'] = 'BIGSERIAL'
        elif 'smallint' in base_type or 'smallserial' in base_type:
            result['data_type'] = 'SMALLSERIAL'
        elif 'int' in base_type or 'serial' in base_type:
            result['data_type'] = 'SERIAL'

    return result


def map_default_value(pg_default: str) -> Optional[str]:
    """
    Map PostgreSQL default value expressions (identity mapping).

    Args:
        pg_default: PostgreSQL default value expression

    Returns:
        Same default value (PostgreSQL to PostgreSQL needs no conversion)
    """
    if not pg_default:
        return None

    # For PostgreSQL-to-PostgreSQL, defaults can be copied as-is
    return pg_default


def map_table_schema(table_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map an entire table schema (identity mapping for PostgreSQL).

    Args:
        table_schema: Complete table schema information

    Returns:
        Mapped table schema for PostgreSQL target
    """
    result = {
        'table_name': table_schema['table_name'],
        'schema_name': table_schema.get('schema_name', 'public'),
        'columns': [],
        'primary_key': table_schema.get('primary_key', []),
        'indexes': [],
        'foreign_keys': [],
    }

    # Map all columns
    for column in table_schema.get('columns', []):
        result['columns'].append(map_column(column))

    # Copy indexes as-is
    for index in table_schema.get('indexes', []):
        result['indexes'].append({
            'index_name': index['index_name'],
            'columns': index['columns'],
            'is_unique': index.get('is_unique', False),
        })

    # Copy foreign keys as-is
    for fk in table_schema.get('foreign_keys', []):
        result['foreign_keys'].append({
            'constraint_name': fk['constraint_name'],
            'columns': fk['columns'],
            'referenced_table': fk['referenced_table'],
            'referenced_columns': fk['referenced_columns'],
            'on_delete': fk.get('on_delete', 'NO ACTION'),
            'on_update': fk.get('on_update', 'NO ACTION'),
        })

    return result


def validate_type_mapping(pg_type: str) -> bool:
    """
    Check if a PostgreSQL type has a known mapping.

    Args:
        pg_type: The PostgreSQL data type to check

    Returns:
        True if the type has a mapping, False otherwise
    """
    normalized = pg_type.lower().strip()
    # Handle array types
    if normalized.endswith('[]'):
        normalized = normalized[:-2]
    return normalized in TYPE_MAPPING


def get_supported_types() -> list:
    """
    Get a list of all supported PostgreSQL data types.

    Returns:
        List of supported PostgreSQL data type names
    """
    return list(TYPE_MAPPING.keys())
