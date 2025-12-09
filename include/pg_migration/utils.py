"""
Shared utility functions for the PostgreSQL migration pipeline.

This module provides common helper functions used across the migration modules,
including SQL identifier validation and literal quoting.
"""

import re
from typing import Union


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.

    Args:
        identifier: The SQL identifier to validate (table name, column name, etc.)
        identifier_type: Description of the identifier type for error messages

    Returns:
        The validated identifier (unchanged if valid)

    Raises:
        ValueError: If the identifier is invalid (empty, too long, or contains
                   invalid characters)
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


def quote_sql_literal(value: Union[int, str]) -> str:
    """
    Quote a value for use in SQL WHERE clause.

    Integers are returned as-is (unquoted), while strings and other types
    are wrapped in single quotes with proper escaping.

    Args:
        value: The value to quote (int, str, UUID, etc.)

    Returns:
        A SQL-safe string representation of the value

    Examples:
        >>> quote_sql_literal(42)
        '42'
        >>> quote_sql_literal("test")
        "'test'"
        >>> quote_sql_literal("O'Brien")
        "'O''Brien'"
    """
    if isinstance(value, int):
        return str(value)
    # For strings, UUIDs, and other types - escape single quotes and wrap in quotes
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
