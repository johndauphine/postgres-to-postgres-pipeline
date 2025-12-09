# Plan: Fix Priority Data Transfer Bugs

Based on Codex review findings, fix three critical bugs in `include/pg_migration/data_transfer.py`.

---

## Bug 1: Null Handling in COPY (Data Loss)

**Problem:** Empty strings become NULL because:
- `_normalize_value()` maps `None` → `''` (empty string)
- COPY declares `NULL ''`
- Result: Both `None` AND genuine empty strings become NULL

**Fix Location:** `include/pg_migration/data_transfer.py`

**Changes:**
1. Line ~506-509: Change COPY NULL marker from `''` to `\N`
   ```python
   # Before
   "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '')"
   # After
   "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL E'\\\\N')"
   ```

2. Lines ~517-543: Update `_normalize_value()` to return `\\N` for None values
   ```python
   if value is None:
       return '\\N'  # Distinct marker, not empty string
   ```

3. Update fallback cases (non-finite floats, bytes decode failures) to also use `\\N`

---

## Bug 2: Pagination Key Selection (Wrong Column)

**Problem:** `_get_primary_key_column()` prefers 'id' column over actual PK:
- Lines 372-375 check for 'id' column FIRST
- Only queries actual PK if no 'id' exists
- Tables with 'id' that's not the PK use wrong pagination column

**Fix Location:** `include/pg_migration/data_transfer.py` lines 354-395

**Changes:**
1. Reorder logic: Query actual PK first, fall back to 'id' only if no PK defined
   ```python
   def _get_primary_key_column(self, schema_name, table_name, columns):
       # 1. First try to get actual primary key from database
       query = """
       SELECT a.attname
       FROM pg_catalog.pg_constraint con
       ...
       """
       try:
           pk_cols = self.source_hook.get_records(query, parameters=(schema_name, table_name))
           if pk_cols:
               return pk_cols[0][0]
       except Exception:
           pass

       # 2. Fall back to 'id' column if it exists
       for col in columns:
           if col.lower() == 'id':
               return col

       # 3. Last resort: first column
       return columns[0] if columns else 'id'
   ```

---

## Bug 3: SQL Identifier Quoting (Injection Risk)

**Problem:** Schema/table names are unquoted in SQL, causing:
- SQL injection risk
- Failures with reserved keywords (select, order, user)
- Failures with mixed-case identifiers
- Failures with special characters

**Affected Locations:**
- `_get_row_count()` lines 308-310
- `_truncate_table()` line 327
- `_read_chunk_keyset()` lines 431-455
- `_write_chunk()` lines 505-508

**Changes:**

1. Add import at top of file:
   ```python
   from psycopg2 import sql
   ```

2. Update `_get_row_count()`:
   ```python
   query = sql.SQL('SELECT COUNT(*) FROM {}.{}').format(
       sql.Identifier(schema_name),
       sql.Identifier(table_name)
   )
   ```

3. Update `_truncate_table()`:
   ```python
   query = sql.SQL('TRUNCATE TABLE {}.{} CASCADE').format(
       sql.Identifier(schema_name),
       sql.Identifier(table_name)
   )
   ```

4. Update `_read_chunk_keyset()`:
   ```python
   base_query = sql.SQL('SELECT {columns}, ctid FROM {schema}.{table}').format(
       columns=sql.SQL(', ').join([sql.Identifier(col) for col in columns]),
       schema=sql.Identifier(schema_name),
       table=sql.Identifier(table_name)
   )
   ```

5. Update `_write_chunk()` COPY statement:
   ```python
   copy_sql = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, ...)').format(
       sql.Identifier(schema_name),
       sql.Identifier(table_name),
       sql.SQL(', ').join([sql.Identifier(col) for col in columns])
   )
   ```

---

## Verification

After implementing all fixes, trigger the migration DAG to verify fixes work in production.

---

## Files to Modify

| File | Changes |
|------|---------|
| `include/pg_migration/data_transfer.py` | All three fixes |

---

## Implementation Order

1. **Bug 3 (Identifier Quoting)** - Foundation fix, enables safe testing
2. **Bug 1 (Null Handling)** - Data integrity fix
3. **Bug 2 (Pagination Key)** - Logic fix

This order ensures SQL safety first, then data correctness.
