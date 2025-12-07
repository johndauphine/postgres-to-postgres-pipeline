"""
Generate partition stress-test data in the source PostgreSQL database.

Creates multiple tables with different primary key patterns (dense, sparse, UUID)
so the DAG's universal partitioning and keyset pagination paths can be regression-tested.
"""

import argparse
import os
import sys
import psycopg2


def parse_args():
    parser = argparse.ArgumentParser(description="Generate partition test data in Postgres")
    parser.add_argument(
        "--conn",
        default=os.environ.get("AIRFLOW_CONN_POSTGRES_SOURCE", "postgresql://postgres:PostgresPassword123@localhost:5434/source_db"),
        help="Postgres connection URI for the source database",
    )
    parser.add_argument("--schema", default=os.environ.get("SOURCE_SCHEMA", "public"), help="Schema to create test tables in")
    parser.add_argument("--drop-existing", action="store_true", help="Drop existing test tables before recreating")
    parser.add_argument("--rows-dense", type=int, default=1_200_000, help="Rows for dense integer PK table")
    parser.add_argument("--rows-sparse", type=int, default=1_600_000, help="Rows for sparse/gapped integer PK table")
    parser.add_argument("--rows-uuid", type=int, default=900_000, help="Rows for UUID PK table")
    return parser.parse_args()


def run_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())


def ensure_extensions(conn):
    run_sql(conn, 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    run_sql(conn, 'CREATE EXTENSION IF NOT EXISTS "pgcrypto";')


def drop_tables(conn, schema):
    for name in ("partition_int_dense", "partition_int_sparse", "partition_uuid_pk"):
        run_sql(conn, f'DROP TABLE IF EXISTS {schema}."{name}" CASCADE;')


def create_tables(conn, schema):
    run_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."partition_int_dense" (
            id BIGINT PRIMARY KEY,
            payload TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    )
    run_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."partition_int_sparse" (
            id BIGINT PRIMARY KEY,
            payload TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    )
    run_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."partition_uuid_pk" (
            id UUID PRIMARY KEY,
            payload TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    )


def load_dense(conn, schema, row_count):
    run_sql(
        conn,
        f"""
        INSERT INTO {schema}."partition_int_dense" (id, payload, created_at)
        SELECT gs,
               md5(random()::text),
               NOW() - (gs %% 1000) * INTERVAL '1 second'
        FROM generate_series(1, %s) AS gs;
        """,
        (row_count,),
    )


def load_sparse(conn, schema, row_count):
    # Gapped PKs (multiples of 10) to exercise NTILE boundaries and non-contiguous ranges
    run_sql(
        conn,
        f"""
        INSERT INTO {schema}."partition_int_sparse" (id, payload, created_at)
        SELECT gs * 10,
               md5(random()::text),
               NOW() - (gs %% 500) * INTERVAL '1 second'
        FROM generate_series(1, %s) AS gs;
        """,
        (row_count,),
    )


def load_uuid(conn, schema, row_count):
    run_sql(
        conn,
        f"""
        INSERT INTO {schema}."partition_uuid_pk" (id, payload, created_at)
        SELECT gen_random_uuid(),
               md5(random()::text),
               NOW() - (gs %% 750) * INTERVAL '1 second'
        FROM generate_series(1, %s) AS gs;
        """,
        (row_count,),
    )


def summarize(conn, schema):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT table_name, row_count
            FROM (
                SELECT 'partition_int_dense' AS table_name, COUNT(*) AS row_count FROM {schema}."partition_int_dense"
                UNION ALL
                SELECT 'partition_int_sparse', COUNT(*) FROM {schema}."partition_int_sparse"
                UNION ALL
                SELECT 'partition_uuid_pk', COUNT(*) FROM {schema}."partition_uuid_pk"
            ) s
            ORDER BY table_name;
            """
        )
        return cur.fetchall()


def main():
    args = parse_args()
    conn = psycopg2.connect(args.conn)
    conn.autocommit = True

    try:
        ensure_extensions(conn)
        run_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {args.schema};")

        if args.drop_existing:
            drop_tables(conn, args.schema)

        create_tables(conn, args.schema)
        load_dense(conn, args.schema, args.rows_dense)
        load_sparse(conn, args.schema, args.rows_sparse)
        load_uuid(conn, args.schema, args.rows_uuid)

        counts = summarize(conn, args.schema)
        for table_name, row_count in counts:
            print(f"{table_name}: {row_count:,} rows")
        print("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
