"""Output handlers for query results."""

from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg


def export_to_csv(df: pd.DataFrame, file_path: str | Path) -> tuple[bool, str]:
    """Export DataFrame to CSV file."""
    try:
        df.to_csv(file_path, index=False)
        return True, f"Exported {len(df)} rows to {file_path}"
    except Exception as e:
        return False, str(e)


def export_to_excel(df: pd.DataFrame, file_path: str | Path) -> tuple[bool, str]:
    """Export DataFrame to Excel file."""
    try:
        df.to_excel(file_path, index=False, engine='openpyxl')
        return True, f"Exported {len(df)} rows to {file_path}"
    except Exception as e:
        return False, str(e)


def insert_to_table(
    conn_str: str,
    df: pd.DataFrame,
    schema: str,
    table_name: str,
) -> tuple[bool, str, int]:
    """Insert DataFrame into existing table using COPY."""
    try:
        if df.empty:
            return True, "No data to insert", 0

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        columns = [f'"{c}"' for c in df.columns]
        copy_sql = f'COPY "{schema}"."{table_name}" ({", ".join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'

        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    for line in buffer:
                        copy.write(line)
                conn.commit()

        return True, f"Inserted {len(df)} rows into {schema}.{table_name}", len(df)
    except Exception as e:
        return False, str(e), 0


def create_and_insert(
    conn_str: str,
    df: pd.DataFrame,
    schema: str,
    table_name: str,
) -> tuple[bool, str, int]:
    """Create table from DataFrame and insert data."""
    try:
        if df.empty:
            return False, "No data to create table from", 0

        # Infer column types
        col_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                pg_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                pg_type = "DOUBLE PRECISION"
            elif pd.api.types.is_bool_dtype(dtype):
                pg_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                pg_type = "TIMESTAMP"
            else:
                pg_type = "TEXT"
            col_defs.append(f'"{col}" {pg_type}')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
            {", ".join(col_defs)}
        )
        """

        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                cur.execute(create_sql)
                conn.commit()

        # Insert data
        return insert_to_table(conn_str, df, schema, table_name)

    except Exception as e:
        return False, str(e), 0


def get_tables(conn_str: str, schema: str) -> list[str]:
    """Get list of tables in schema."""
    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                """, (schema,))
                return [row[0] for row in cur.fetchall()]
    except Exception:
        return []
