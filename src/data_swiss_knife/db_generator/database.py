"""PostgreSQL database operations with fast COPY insert."""

from io import StringIO

import pandas as pd
import psycopg


CONNECTION_TIMEOUT = 15  # seconds


def test_connection(
    host: str, port: int, database: str, user: str, password: str
) -> tuple[bool, str]:
    """Test PostgreSQL connection with 15s timeout."""
    try:
        conn_str = f"host={host} port={port} dbname={database} user={user} password={password} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
        return True, f"Connected: {version[:50]}..."
    except psycopg.OperationalError as e:
        if "timeout" in str(e).lower():
            return False, f"Connection timeout after {CONNECTION_TIMEOUT} seconds"
        return False, str(e)
    except Exception as e:
        return False, str(e)


def create_table(
    conn_str: str,
    schema: str,
    table_name: str,
    columns: list[dict],
    primary_key: str | None = None,
    indexes: list[str] | None = None,
) -> tuple[bool, str]:
    """Create a PostgreSQL table with specified schema."""
    try:
        # Build column definitions
        col_defs = []
        for col in columns:
            col_def = f'"{col["name"]}" {col["pg_type"]}'
            if col.get("not_null"):
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Add primary key constraint
        if primary_key:
            col_defs.append(f'PRIMARY KEY ("{primary_key}")')

        columns_sql = ",\n    ".join(col_defs)

        create_sql = f"""
CREATE TABLE "{schema}"."{table_name}" (
    {columns_sql}
)
"""
        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                # Create table
                cur.execute(create_sql)

                # Create indexes
                if indexes:
                    for idx_col in indexes:
                        idx_name = f"idx_{table_name}_{idx_col}"
                        cur.execute(
                            f'CREATE INDEX "{idx_name}" '
                            f'ON "{schema}"."{table_name}" ("{idx_col}")'
                        )

                conn.commit()

        return True, f"Table {schema}.{table_name} created successfully"
    except psycopg.OperationalError as e:
        if "timeout" in str(e).lower():
            return False, f"Connection timeout after {CONNECTION_TIMEOUT} seconds"
        return False, str(e)
    except Exception as e:
        return False, str(e)


def insert_data_copy(
    conn_str: str,
    schema: str,
    table_name: str,
    df: pd.DataFrame,
    date_formats: dict[str, str] | None = None,
) -> tuple[bool, str, int]:
    """Insert data using PostgreSQL COPY for fast bulk insert."""
    try:
        # Apply date format conversions
        df_copy = df.copy()

        if date_formats:
            for col, fmt in date_formats.items():
                if col in df_copy.columns and fmt:
                    df_copy[col] = pd.to_datetime(df_copy[col], format=fmt)

        # Convert to CSV string for COPY
        buffer = StringIO()
        df_copy.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        columns = [f'"{c}"' for c in df_copy.columns]
        copy_sql = f'COPY "{schema}"."{table_name}" ({", ".join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'

        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    for line in buffer:
                        copy.write(line)
                conn.commit()

        return True, "Data inserted successfully", len(df)
    except psycopg.OperationalError as e:
        if "timeout" in str(e).lower():
            return False, f"Connection timeout after {CONNECTION_TIMEOUT} seconds", 0
        return False, str(e), 0
    except Exception as e:
        return False, str(e), 0


def get_schemas(conn_str: str) -> list[str]:
    """Get list of schemas in the database."""
    try:
        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                return [row[0] for row in cur.fetchall()]
    except Exception:
        return ["public"]
