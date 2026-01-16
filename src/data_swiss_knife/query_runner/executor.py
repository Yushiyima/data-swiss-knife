"""Threaded query executor for parameterized queries."""

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from typing import Any, Callable

import pandas as pd
import psycopg

CONNECTION_TIMEOUT = 15  # seconds


@dataclass
class QueryResult:
    """Result of a single query execution."""
    params: dict[str, Any]
    data: pd.DataFrame | None
    error: str | None
    row_count: int
    execution_time: float = 0.0
    rows_inserted: int = 0


@dataclass
class ExecutionStats:
    """Statistics for query execution."""
    total: int = 0
    completed: int = 0
    success: int = 0
    errors: int = 0
    rows_fetched: int = 0
    rows_inserted: int = 0
    start_time: float = 0.0
    elapsed_time: float = 0.0
    avg_time_per_query: float = 0.0
    estimated_remaining: float = 0.0

    def format_time(self, seconds: float) -> str:
        """Format seconds into human readable string."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"


def extract_parameters(query: str) -> list[str]:
    """Extract parameter names from query (e.g., :param_name)."""
    pattern = r':([a-zA-Z_][a-zA-Z0-9_]*)'
    return list(set(re.findall(pattern, query)))


def substitute_params(query: str, params: dict[str, Any]) -> tuple[str, list]:
    """Convert :param_name to %s and return values in order."""
    param_names = []

    def replacer(match):
        name = match.group(1)
        param_names.append(name)
        return '%s'

    pattern = r':([a-zA-Z_][a-zA-Z0-9_]*)'
    new_query = re.sub(pattern, replacer, query)
    values = [params.get(name) for name in param_names]

    return new_query, values


def stream_insert_df(conn_str: str, df: pd.DataFrame, schema: str, table_name: str) -> int:
    """Insert DataFrame to table using COPY (fast). Returns rows inserted."""
    if df.empty:
        return 0

    try:
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        columns = [f'"{c}"' for c in df.columns]
        copy_sql = f'COPY "{schema}"."{table_name}" ({", ".join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'

        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    for line in buffer:
                        copy.write(line)
                conn.commit()

        return len(df)
    except Exception:
        return 0


def execute_single_query(
    conn_str: str,
    query: str,
    params: dict[str, Any],
) -> QueryResult:
    """Execute a single query with given parameters."""
    start_time = time.time()
    try:
        sql, values = substitute_params(query, params)

        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)

                # Check if query returns data
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=columns)
                    exec_time = time.time() - start_time
                    return QueryResult(
                        params=params, data=df, error=None,
                        row_count=len(df), execution_time=exec_time
                    )
                else:
                    conn.commit()
                    exec_time = time.time() - start_time
                    return QueryResult(
                        params=params, data=None, error=None,
                        row_count=cur.rowcount, execution_time=exec_time
                    )

    except Exception as e:
        exec_time = time.time() - start_time
        return QueryResult(
            params=params, data=None, error=str(e),
            row_count=0, execution_time=exec_time
        )


def execute_and_stream(
    conn_str: str,
    query: str,
    params: dict[str, Any],
    target_conn_str: str,
    target_schema: str,
    target_table: str,
    add_param_columns: bool = True,
) -> QueryResult:
    """Execute query and immediately stream results to target table."""
    start_time = time.time()
    try:
        sql, values = substitute_params(query, params)

        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)

                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=columns)

                    # Add parameter columns
                    if add_param_columns:
                        for key, value in params.items():
                            df[f'_param_{key}'] = value

                    # Stream to target table immediately
                    rows_inserted = stream_insert_df(
                        target_conn_str, df, target_schema, target_table
                    )

                    exec_time = time.time() - start_time
                    return QueryResult(
                        params=params, data=None, error=None,
                        row_count=len(df), execution_time=exec_time,
                        rows_inserted=rows_inserted
                    )
                else:
                    conn.commit()
                    exec_time = time.time() - start_time
                    return QueryResult(
                        params=params, data=None, error=None,
                        row_count=cur.rowcount, execution_time=exec_time
                    )

    except Exception as e:
        exec_time = time.time() - start_time
        return QueryResult(
            params=params, data=None, error=str(e),
            row_count=0, execution_time=exec_time
        )


def execute_param_query(conn_str: str, query: str, params: dict[str, Any]) -> pd.DataFrame:
    """Execute a query to get parameter values."""
    try:
        sql, values = substitute_params(query, params)

        conn_with_timeout = f"{conn_str} connect_timeout={CONNECTION_TIMEOUT}"
        with psycopg.connect(conn_with_timeout) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return pd.DataFrame(rows, columns=columns)
        return pd.DataFrame()
    except Exception as e:
        raise Exception(f"Parameter query failed: {e}")


class ThreadedQueryExecutor:
    """Execute queries with multiple parameter sets using threading."""

    def __init__(self, conn_str: str, max_workers: int = 4):
        self.conn_str = conn_str
        self.max_workers = max_workers
        self.results: list[QueryResult] = []
        self.progress_callback = None
        self.stats = ExecutionStats()

    def set_progress_callback(self, callback):
        """Set callback for progress updates: callback(stats: ExecutionStats)."""
        self.progress_callback = callback

    def execute(
        self,
        query: str,
        param_combinations: list[dict[str, Any]],
    ) -> list[QueryResult]:
        """Execute query for all parameter combinations."""
        self.results = []
        total = len(param_combinations)

        self.stats = ExecutionStats(
            total=total,
            completed=0,
            start_time=time.time(),
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    execute_single_query, self.conn_str, query, params
                ): params
                for params in param_combinations
            }

            for future in as_completed(futures):
                result = future.result()
                self.results.append(result)

                # Update stats
                self.stats.completed += 1
                if result.error:
                    self.stats.errors += 1
                else:
                    self.stats.success += 1
                    self.stats.rows_fetched += result.row_count

                self.stats.elapsed_time = time.time() - self.stats.start_time

                # Calculate average and ETA
                if self.stats.completed > 0:
                    self.stats.avg_time_per_query = self.stats.elapsed_time / self.stats.completed
                    remaining = self.stats.total - self.stats.completed
                    self.stats.estimated_remaining = remaining * self.stats.avg_time_per_query

                if self.progress_callback:
                    self.progress_callback(self.stats)

        return self.results

    def execute_with_streaming(
        self,
        query: str,
        param_combinations: list[dict[str, Any]],
        target_conn_str: str,
        target_schema: str,
        target_table: str,
        add_param_columns: bool = True,
    ) -> list[QueryResult]:
        """Execute queries and stream results to target table immediately."""
        self.results = []
        total = len(param_combinations)

        self.stats = ExecutionStats(
            total=total,
            completed=0,
            start_time=time.time(),
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    execute_and_stream,
                    self.conn_str,
                    query,
                    params,
                    target_conn_str,
                    target_schema,
                    target_table,
                    add_param_columns,
                ): params
                for params in param_combinations
            }

            for future in as_completed(futures):
                result = future.result()
                self.results.append(result)

                # Update stats
                self.stats.completed += 1
                if result.error:
                    self.stats.errors += 1
                else:
                    self.stats.success += 1
                    self.stats.rows_fetched += result.row_count
                    self.stats.rows_inserted += result.rows_inserted

                self.stats.elapsed_time = time.time() - self.stats.start_time

                # Calculate average and ETA
                if self.stats.completed > 0:
                    self.stats.avg_time_per_query = self.stats.elapsed_time / self.stats.completed
                    remaining = self.stats.total - self.stats.completed
                    self.stats.estimated_remaining = remaining * self.stats.avg_time_per_query

                if self.progress_callback:
                    self.progress_callback(self.stats)

        return self.results

    def get_combined_results(self) -> pd.DataFrame:
        """Combine all successful results into one DataFrame."""
        dfs = []
        for result in self.results:
            if result.data is not None and not result.data.empty:
                # Add parameters as columns
                df = result.data.copy()
                for key, value in result.params.items():
                    df[f'_param_{key}'] = value
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def get_error_count(self) -> int:
        """Get count of failed executions."""
        return sum(1 for r in self.results if r.error is not None)

    def get_success_count(self) -> int:
        """Get count of successful executions."""
        return sum(1 for r in self.results if r.error is None)

    def get_total_execution_time(self) -> float:
        """Get total execution time."""
        return self.stats.elapsed_time
