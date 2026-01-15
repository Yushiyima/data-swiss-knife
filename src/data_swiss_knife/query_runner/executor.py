"""Threaded query executor for parameterized queries."""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import pandas as pd
import psycopg


@dataclass
class QueryResult:
    """Result of a single query execution."""
    params: dict[str, Any]
    data: pd.DataFrame | None
    error: str | None
    row_count: int


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


def execute_single_query(
    conn_str: str,
    query: str,
    params: dict[str, Any],
) -> QueryResult:
    """Execute a single query with given parameters."""
    try:
        sql, values = substitute_params(query, params)

        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)

                # Check if query returns data
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=columns)
                    return QueryResult(params=params, data=df, error=None, row_count=len(df))
                else:
                    conn.commit()
                    return QueryResult(params=params, data=None, error=None, row_count=cur.rowcount)

    except Exception as e:
        return QueryResult(params=params, data=None, error=str(e), row_count=0)


def execute_param_query(conn_str: str, query: str, params: dict[str, Any]) -> pd.DataFrame:
    """Execute a query to get parameter values."""
    try:
        sql, values = substitute_params(query, params)

        with psycopg.connect(conn_str) as conn:
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

    def set_progress_callback(self, callback):
        """Set callback for progress updates: callback(completed, total)."""
        self.progress_callback = callback

    def execute(
        self,
        query: str,
        param_combinations: list[dict[str, Any]],
    ) -> list[QueryResult]:
        """Execute query for all parameter combinations."""
        self.results = []
        total = len(param_combinations)
        completed = 0

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
                completed += 1

                if self.progress_callback:
                    self.progress_callback(completed, total)

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
