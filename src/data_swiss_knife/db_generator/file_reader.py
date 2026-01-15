"""File reader module for CSV and Excel files."""

from pathlib import Path

import pandas as pd


def read_file(file_path: str | Path, nrows: int | None = None) -> pd.DataFrame:
    """Read CSV or Excel file into a DataFrame.

    Args:
        file_path: Path to the file
        nrows: Number of rows to read (None for all)

    Returns:
        DataFrame with file contents
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path, nrows=nrows)
    elif suffix in (".xlsx", ".xls"):
        return pd.read_excel(file_path, nrows=nrows)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def get_sample_data(file_path: str | Path, n_samples: int = 5) -> pd.DataFrame:
    """Get sample rows from a file for preview."""
    return read_file(file_path, nrows=n_samples)
