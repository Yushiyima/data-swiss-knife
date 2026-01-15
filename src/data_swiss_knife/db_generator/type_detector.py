"""Column type detection and mapping to PostgreSQL types."""

from datetime import datetime

import pandas as pd

# PostgreSQL type mapping
PG_TYPES = {
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "FLOAT": "DOUBLE PRECISION",
    "TEXT": "TEXT",
    "VARCHAR": "VARCHAR(255)",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "NUMERIC": "NUMERIC",
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y.%m.%d",
    "%d.%m.%Y",
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
]


def detect_column_type(series: pd.Series) -> str:
    """Detect the most appropriate PostgreSQL type for a column."""
    # Drop nulls for analysis
    non_null = series.dropna()

    if len(non_null) == 0:
        return "TEXT"

    # Check if already a proper dtype
    dtype = series.dtype

    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"

    if pd.api.types.is_integer_dtype(dtype):
        max_val = non_null.abs().max()
        if max_val <= 2147483647:
            return "INTEGER"
        return "BIGINT"

    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"

    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"

    # For object dtype, try to infer
    if dtype == object:
        # Try boolean
        str_vals = non_null.astype(str).str.lower().unique()
        if set(str_vals).issubset({"true", "false", "1", "0", "yes", "no"}):
            return "BOOLEAN"

        # Try integer
        try:
            converted = pd.to_numeric(non_null, errors="raise")
            if (converted == converted.astype(int)).all():
                max_val = converted.abs().max()
                if max_val <= 2147483647:
                    return "INTEGER"
                return "BIGINT"
        except (ValueError, TypeError):
            pass

        # Try float
        try:
            pd.to_numeric(non_null, errors="raise")
            return "FLOAT"
        except (ValueError, TypeError):
            pass

        # Try date/timestamp
        date_fmt = detect_date_format(non_null)
        if date_fmt:
            if "%H" in date_fmt or "%M" in date_fmt:
                return "TIMESTAMP"
            return "DATE"

        # Check string length for VARCHAR vs TEXT
        max_len = non_null.astype(str).str.len().max()
        if max_len <= 255:
            return "VARCHAR"

    return "TEXT"


def detect_date_format(series: pd.Series) -> str | None:
    """Detect the date format of a string series."""
    sample = series.dropna().head(100)

    for fmt in DATE_FORMATS:
        try:
            for val in sample:
                datetime.strptime(str(val), fmt)
            return fmt
        except ValueError:
            continue

    return None


def analyze_dataframe(df: pd.DataFrame) -> dict:
    """Analyze all columns in a DataFrame and return type info."""
    result = {}

    for col in df.columns:
        detected_type = detect_column_type(df[col])
        date_format = None

        if detected_type in ("DATE", "TIMESTAMP"):
            date_format = detect_date_format(df[col])

        result[col] = {
            "detected_type": detected_type,
            "pg_type": PG_TYPES.get(detected_type, "TEXT"),
            "date_format": date_format,
            "sample_values": df[col].dropna().head(3).tolist(),
            "null_count": df[col].isna().sum(),
            "unique_count": df[col].nunique(),
        }

    return result
