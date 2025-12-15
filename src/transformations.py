"""
Data transformations: cleaning, type casting, derived columns, and Parquet output.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from src.config import PARQUET_DIR, CATEGORY_LABELS
from src.utils import setup_logging

logger = setup_logging()


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning: drop full-duplicate rows, strip string columns."""
    initial_rows = len(df)
    df = df.drop_duplicates()
    dropped = initial_rows - len(df)
    if dropped:
        logger.info(f"Dropped {dropped} duplicate rows.")

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    return df


def build_event_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create an event_date column from available date components.

    Handles two cases:
      1. Separate year / month / day columns
      2. An existing event_time / event_date timestamp column
    """
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
        df["event_date"] = df["event_time"].dt.date
    elif {"year", "month", "day"}.issubset(df.columns):
        df["event_date"] = pd.to_datetime(
            df[["year", "month", "day"]].rename(
                columns={"year": "year", "month": "month", "day": "day"}
            ),
            errors="coerce",
        ).dt.date
        # Create a full timestamp at midnight for downstream use
        df["event_time"] = pd.to_datetime(df["event_date"])
    else:
        logger.warning("No date columns found — event_date will be null.")
        df["event_date"] = pd.NaT
        df["event_time"] = pd.NaT

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add analytical derived columns from event_time."""
    if "event_time" in df.columns and df["event_time"].notna().any():
        ts = pd.to_datetime(df["event_time"], errors="coerce")
        df["event_hour"] = ts.dt.hour
        df["event_month"] = ts.dt.to_period("M").astype(str)
        df["day_of_week"] = ts.dt.day_name()
    else:
        df["event_hour"] = None
        df["event_month"] = None
        df["day_of_week"] = None

    # Map category IDs to labels if the column exists
    if "category_id" in df.columns:
        df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce")
        df["category_name"] = (
            df["category_id"]
            .map(CATEGORY_LABELS)
            .fillna("Other")
        )

    # Ensure price is numeric
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "price_secondary" in df.columns:
        df["price_secondary"] = pd.to_numeric(df["price_secondary"], errors="coerce")

    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill or flag missing values sensibly."""
    # Numeric columns: fill NaN with 0 for price-like fields
    for col in ["price", "price_secondary"]:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count:
                logger.info(f"Filling {null_count} null values in '{col}' with 0.")
                df[col] = df[col].fillna(0)

    # Categorical columns: fill with 'unknown'
    for col in ["country", "device", "event_type", "colour", "location"]:
        if col in df.columns:
            df[col] = df[col].fillna("unknown").astype(str)

    return df


def write_partitioned_parquet(df: pd.DataFrame, output_dir: Path = PARQUET_DIR) -> None:
    """
    Write the DataFrame as Parquet files partitioned by event_date.
    Uses PyArrow for efficient columnar storage.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert event_date to string for partitioning
    df = df.copy()
    df["event_date_str"] = df["event_date"].astype(str)

    table = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["event_date_str"],
    )

    partition_count = df["event_date_str"].nunique()
    logger.info(
        f"Wrote {len(df):,} rows to partitioned Parquet "
        f"({partition_count} partitions) in {output_dir}"
    )
