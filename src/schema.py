"""
Schema mapping and validation for incoming CSV data.

Handles flexible column name matching so the pipeline works across
different CSV formats without manual configuration.
"""

import re
import pandas as pd
from src.config import COLUMN_ALIASES, REQUIRED_COLUMNS, OPTIONAL_ANALYTICS_COLUMNS
from src.utils import setup_logging

logger = setup_logging()


def normalize_column_name(name: str) -> str:
    """Lowercase, strip, and collapse whitespace in a column name."""
    return re.sub(r"\s+", " ", name.strip().lower())


def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename DataFrame columns to standard analytical names using COLUMN_ALIASES.

    Returns a new DataFrame with mapped column names.
    Unmapped columns are converted to snake_case and kept.
    """
    rename_map = {}
    for col in df.columns:
        normalized = normalize_column_name(col)
        if normalized in COLUMN_ALIASES:
            rename_map[col] = COLUMN_ALIASES[normalized]
        else:
            # Convert to snake_case
            snake = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
            rename_map[col] = snake

    df = df.rename(columns=rename_map)

    # Drop duplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()]

    mapped = set(rename_map.values())
    logger.info(f"Mapped {len(rename_map)} columns: {sorted(mapped)}")
    return df


def validate_schema(df: pd.DataFrame) -> bool:
    """
    Check that all required columns are present after mapping.
    Log warnings for missing optional analytics columns.
    Returns True if required columns are present.
    """
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        logger.error(f"Missing required columns: {missing_required}")
        return False

    for col, purpose in OPTIONAL_ANALYTICS_COLUMNS.items():
        if col not in df.columns:
            logger.warning(f"Optional column '{col}' not found — {purpose} will be skipped.")

    logger.info("Schema validation passed.")
    return True


def get_available_analytics(df: pd.DataFrame) -> dict:
    """Return a dict of which optional analytics are available."""
    available = {}
    for col, purpose in OPTIONAL_ANALYTICS_COLUMNS.items():
        available[col] = col in df.columns
    return available
