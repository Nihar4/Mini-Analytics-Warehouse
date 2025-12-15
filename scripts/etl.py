#!/usr/bin/env python3
"""
ETL Pipeline — Load, validate, transform, and write partitioned Parquet.

Usage:
    python scripts/etl.py --input data/raw/events.csv
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.utils import setup_logging, ensure_directories, detect_separator
from src.schema import map_columns, validate_schema
from src.transformations import (
    clean_data,
    build_event_datetime,
    add_derived_columns,
    handle_missing_values,
    write_partitioned_parquet,
)
from src.config import RAW_DIR

logger = setup_logging()


def run_etl(input_path: Path) -> None:
    """Execute the full ETL pipeline on the given CSV file."""

    # ── Step 1: Load raw CSV ─────────────────────────────────────────────
    logger.info(f"Loading CSV from {input_path}")
    if not input_path.exists():
        logger.error(f"File not found: {input_path}")
        sys.exit(1)

    separator = detect_separator(input_path)
    logger.info(f"Detected separator: '{separator}'")

    df = pd.read_csv(input_path, sep=separator, encoding="utf-8", low_memory=False)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    logger.info(f"Raw columns: {list(df.columns)}")

    # ── Step 2: Map column names ─────────────────────────────────────────
    df = map_columns(df)

    # ── Step 3: Validate schema ──────────────────────────────────────────
    if not validate_schema(df):
        logger.error("Schema validation failed. Exiting.")
        sys.exit(1)

    # ── Step 4: Clean data ───────────────────────────────────────────────
    df = clean_data(df)

    # ── Step 5: Build event datetime ─────────────────────────────────────
    df = build_event_datetime(df)

    # ── Step 6: Add derived columns ──────────────────────────────────────
    df = add_derived_columns(df)

    # ── Step 7: Handle missing values ────────────────────────────────────
    df = handle_missing_values(df)

    # ── Step 8: Write partitioned Parquet ────────────────────────────────
    ensure_directories()
    write_partitioned_parquet(df)

    logger.info("ETL pipeline completed successfully.")
    logger.info(f"Final schema: {list(df.columns)}")
    logger.info(f"Final row count: {len(df):,}")
    logger.info(f"Date range: {df['event_date'].min()} to {df['event_date'].max()}")


def main():
    parser = argparse.ArgumentParser(
        description="Mini Analytics Warehouse — ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example:\n  python scripts/etl.py --input data/raw/events.csv",
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=str(RAW_DIR / "events.csv"),
        help="Path to the input CSV file (default: data/raw/events.csv)",
    )
    args = parser.parse_args()
    input_path = Path(args.input)

    # Resolve relative paths from project root
    if not input_path.is_absolute():
        input_path = Path(__file__).resolve().parent.parent / input_path

    run_etl(input_path)


if __name__ == "__main__":
    main()
