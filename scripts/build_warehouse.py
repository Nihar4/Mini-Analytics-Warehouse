#!/usr/bin/env python3
"""
Build Warehouse — Create DuckDB database with aggregate tables from Parquet data.

Usage:
    python scripts/build_warehouse.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils import setup_logging, ensure_directories
from src.warehouse import get_connection, register_parquet_view, build_aggregate_tables
from src.config import PARQUET_DIR, WAREHOUSE_DB

logger = setup_logging()


def main():
    ensure_directories()

    # Check that Parquet data exists
    parquet_files = list(PARQUET_DIR.rglob("*.parquet"))
    if not parquet_files:
        logger.error(
            f"No Parquet files found in {PARQUET_DIR}. "
            "Run the ETL pipeline first: python scripts/etl.py"
        )
        sys.exit(1)

    logger.info(f"Found {len(parquet_files)} Parquet file(s) in {PARQUET_DIR}")

    # Connect to DuckDB
    conn = get_connection(WAREHOUSE_DB)
    logger.info(f"Connected to DuckDB warehouse: {WAREHOUSE_DB}")

    # Register Parquet as a view
    register_parquet_view(conn)

    # Build aggregate tables
    build_aggregate_tables(conn)

    # Show summary
    tables = conn.execute("SHOW TABLES").fetchall()
    logger.info(f"Warehouse tables: {[t[0] for t in tables]}")

    for table_name in [t[0] for t in tables]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"  {table_name}: {count:,} rows")

    conn.close()
    logger.info("Warehouse build complete.")


if __name__ == "__main__":
    main()
