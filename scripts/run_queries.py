#!/usr/bin/env python3
"""
Run Queries — Execute SQL analytics queries against the Parquet dataset.

Usage:
    python scripts/run_queries.py                        # run all queries
    python scripts/run_queries.py --query daily_active_users  # run one query
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils import setup_logging, ensure_directories
from src.warehouse import get_connection, register_parquet_view, run_sql_file, run_all_queries
from src.config import PARQUET_DIR, SQL_DIR, QUERY_RESULTS_DIR

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(
        description="Mini Analytics Warehouse — Run SQL Queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_queries.py\n"
            "  python scripts/run_queries.py --query daily_active_users\n"
            "  python scripts/run_queries.py --query total_events_by_day --show 20"
        ),
    )
    parser.add_argument(
        "--query", "-q",
        type=str,
        default=None,
        help="Name of a specific query to run (without .sql extension)",
    )
    parser.add_argument(
        "--show", "-s",
        type=int,
        default=10,
        help="Number of result rows to print (default: 10)",
    )
    args = parser.parse_args()

    ensure_directories()

    # Check Parquet data
    parquet_files = list(PARQUET_DIR.rglob("*.parquet"))
    if not parquet_files:
        logger.error(
            f"No Parquet files found in {PARQUET_DIR}. "
            "Run the ETL pipeline first: python scripts/etl.py"
        )
        sys.exit(1)

    # Connect using in-memory DuckDB (reads Parquet directly)
    conn = get_connection(":memory:")
    register_parquet_view(conn, PARQUET_DIR)

    if args.query:
        # Run a single query
        sql_file = SQL_DIR / f"{args.query}.sql"
        if not sql_file.exists():
            available = [f.stem for f in SQL_DIR.glob("*.sql")]
            logger.error(f"Query '{args.query}' not found. Available: {available}")
            sys.exit(1)

        df = run_sql_file(conn, sql_file)
        if not df.empty:
            csv_path = QUERY_RESULTS_DIR / f"{args.query}.csv"
            df.to_csv(csv_path, index=False)
            print(f"\n{'=' * 60}")
            print(f"  Query: {args.query}")
            print(f"{'=' * 60}")
            print(df.head(args.show).to_string(index=False))
            print(f"\n({len(df)} total rows — saved to {csv_path})")
    else:
        # Run all queries
        results = run_all_queries(conn)

        print(f"\n{'=' * 60}")
        print(f"  Query Results Summary")
        print(f"{'=' * 60}")
        for name, df in results.items():
            print(f"\n── {name} ({len(df)} rows) ──")
            print(df.head(args.show).to_string(index=False))
            print()

        print(f"\n{len(results)} queries executed. Results saved to {QUERY_RESULTS_DIR}/")

    conn.close()


if __name__ == "__main__":
    main()
