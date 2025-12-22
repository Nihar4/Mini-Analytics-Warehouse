#!/usr/bin/env python3
"""
Run MapReduce Analytics — Execute analytics jobs using the MapReduce engine.

Reads Parquet data directly and runs parallel map/shuffle/reduce pipelines.
Compare results and timing against the DuckDB SQL layer.

Usage:
    python scripts/run_mapreduce.py                        # run all jobs
    python scripts/run_mapreduce.py --job top_products     # run one job
    python scripts/run_mapreduce.py --compare              # run both MR + SQL and compare timings
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.config import PARQUET_DIR, QUERY_RESULTS_DIR
from src.utils import setup_logging, ensure_directories
from src.mr_jobs import JOBS

logger = setup_logging()


def load_parquet(parquet_dir: Path) -> pd.DataFrame:
    """Load all partitioned Parquet files into a single DataFrame."""
    files = list(parquet_dir.rglob("*.parquet"))
    if not files:
        logger.error(f"No Parquet files in {parquet_dir}. Run: python scripts/etl.py first.")
        sys.exit(1)
    logger.info(f"Loading {len(files)} Parquet files...")
    df = pd.read_parquet(parquet_dir)
    logger.info(f"Loaded {len(df):,} rows")
    return df


def run_all_jobs(df: pd.DataFrame) -> dict:
    """Run every MapReduce job and return results."""
    ensure_directories()
    results = {}

    print(f"\n{'=' * 70}")
    print(f"  MapReduce Analytics — {len(df):,} rows")
    print(f"{'=' * 70}\n")

    for job_name, job_fn in JOBS.items():
        t0 = time.time()
        out_df = job_fn(df)
        elapsed = time.time() - t0

        if out_df.empty:
            print(f"── {job_name}: SKIPPED (required column missing)\n")
            continue

        # Save CSV
        csv_path = QUERY_RESULTS_DIR / f"mr_{job_name}.csv"
        out_df.to_csv(csv_path, index=False)

        print(f"── {job_name}  ({len(out_df)} rows | {elapsed:.2f}s)")
        print(out_df.head(10).to_string(index=False))
        print(f"   → saved to {csv_path}\n")

        results[job_name] = (out_df, elapsed)

    return results


def run_single_job(df: pd.DataFrame, job_name: str, show: int = 10) -> None:
    """Run one named MapReduce job."""
    if job_name not in JOBS:
        print(f"Unknown job '{job_name}'. Available: {list(JOBS.keys())}")
        sys.exit(1)

    ensure_directories()
    t0 = time.time()
    out_df = JOBS[job_name](df)
    elapsed = time.time() - t0

    print(f"\n{'=' * 70}")
    print(f"  MapReduce Job: {job_name}  ({elapsed:.2f}s)")
    print(f"{'=' * 70}\n")

    if out_df.empty:
        print("No results — required column may be missing.")
        return

    print(out_df.head(show).to_string(index=False))
    csv_path = QUERY_RESULTS_DIR / f"mr_{job_name}.csv"
    out_df.to_csv(csv_path, index=False)
    print(f"\n({len(out_df)} total rows — saved to {csv_path})")


def compare_with_duckdb(df: pd.DataFrame) -> None:
    """Run both MapReduce and DuckDB SQL for a few jobs and compare timings."""
    import duckdb

    print(f"\n{'=' * 70}")
    print("  MapReduce vs DuckDB — Timing Comparison")
    print(f"{'=' * 70}\n")

    parquet_glob = str(PARQUET_DIR / "**" / "*.parquet")
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW events AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)

    benchmarks = [
        {
            "name": "total_events_by_day",
            "mr_job": JOBS["total_events_by_day"],
            "sql": "SELECT event_date_str AS event_date, COUNT(*) AS total_events FROM events GROUP BY event_date_str ORDER BY event_date_str",
        },
        {
            "name": "daily_active_users",
            "mr_job": JOBS["daily_active_users"],
            "sql": "SELECT event_date_str AS event_date, COUNT(DISTINCT session_id) AS active_users FROM events GROUP BY event_date_str ORDER BY event_date_str",
        },
        {
            "name": "revenue_by_category",
            "mr_job": JOBS["revenue_by_category"],
            "sql": "SELECT category_name, ROUND(SUM(price),2) AS total_revenue, COUNT(*) AS event_count, ROUND(AVG(price),2) AS avg_price FROM events GROUP BY category_name ORDER BY total_revenue DESC",
        },
        {
            "name": "country_breakdown",
            "mr_job": JOBS["country_breakdown"],
            "sql": "SELECT country, COUNT(*) AS total_events, COUNT(DISTINCT session_id) AS unique_sessions FROM events GROUP BY country ORDER BY total_events DESC",
        },
    ]

    print(f"{'Job':<28} {'MapReduce':>12} {'DuckDB SQL':>12}  {'Winner'}")
    print("-" * 62)

    for b in benchmarks:
        # MapReduce timing
        t0 = time.time()
        b["mr_job"](df)
        mr_time = time.time() - t0

        # DuckDB timing
        t0 = time.time()
        conn.execute(b["sql"]).fetchdf()
        sql_time = time.time() - t0

        winner = "MapReduce" if mr_time < sql_time else "DuckDB   "
        print(f"{b['name']:<28} {mr_time:>10.3f}s {sql_time:>10.3f}s  {winner}")

    conn.close()
    print("\nNote: DuckDB is optimized for columnar SQL — MapReduce shines at")
    print("scale across distributed nodes, not single-machine benchmarks.")


def main():
    parser = argparse.ArgumentParser(
        description="Mini Analytics Warehouse — MapReduce Analytics Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_mapreduce.py\n"
            "  python scripts/run_mapreduce.py --job top_products\n"
            "  python scripts/run_mapreduce.py --compare"
        ),
    )
    parser.add_argument(
        "--job", "-j",
        type=str,
        default=None,
        help=f"Specific job to run. Choices: {list(JOBS.keys())}",
    )
    parser.add_argument(
        "--show", "-s",
        type=int,
        default=10,
        help="Rows to display (default: 10)",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare MapReduce vs DuckDB SQL timings",
    )
    args = parser.parse_args()

    df = load_parquet(PARQUET_DIR)

    if args.compare:
        compare_with_duckdb(df)
    elif args.job:
        run_single_job(df, args.job, show=args.show)
    else:
        run_all_jobs(df)


if __name__ == "__main__":
    main()
