#!/usr/bin/env python3
"""
Run MapReduce Analytics
=======================
Execute analytics jobs using the local MapReduce engine.

Each job runs through three explicit phases:
  MAP     — parallel threads emit (key, value) pairs from raw event rows
  SHUFFLE — pairs are grouped by key in-memory
  REDUCE  — each key's values are collapsed into a final aggregate

Intermediate outputs (map / shuffle / reduce CSVs) are saved to:
  outputs/mapreduce/<job>_map_output.csv
  outputs/mapreduce/<job>_shuffle_output.csv
  outputs/mapreduce/<job>_reduce_output.csv

Final results are also loaded into DuckDB as tables for SQL access.

Usage:
    python scripts/run_mapreduce.py                          # all jobs
    python scripts/run_mapreduce.py --job revenue_by_category
    python scripts/run_mapreduce.py --compare                # vs DuckDB timing
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import duckdb

from src.config import PARQUET_DIR, QUERY_RESULTS_DIR, OUTPUTS_DIR
from src.utils import setup_logging, ensure_directories
from src.mapreduce import set_output_dir
from src.mr_jobs import JOBS

logger = setup_logging()

MR_OUTPUT_DIR = OUTPUTS_DIR / "mapreduce"
MR_DUCKDB_PATH = OUTPUTS_DIR / "mapreduce" / "mr_results.duckdb"


def load_parquet(parquet_dir: Path) -> pd.DataFrame:
    """Load all partitioned Parquet files into a single DataFrame."""
    files = list(parquet_dir.rglob("*.parquet"))
    if not files:
        logger.error(
            f"No Parquet files found in {parquet_dir}.\n"
            "Run the ETL pipeline first:  python scripts/etl.py"
        )
        sys.exit(1)
    logger.info(f"Loading {len(files)} Parquet partitions...")
    df = pd.read_parquet(parquet_dir)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


def load_into_duckdb(job_name: str, result_df: pd.DataFrame,
                     conn: duckdb.DuckDBPyConnection) -> None:
    """Register a MapReduce result DataFrame as a DuckDB table."""
    table_name = f"mr_{job_name}"
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM result_df")
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Loaded into DuckDB table '{table_name}': {count} rows")


def print_section(title: str) -> None:
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}\n")


def run_all_jobs(df: pd.DataFrame) -> dict:
    """Run every MapReduce job, save outputs, load into DuckDB."""
    ensure_directories()
    MR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    set_output_dir(MR_OUTPUT_DIR)

    conn = duckdb.connect(str(MR_DUCKDB_PATH))
    results = {}

    print_section(f"MapReduce Engine  |  {len(df):,} rows  |  {len(JOBS)} jobs")

    for job_name, job_fn in JOBS.items():
        t0 = time.time()
        out_df = job_fn(df)
        elapsed = time.time() - t0

        if out_df.empty:
            print(f"  ✗  {job_name:<30} SKIPPED (required column missing)")
            continue

        # Save final result CSV
        csv_path = QUERY_RESULTS_DIR / f"mr_{job_name}.csv"
        out_df.to_csv(csv_path, index=False)

        # Load into DuckDB
        load_into_duckdb(job_name, out_df, conn)

        print(f"  ✓  {job_name:<30} {len(out_df):>4} rows  {elapsed:.2f}s")
        results[job_name] = out_df

    # Print tables in DuckDB
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    print(f"\n  DuckDB tables created: {tables}")
    print(f"  DB path: {MR_DUCKDB_PATH}")

    conn.close()

    # Print results
    for name, df_result in results.items():
        print_section(f"Result: {name}")
        print(df_result.head(10).to_string(index=False))

    print(f"\n  Intermediate outputs: {MR_OUTPUT_DIR}/")
    print(f"  Final CSVs:           {QUERY_RESULTS_DIR}/mr_*.csv")
    return results


def run_single_job(df: pd.DataFrame, job_name: str, show: int = 15) -> None:
    """Run one named MapReduce job and show the result."""
    if job_name not in JOBS:
        print(f"Unknown job '{job_name}'.\nAvailable: {list(JOBS.keys())}")
        sys.exit(1)

    ensure_directories()
    MR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    set_output_dir(MR_OUTPUT_DIR)

    print_section(f"MapReduce Job: {job_name}")

    t0 = time.time()
    out_df = JOBS[job_name](df)
    elapsed = time.time() - t0

    if out_df.empty:
        print("No results — required column may be missing.")
        return

    # Save CSV
    csv_path = QUERY_RESULTS_DIR / f"mr_{job_name}.csv"
    out_df.to_csv(csv_path, index=False)

    # Load into DuckDB
    conn = duckdb.connect(str(MR_DUCKDB_PATH))
    load_into_duckdb(job_name, out_df, conn)
    conn.close()

    print(out_df.head(show).to_string(index=False))
    print(f"\n  {len(out_df)} rows  |  {elapsed:.2f}s  |  saved → {csv_path}")
    print(f"  Intermediates: {MR_OUTPUT_DIR}/{job_name}_*.csv")


def compare_with_duckdb(df: pd.DataFrame) -> None:
    """Side-by-side timing: MapReduce engine vs DuckDB SQL."""
    ensure_directories()
    MR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    set_output_dir(MR_OUTPUT_DIR)

    parquet_glob = str(PARQUET_DIR / "**" / "*.parquet")
    conn = duckdb.connect(":memory:")
    conn.execute(
        f"CREATE VIEW events AS "
        f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)"
    )

    benchmarks = [
        {
            "name": "event_count_by_type",
            "mr":  JOBS["event_count_by_type"],
            "sql": "SELECT page_number AS event_type, COUNT(*) AS event_count "
                   "FROM events GROUP BY page_number ORDER BY event_count DESC",
        },
        {
            "name": "country_event_count",
            "mr":  JOBS["country_event_count"],
            "sql": "SELECT country, COUNT(*) AS total_events, "
                   "COUNT(DISTINCT session_id) AS unique_sessions "
                   "FROM events GROUP BY country ORDER BY total_events DESC",
        },
        {
            "name": "revenue_by_category",
            "mr":  JOBS["revenue_by_category"],
            "sql": "SELECT category_name, ROUND(SUM(price),2) AS total_revenue, "
                   "COUNT(*) AS event_count, ROUND(AVG(price),2) AS avg_price "
                   "FROM events GROUP BY category_name ORDER BY total_revenue DESC",
        },
        {
            "name": "total_events_by_day",
            "mr":  JOBS["total_events_by_day"],
            "sql": "SELECT event_date_str AS event_date, COUNT(*) AS total_events "
                   "FROM events GROUP BY event_date_str ORDER BY event_date_str",
        },
    ]

    print_section("MapReduce vs DuckDB SQL — Benchmark")
    print(f"  Dataset: {len(df):,} rows\n")
    print(f"  {'Job':<28} {'MapReduce':>10} {'DuckDB SQL':>12}  Winner")
    print("  " + "─" * 58)

    for b in benchmarks:
        t0 = time.time()
        b["mr"](df)
        mr_time = time.time() - t0

        t0 = time.time()
        conn.execute(b["sql"]).fetchdf()
        sql_time = time.time() - t0

        winner = "MapReduce" if mr_time < sql_time else "DuckDB   "
        print(f"  {b['name']:<28} {mr_time:>8.3f}s {sql_time:>10.3f}s  {winner}")

    conn.close()
    print(
        "\n  Note: DuckDB uses vectorized columnar execution — fastest on a single\n"
        "  machine. MapReduce scales horizontally across distributed worker nodes,\n"
        "  which is where it outperforms SQL engines at petabyte scale."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Mini BigQuery — MapReduce Analytics Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_mapreduce.py\n"
            "  python scripts/run_mapreduce.py --job revenue_by_category\n"
            "  python scripts/run_mapreduce.py --job event_count_by_type --show 20\n"
            "  python scripts/run_mapreduce.py --compare"
        ),
    )
    parser.add_argument(
        "--job", "-j", default=None,
        help=f"Job to run. Choices: {list(JOBS.keys())}",
    )
    parser.add_argument(
        "--show", "-s", type=int, default=15,
        help="Result rows to print (default: 15)",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="Benchmark MapReduce vs DuckDB SQL",
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
