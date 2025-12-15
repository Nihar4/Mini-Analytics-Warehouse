"""
DuckDB warehouse layer: read Parquet, run SQL, build aggregate tables.
"""

import duckdb
import pandas as pd
from pathlib import Path

from src.config import PARQUET_DIR, WAREHOUSE_DB, SQL_DIR, QUERY_RESULTS_DIR
from src.schema import get_available_analytics
from src.utils import setup_logging

logger = setup_logging()


def get_connection(db_path=WAREHOUSE_DB) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to the warehouse database file."""
    if isinstance(db_path, Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    return conn


def register_parquet_view(conn: duckdb.DuckDBPyConnection, parquet_dir: Path = PARQUET_DIR) -> None:
    """Register the partitioned Parquet dataset as a view called 'events'."""
    parquet_glob = str(parquet_dir / "**" / "*.parquet")
    conn.execute(f"""
        CREATE OR REPLACE VIEW events AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)
    count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    logger.info(f"Registered 'events' view with {count:,} rows from {parquet_dir}")


def run_sql_file(conn: duckdb.DuckDBPyConnection, sql_path: Path) -> pd.DataFrame:
    """Execute a SQL file and return results as a DataFrame."""
    sql = sql_path.read_text().strip()
    try:
        result = conn.execute(sql).fetchdf()
        logger.info(f"Query '{sql_path.name}' returned {len(result)} rows.")
        return result
    except duckdb.Error as e:
        logger.warning(f"Query '{sql_path.name}' failed: {e}")
        return pd.DataFrame()


def run_all_queries(conn: duckdb.DuckDBPyConnection, sql_dir: Path = SQL_DIR) -> dict:
    """Run every .sql file in sql_dir, save results to CSV, return dict of DataFrames."""
    QUERY_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    # Check which columns are available to skip inapplicable queries
    try:
        columns = [row[0] for row in conn.execute("DESCRIBE events").fetchall()]
    except Exception:
        columns = []

    skip_rules = {
        "revenue_by_category.sql": "price" in columns,
        "top_products.sql": "product_id" in columns,
        "country_device_breakdown.sql": "country" in columns,
    }

    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        logger.warning(f"No SQL files found in {sql_dir}")
        return results

    for sql_file in sql_files:
        # Check skip rules
        if sql_file.name in skip_rules and not skip_rules[sql_file.name]:
            logger.info(f"Skipping '{sql_file.name}' — required column not available.")
            continue

        df = run_sql_file(conn, sql_file)
        if not df.empty:
            query_name = sql_file.stem
            csv_path = QUERY_RESULTS_DIR / f"{query_name}.csv"
            df.to_csv(csv_path, index=False)
            results[query_name] = df
            logger.info(f"Saved results to {csv_path}")

    return results


def build_aggregate_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create materialized aggregate tables in the DuckDB database.
    These persist across sessions for fast analytical access.
    """
    # Check available columns
    try:
        columns = [row[0] for row in conn.execute("DESCRIBE events").fetchall()]
    except Exception:
        logger.error("Cannot describe events view — skipping aggregate build.")
        return

    # ── daily_metrics ────────────────────────────────────────────────────
    revenue_col = ", ROUND(SUM(price), 2) AS total_revenue" if "price" in columns else ""
    conn.execute(f"""
        CREATE OR REPLACE TABLE daily_metrics AS
        SELECT
            event_date_str AS event_date,
            COUNT(*)       AS total_events,
            COUNT(DISTINCT session_id) AS unique_sessions
            {revenue_col}
        FROM events
        GROUP BY event_date_str
        ORDER BY event_date_str
    """)
    logger.info("Built aggregate table: daily_metrics")

    # ── top_products ─────────────────────────────────────────────────────
    if "product_id" in columns:
        revenue_product = ", ROUND(SUM(price), 2) AS total_revenue" if "price" in columns else ""
        conn.execute(f"""
            CREATE OR REPLACE TABLE top_products AS
            SELECT
                product_id,
                COUNT(*)               AS view_count,
                COUNT(DISTINCT session_id) AS unique_sessions
                {revenue_product}
            FROM events
            GROUP BY product_id
            ORDER BY view_count DESC
            LIMIT 50
        """)
        logger.info("Built aggregate table: top_products")
    else:
        logger.info("Skipping top_products — no product_id column.")

    # ── country_device_metrics ───────────────────────────────────────────
    if "country" in columns:
        conn.execute("""
            CREATE OR REPLACE TABLE country_metrics AS
            SELECT
                country,
                COUNT(*)               AS total_events,
                COUNT(DISTINCT session_id) AS unique_sessions
            FROM events
            GROUP BY country
            ORDER BY total_events DESC
        """)
        logger.info("Built aggregate table: country_metrics")
    else:
        logger.info("Skipping country_metrics — no country column.")

    # ── category_metrics ─────────────────────────────────────────────────
    if "category_name" in columns:
        revenue_cat = ", ROUND(SUM(price), 2) AS total_revenue" if "price" in columns else ""
        conn.execute(f"""
            CREATE OR REPLACE TABLE category_metrics AS
            SELECT
                category_name,
                COUNT(*)               AS total_events,
                COUNT(DISTINCT session_id) AS unique_sessions
                {revenue_cat}
            FROM events
            GROUP BY category_name
            ORDER BY total_events DESC
        """)
        logger.info("Built aggregate table: category_metrics")

    logger.info("All aggregate tables built successfully.")
