#!/usr/bin/env python3
"""
Interactive Analysis Script — Explore the Mini Analytics Warehouse.

Connects to DuckDB, runs core queries, and generates simple plots.
This replaces a Jupyter notebook for easy execution.

Usage:
    python scripts/analysis.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd

from src.config import PARQUET_DIR, WAREHOUSE_DB, CHARTS_DIR
from src.utils import setup_logging, ensure_directories

logger = setup_logging()


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


def main():
    ensure_directories()

    # ── Connect to the warehouse ─────────────────────────────────────────
    print_section("Mini Analytics Warehouse — Interactive Analysis")

    if WAREHOUSE_DB.exists():
        conn = duckdb.connect(str(WAREHOUSE_DB), read_only=True)
        print(f"Connected to warehouse: {WAREHOUSE_DB}")
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        print(f"Available tables: {tables}")
        source = "warehouse"
    else:
        # Fall back to reading Parquet directly
        parquet_files = list(PARQUET_DIR.rglob("*.parquet"))
        if not parquet_files:
            print("ERROR: No warehouse or Parquet data found.")
            print("Run the pipeline first:")
            print("  python scripts/etl.py")
            print("  python scripts/build_warehouse.py")
            sys.exit(1)
        conn = duckdb.connect(":memory:")
        parquet_glob = str(PARQUET_DIR / "**" / "*.parquet")
        conn.execute(f"""
            CREATE VIEW events AS
            SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        """)
        source = "parquet"
        print(f"Connected to Parquet data: {PARQUET_DIR}")

    # ── Dataset Overview ─────────────────────────────────────────────────
    print_section("1. Dataset Overview")

    if source == "warehouse":
        # Use aggregate tables
        overview = conn.execute("SELECT * FROM daily_metrics ORDER BY event_date LIMIT 5").fetchdf()
        total = conn.execute("SELECT SUM(total_events) FROM daily_metrics").fetchone()[0]
        days = conn.execute("SELECT COUNT(*) FROM daily_metrics").fetchone()[0]
    else:
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        days = conn.execute("SELECT COUNT(DISTINCT event_date_str) FROM events").fetchone()[0]

    print(f"Total events: {total:,}")
    print(f"Total days:   {days}")

    # Schema
    cols = conn.execute(
        "DESCRIBE events" if source != "warehouse"
        else f"DESCRIBE {tables[0]}" if tables else "SELECT 1"
    ).fetchdf()
    print(f"\nSchema preview:\n{cols.to_string(index=False)}")

    # ── Daily Activity ───────────────────────────────────────────────────
    print_section("2. Daily Activity (first 15 days)")

    if source == "warehouse":
        daily = conn.execute("""
            SELECT event_date, total_events, unique_sessions
            FROM daily_metrics
            ORDER BY event_date
            LIMIT 15
        """).fetchdf()
    else:
        daily = conn.execute("""
            SELECT event_date_str AS event_date,
                   COUNT(*) AS total_events,
                   COUNT(DISTINCT session_id) AS unique_sessions
            FROM events
            GROUP BY event_date_str
            ORDER BY event_date_str
            LIMIT 15
        """).fetchdf()
    print(daily.to_string(index=False))

    # ── Top Products ─────────────────────────────────────────────────────
    print_section("3. Top 10 Products")

    try:
        if source == "warehouse" and "top_products" in tables:
            products = conn.execute("""
                SELECT * FROM top_products LIMIT 10
            """).fetchdf()
        else:
            products = conn.execute("""
                SELECT product_id, category_name,
                       COUNT(*) AS view_count,
                       ROUND(AVG(price), 2) AS avg_price
                FROM events
                GROUP BY product_id, category_name
                ORDER BY view_count DESC
                LIMIT 10
            """).fetchdf()
        print(products.to_string(index=False))
    except Exception as e:
        print(f"Skipped — {e}")

    # ── Category Breakdown ───────────────────────────────────────────────
    print_section("4. Revenue by Category")

    try:
        if source == "warehouse" and "category_metrics" in tables:
            cats = conn.execute("SELECT * FROM category_metrics").fetchdf()
        else:
            cats = conn.execute("""
                SELECT category_name,
                       COUNT(*) AS total_events,
                       ROUND(SUM(price), 2) AS total_revenue,
                       ROUND(AVG(price), 2) AS avg_price
                FROM events
                GROUP BY category_name
                ORDER BY total_revenue DESC
            """).fetchdf()
        print(cats.to_string(index=False))
    except Exception as e:
        print(f"Skipped — {e}")

    # ── Country Breakdown ────────────────────────────────────────────────
    print_section("5. Country Breakdown (top 10)")

    try:
        if source == "warehouse" and "country_metrics" in tables:
            countries = conn.execute("""
                SELECT * FROM country_metrics LIMIT 10
            """).fetchdf()
        else:
            countries = conn.execute("""
                SELECT country,
                       COUNT(*) AS total_events,
                       COUNT(DISTINCT session_id) AS unique_sessions
                FROM events
                GROUP BY country
                ORDER BY total_events DESC
                LIMIT 10
            """).fetchdf()
        print(countries.to_string(index=False))
    except Exception as e:
        print(f"Skipped — {e}")

    # ── Session Depth ────────────────────────────────────────────────────
    print_section("6. Session Depth Distribution")

    session_depth = conn.execute("""
        SELECT pages_per_session, COUNT(*) AS session_count
        FROM (
            SELECT session_id, COUNT(*) AS pages_per_session
            FROM events
            GROUP BY session_id
        )
        GROUP BY pages_per_session
        ORDER BY pages_per_session
        LIMIT 20
    """).fetchdf()
    print(session_depth.to_string(index=False))

    # ── Generate Charts ──────────────────────────────────────────────────
    print_section("7. Generating Charts")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        CHARTS_DIR.mkdir(parents=True, exist_ok=True)

        # Chart 1: Daily events
        if source == "warehouse":
            chart_data = conn.execute("""
                SELECT event_date, total_events FROM daily_metrics ORDER BY event_date
            """).fetchdf()
        else:
            chart_data = conn.execute("""
                SELECT event_date_str AS event_date, COUNT(*) AS total_events
                FROM events GROUP BY event_date_str ORDER BY event_date_str
            """).fetchdf()

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(range(len(chart_data)), chart_data["total_events"], linewidth=1.5, color="#2196F3")
        ax.set_title("Daily Event Volume", fontsize=14, fontweight="bold")
        ax.set_xlabel("Day Index")
        ax.set_ylabel("Events")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(CHARTS_DIR / "daily_events.png"), dpi=150)
        plt.close()
        print(f"Saved: {CHARTS_DIR / 'daily_events.png'}")

        # Chart 2: Category distribution
        try:
            if source == "warehouse" and "category_metrics" in tables:
                cat_data = conn.execute("SELECT category_name, total_events FROM category_metrics").fetchdf()
            else:
                cat_data = conn.execute("""
                    SELECT category_name, COUNT(*) AS total_events
                    FROM events GROUP BY category_name ORDER BY total_events DESC
                """).fetchdf()

            fig, ax = plt.subplots(figsize=(8, 5))
            colors = ["#2196F3", "#4CAF50", "#FF9800", "#F44336", "#9C27B0"]
            ax.bar(cat_data["category_name"], cat_data["total_events"], color=colors[:len(cat_data)])
            ax.set_title("Events by Category", fontsize=14, fontweight="bold")
            ax.set_ylabel("Events")
            ax.grid(True, alpha=0.3, axis="y")
            fig.tight_layout()
            fig.savefig(str(CHARTS_DIR / "category_distribution.png"), dpi=150)
            plt.close()
            print(f"Saved: {CHARTS_DIR / 'category_distribution.png'}")
        except Exception:
            pass

        # Chart 3: Session depth
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(session_depth["pages_per_session"], session_depth["session_count"], color="#4CAF50")
        ax.set_title("Session Depth Distribution", fontsize=14, fontweight="bold")
        ax.set_xlabel("Pages per Session")
        ax.set_ylabel("Number of Sessions")
        ax.grid(True, alpha=0.3, axis="y")
        fig.tight_layout()
        fig.savefig(str(CHARTS_DIR / "session_depth.png"), dpi=150)
        plt.close()
        print(f"Saved: {CHARTS_DIR / 'session_depth.png'}")

        # Chart 4: Top 10 products
        try:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.barh(products["product_id"].astype(str), products["view_count"], color="#FF9800")
            ax.set_title("Top 10 Products by Views", fontsize=14, fontweight="bold")
            ax.set_xlabel("View Count")
            ax.invert_yaxis()
            ax.grid(True, alpha=0.3, axis="x")
            fig.tight_layout()
            fig.savefig(str(CHARTS_DIR / "top_products.png"), dpi=150)
            plt.close()
            print(f"Saved: {CHARTS_DIR / 'top_products.png'}")
        except Exception:
            pass

        print(f"\nAll charts saved to {CHARTS_DIR}/")

    except ImportError:
        print("matplotlib not installed — skipping charts.")
        print("Install with: pip install matplotlib")

    conn.close()
    print_section("Analysis Complete")


if __name__ == "__main__":
    main()
