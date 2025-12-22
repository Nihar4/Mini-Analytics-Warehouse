"""
MapReduce Jobs — Analytics queries implemented as MapReduce pipelines.

Each job follows the pattern:
  map_fn(row) -> yields (key, value) pairs
  reduce_fn(key, values) -> aggregated result

Run with:
  python scripts/run_mapreduce.py
  python scripts/run_mapreduce.py --job total_events_by_day
"""

import math
from collections import defaultdict
from typing import Iterator, Any

import pandas as pd

from src.mapreduce import run_job, results_to_df


# ─────────────────────────────────────────────────────────────────────────────
# 1. TOTAL EVENTS BY DAY
#    Map:    (event_date, 1)
#    Reduce: sum
# ─────────────────────────────────────────────────────────────────────────────

def job_total_events_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """Count total events per day."""

    def map_fn(row) -> Iterator[tuple]:
        yield (str(row.get("event_date_str", "unknown")), 1)

    def reduce_fn(key, values) -> int:
        return sum(values)

    results = run_job(df, map_fn, reduce_fn, job_name="total_events_by_day")
    out = results_to_df(results, "event_date", "total_events")
    return out.sort_values("event_date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. DAILY ACTIVE USERS
#    Map:    (event_date, session_id)  — note: value is the session, not 1
#    Reduce: count distinct session_ids
# ─────────────────────────────────────────────────────────────────────────────

def job_daily_active_users(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique sessions per day."""

    def map_fn(row) -> Iterator[tuple]:
        yield (str(row.get("event_date_str", "unknown")), row.get("session_id", ""))

    def reduce_fn(key, values) -> int:
        return len(set(values))

    results = run_job(df, map_fn, reduce_fn, job_name="daily_active_users")
    out = results_to_df(results, "event_date", "active_users")
    return out.sort_values("event_date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 3. REVENUE BY CATEGORY
#    Map:    (category_name, price)
#    Reduce: sum, count, avg
# ─────────────────────────────────────────────────────────────────────────────

def job_revenue_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """Compute revenue metrics per category."""
    if "price" not in df.columns:
        return pd.DataFrame()

    def map_fn(row) -> Iterator[tuple]:
        cat = str(row.get("category_name", "Other"))
        try:
            price = float(row.get("price", 0) or 0)
        except (ValueError, TypeError):
            price = 0.0
        yield (cat, price)

    def reduce_fn(key, values) -> dict:
        total = round(sum(values), 2)
        count = len(values)
        avg   = round(total / count, 2) if count else 0
        return {"total_revenue": total, "event_count": count, "avg_price": avg}

    results = run_job(df, map_fn, reduce_fn, job_name="revenue_by_category")

    rows = [
        {
            "category_name": k,
            "total_revenue": v["total_revenue"],
            "event_count":   v["event_count"],
            "avg_price":     v["avg_price"],
        }
        for k, v in results.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("total_revenue", ascending=False)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4. TOP PRODUCTS
#    Map:    (product_id, {"count": 1, "price": price})
#    Reduce: sum counts and prices
# ─────────────────────────────────────────────────────────────────────────────

def job_top_products(df: pd.DataFrame) -> pd.DataFrame:
    """Rank products by view count and revenue."""
    if "product_id" not in df.columns:
        return pd.DataFrame()

    def map_fn(row) -> Iterator[tuple]:
        pid = str(row.get("product_id", "unknown"))
        try:
            price = float(row.get("price", 0) or 0)
        except (ValueError, TypeError):
            price = 0.0
        yield (pid, {"count": 1, "revenue": price, "session": row.get("session_id", "")})

    def reduce_fn(key, values) -> dict:
        total_count   = sum(v["count"] for v in values)
        total_revenue = round(sum(v["revenue"] for v in values), 2)
        unique_sess   = len({v["session"] for v in values})
        avg_price     = round(total_revenue / total_count, 2) if total_count else 0
        return {
            "view_count":       total_count,
            "total_revenue":    total_revenue,
            "unique_sessions":  unique_sess,
            "avg_price":        avg_price,
        }

    results = run_job(df, map_fn, reduce_fn, job_name="top_products")

    rows = [
        {
            "product_id":       k,
            "view_count":       v["view_count"],
            "unique_sessions":  v["unique_sessions"],
            "avg_price":        v["avg_price"],
            "total_revenue":    v["total_revenue"],
        }
        for k, v in results.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("view_count", ascending=False)
        .head(25)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. COUNTRY BREAKDOWN
#    Map:    (country, session_id)
#    Reduce: count events and distinct sessions
# ─────────────────────────────────────────────────────────────────────────────

def job_country_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Events and unique sessions per country."""
    if "country" not in df.columns:
        return pd.DataFrame()

    def map_fn(row) -> Iterator[tuple]:
        yield (str(row.get("country", "unknown")), row.get("session_id", ""))

    def reduce_fn(key, values) -> dict:
        return {"total_events": len(values), "unique_sessions": len(set(values))}

    results = run_job(df, map_fn, reduce_fn, job_name="country_breakdown")

    rows = [
        {
            "country":         k,
            "total_events":    v["total_events"],
            "unique_sessions": v["unique_sessions"],
        }
        for k, v in results.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("total_events", ascending=False)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 6. SESSION DEPTH
#    Phase 1 — Map: (session_id, 1)  Reduce: sum → pages per session
#    Phase 2 — Map: (depth, 1)       Reduce: sum → how many sessions at each depth
#    (Chained MapReduce — two-stage job)
# ─────────────────────────────────────────────────────────────────────────────

def job_session_depth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Two-stage MapReduce:
      Stage 1: count pages per session
      Stage 2: count sessions per depth
    """

    # Stage 1: pages per session
    def map1(row) -> Iterator[tuple]:
        yield (str(row.get("session_id", "unknown")), 1)

    def reduce1(key, values) -> int:
        return sum(values)

    stage1 = run_job(df, map1, reduce1, job_name="session_depth/stage1")
    # stage1 = {session_id: page_count}

    # Stage 2: distribution of session depths
    depth_df = pd.DataFrame(
        [(sid, depth) for sid, depth in stage1.items()],
        columns=["session_id", "pages_per_session"],
    )

    def map2(row) -> Iterator[tuple]:
        yield (int(row["pages_per_session"]), 1)

    def reduce2(key, values) -> int:
        return sum(values)

    stage2 = run_job(depth_df, map2, reduce2, job_name="session_depth/stage2")

    return (
        pd.DataFrame(list(stage2.items()), columns=["pages_per_session", "session_count"])
        .sort_values("pages_per_session")
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Job Registry
# ─────────────────────────────────────────────────────────────────────────────

JOBS = {
    "total_events_by_day":  job_total_events_by_day,
    "daily_active_users":   job_daily_active_users,
    "revenue_by_category":  job_revenue_by_category,
    "top_products":         job_top_products,
    "country_breakdown":    job_country_breakdown,
    "session_depth":        job_session_depth,
}
