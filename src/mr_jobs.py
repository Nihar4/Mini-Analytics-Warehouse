"""
MapReduce Jobs
==============
Analytics jobs implemented using the local MapReduce engine.

Each job follows the three-phase pattern:
  Map    — row → emit (key, value) pairs
  Shuffle — group all values by key                [handled by engine]
  Reduce  — collapse list of values into one result

Core jobs (required)
--------------------
  event_count_by_type   — count events per page-position / event type
  country_event_count   — count events and unique sessions per country
  revenue_by_category   — total/avg revenue and event count per category

Additional jobs
---------------
  total_events_by_day   — daily event volume
  daily_active_users    — unique sessions per day
  top_products          — top products by views and revenue
  session_depth         — two-stage chained job: pages-per-session distribution

Run:
    python scripts/run_mapreduce.py
    python scripts/run_mapreduce.py --job revenue_by_category
"""

import pandas as pd
from typing import Iterator

from src.mapreduce import run_job, results_to_df


# ─────────────────────────────────────────────────────────────────────────────
# JOB 1 — event_count_by_type
#
#   Map:    row → (event_type_or_page_position, 1)
#   Reduce: sum(values)
#
#   Answers: Which event types / page positions get the most traffic?
# ─────────────────────────────────────────────────────────────────────────────

def _map_event_type(row) -> Iterator[tuple]:
    # Use event_type if available, fall back to page_number (clickstream position)
    key = str(row.get("event_type") or row.get("page_number") or "unknown")
    yield (key, 1)

def _reduce_count(key, values) -> int:
    return sum(values)

def job_event_count_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count total events grouped by event type (or page position).

    Map:    (event_type, 1)
    Reduce: sum → total count per type
    """
    results = run_job(df, _map_event_type, _reduce_count,
                      job_name="event_count_by_type")
    out = results_to_df(results, "event_type", "event_count")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# JOB 2 — country_event_count
#
#   Map:    row → (country, {"events": 1, "session": session_id})
#   Reduce: sum events, count distinct sessions
#
#   Answers: Which countries drive the most traffic?
# ─────────────────────────────────────────────────────────────────────────────

def _map_country(row) -> Iterator[tuple]:
    country = str(row.get("country") or "unknown")
    yield (country, {"events": 1, "session": str(row.get("session_id", ""))})

def _reduce_country(key, values) -> dict:
    total_events    = sum(v["events"] for v in values)
    unique_sessions = len({v["session"] for v in values})
    return {"total_events": total_events, "unique_sessions": unique_sessions}

def job_country_event_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count events and unique sessions per country.

    Map:    (country, {events, session_id})
    Reduce: total_events=sum, unique_sessions=len(set)
    """
    if "country" not in df.columns:
        return pd.DataFrame()
    results = run_job(df, _map_country, _reduce_country,
                      job_name="country_event_count")
    rows = [
        {"country": k, "total_events": v["total_events"],
         "unique_sessions": v["unique_sessions"]}
        for k, v in results.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("total_events", ascending=False)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# JOB 3 — revenue_by_category
#
#   Map:    row → (category_name, price)
#   Reduce: total_revenue=sum, event_count=len, avg_price=mean
#
#   Answers: Which product categories generate the most revenue?
# ─────────────────────────────────────────────────────────────────────────────

def _map_revenue(row) -> Iterator[tuple]:
    cat = str(row.get("category_name") or row.get("category_id") or "Other")
    try:
        price = float(row.get("price", 0) or 0)
    except (ValueError, TypeError):
        price = 0.0
    yield (cat, price)

def _reduce_revenue(key, values) -> dict:
    total   = round(sum(values), 2)
    count   = len(values)
    avg     = round(total / count, 2) if count else 0.0
    minimum = round(min(values), 2)
    maximum = round(max(values), 2)
    return {
        "total_revenue": total,
        "event_count":   count,
        "avg_price":     avg,
        "min_price":     minimum,
        "max_price":     maximum,
    }

def job_revenue_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute revenue metrics per product category.

    Map:    (category_name, price)
    Reduce: sum/count/avg/min/max
    """
    if "price" not in df.columns:
        return pd.DataFrame()
    results = run_job(df, _map_revenue, _reduce_revenue,
                      job_name="revenue_by_category")
    rows = [
        {
            "category_name":  k,
            "total_revenue":  v["total_revenue"],
            "event_count":    v["event_count"],
            "avg_price":      v["avg_price"],
            "min_price":      v["min_price"],
            "max_price":      v["max_price"],
        }
        for k, v in results.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("total_revenue", ascending=False)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# JOB 4 — total_events_by_day
#    Map: (event_date, 1)  |  Reduce: sum
# ─────────────────────────────────────────────────────────────────────────────

def _map_event_date(row) -> Iterator[tuple]:
    yield (str(row.get("event_date_str", "unknown")), 1)

def job_total_events_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """Count total events per day."""
    results = run_job(df, _map_event_date, _reduce_count,
                      job_name="total_events_by_day")
    out = results_to_df(results, "event_date", "total_events")
    return out.sort_values("event_date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# JOB 5 — daily_active_users
#    Map: (event_date, session_id)  |  Reduce: len(set)
# ─────────────────────────────────────────────────────────────────────────────

def _map_date_session(row) -> Iterator[tuple]:
    yield (str(row.get("event_date_str", "unknown")), str(row.get("session_id", "")))

def _reduce_distinct_count(key, values) -> int:
    return len(set(values))

def job_daily_active_users(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique sessions per day."""
    results = run_job(df, _map_date_session, _reduce_distinct_count,
                      job_name="daily_active_users")
    out = results_to_df(results, "event_date", "active_users")
    return out.sort_values("event_date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# JOB 6 — top_products
#    Map: (product_id, {count, revenue, session})  |  Reduce: aggregate
# ─────────────────────────────────────────────────────────────────────────────

def _map_product(row) -> Iterator[tuple]:
    pid = str(row.get("product_id", "unknown"))
    try:
        price = float(row.get("price", 0) or 0)
    except (ValueError, TypeError):
        price = 0.0
    yield (pid, {"count": 1, "revenue": price, "session": str(row.get("session_id", ""))})

def _reduce_product(key, values) -> dict:
    total_count   = sum(v["count"] for v in values)
    total_revenue = round(sum(v["revenue"] for v in values), 2)
    unique_sess   = len({v["session"] for v in values})
    avg_price     = round(total_revenue / total_count, 2) if total_count else 0
    return {
        "view_count":      total_count,
        "total_revenue":   total_revenue,
        "unique_sessions": unique_sess,
        "avg_price":       avg_price,
    }

def job_top_products(df: pd.DataFrame) -> pd.DataFrame:
    """Top products by view count, revenue, and unique sessions."""
    if "product_id" not in df.columns:
        return pd.DataFrame()
    results = run_job(df, _map_product, _reduce_product,
                      job_name="top_products")
    rows = [
        {
            "product_id":      k,
            "view_count":      v["view_count"],
            "unique_sessions": v["unique_sessions"],
            "avg_price":       v["avg_price"],
            "total_revenue":   v["total_revenue"],
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
# JOB 7 — session_depth  (two-stage chained MapReduce)
#
#   Stage 1: Map (session_id, 1) → Reduce sum → pages per session
#   Stage 2: Map (page_count, 1) → Reduce sum → distribution histogram
#
#   This demonstrates chained / multi-stage MapReduce jobs.
# ─────────────────────────────────────────────────────────────────────────────

def _map_session_id(row) -> Iterator[tuple]:
    yield (str(row.get("session_id", "unknown")), 1)

def job_session_depth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Two-stage MapReduce:
      Stage 1: pages per session (session_id → page count)
      Stage 2: distribution of those counts (depth → session count)
    """
    # Stage 1
    stage1 = run_job(df, _map_session_id, _reduce_count,
                     job_name="session_depth/stage1")

    depth_df = pd.DataFrame(
        list(stage1.items()), columns=["session_id", "pages_per_session"]
    )

    # Stage 2
    def _map_depth(row) -> Iterator[tuple]:
        yield (int(row["pages_per_session"]), 1)

    stage2 = run_job(depth_df, _map_depth, _reduce_count,
                     job_name="session_depth/stage2")

    return (
        pd.DataFrame(list(stage2.items()), columns=["pages_per_session", "session_count"])
        .sort_values("pages_per_session")
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Job Registry — used by run_mapreduce.py
# ─────────────────────────────────────────────────────────────────────────────

JOBS = {
    # Core required jobs
    "event_count_by_type":  job_event_count_by_type,
    "country_event_count":  job_country_event_count,
    "revenue_by_category":  job_revenue_by_category,
    # Additional jobs
    "total_events_by_day":  job_total_events_by_day,
    "daily_active_users":   job_daily_active_users,
    "top_products":         job_top_products,
    "session_depth":        job_session_depth,
}
