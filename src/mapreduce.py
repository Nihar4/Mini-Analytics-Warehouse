"""
MapReduce Engine — A parallel MapReduce framework built on multiprocessing.

Implements the classic Map → Shuffle → Reduce pipeline:
  1. MAP:     Each worker reads a data chunk and emits (key, value) pairs
  2. SHUFFLE: Group all values by key (single-node, in-memory)
  3. REDUCE:  Each worker aggregates values for its assigned keys

Design note:
  Python's multiprocessing requires all functions passed across process
  boundaries to be picklable (i.e., defined at module top level, not as
  closures). Map/Reduce functions in mr_jobs.py are therefore defined as
  top-level functions and passed by reference.

Usage:
    from src.mapreduce import run_job, run_job_threaded
"""

import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import Callable, Iterator, Any
import pandas as pd
import time

from src.utils import setup_logging

logger = setup_logging()


# ── Type aliases ──────────────────────────────────────────────────────────────
MapFn    = Callable[[pd.Series], Iterator[tuple]]
ReduceFn = Callable[[Any, list], Any]


# ── Thread-safe map worker (avoids pickling) ──────────────────────────────────

def _threaded_map_chunk(args):
    """Apply map_fn to a DataFrame chunk. Returns list of (key, value) pairs."""
    chunk, map_fn = args
    pairs = []
    for _, row in chunk.iterrows():
        for key, value in map_fn(row):
            pairs.append((key, value))
    return pairs


# ── Core Engine (threaded — works with any callable) ─────────────────────────

def run_job(
    df: pd.DataFrame,
    map_fn: MapFn,
    reduce_fn: ReduceFn,
    n_workers: int = None,
    chunk_size: int = 10_000,
    job_name: str = "MapReduceJob",
) -> dict:
    """
    Execute a MapReduce job over a pandas DataFrame.

    Uses a ThreadPoolExecutor for the Map phase (no pickling required)
    and processes the Shuffle / Reduce phases in the main thread.

    Args:
        df:          Input data.
        map_fn:      Function(row) -> Iterator[(key, value)]
        reduce_fn:   Function(key, [values]) -> result
        n_workers:   Parallel threads for Map phase (default: CPU count).
        chunk_size:  Rows per map worker.
        job_name:    Label for logging.

    Returns:
        dict mapping key -> reduced result.
    """
    if n_workers is None:
        n_workers = min(mp.cpu_count(), 8)

    t0 = time.time()
    logger.info(
        f"[{job_name}] Starting: {len(df):,} rows, "
        f"{n_workers} threads, chunk={chunk_size:,}"
    )

    # ── Phase 1: MAP (parallel threads) ──────────────────────────────────────
    chunks = [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]
    map_args = [(chunk, map_fn) for chunk in chunks]

    all_pairs = []
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(_threaded_map_chunk, a) for a in map_args]
        for future in as_completed(futures):
            all_pairs.extend(future.result())

    t1 = time.time()
    logger.info(f"[{job_name}] Map done: {len(all_pairs):,} pairs ({t1 - t0:.2f}s)")

    # ── Phase 2: SHUFFLE (group by key) ──────────────────────────────────────
    grouped: dict = defaultdict(list)
    for key, value in all_pairs:
        grouped[key].append(value)

    t2 = time.time()
    logger.info(f"[{job_name}] Shuffle done: {len(grouped):,} unique keys ({t2 - t1:.2f}s)")

    # ── Phase 3: REDUCE (sequential — reduce_fn can be any callable) ─────────
    results = {}
    for key, values in grouped.items():
        results[key] = reduce_fn(key, values)

    t3 = time.time()
    logger.info(f"[{job_name}] Reduce done ({t3 - t2:.2f}s) | Total wall: {t3 - t0:.2f}s")

    return results


def results_to_df(results: dict, key_col: str, value_col: str) -> pd.DataFrame:
    """Convert a MapReduce results dict to a sorted DataFrame."""
    df = pd.DataFrame(
        list(results.items()),
        columns=[key_col, value_col],
    )
    return df.sort_values(value_col, ascending=False).reset_index(drop=True)
