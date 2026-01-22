"""
MapReduce Engine
================
A parallel MapReduce framework inspired by Google's original MapReduce paper.

Pipeline stages
---------------
  1. MAP     — Each worker thread reads a data shard and emits (key, value) pairs.
  2. SHUFFLE — All pairs are grouped by key in-memory (the "sort & group" step).
  3. REDUCE  — Each key's value list is collapsed into a single aggregate result.

Intermediate outputs
--------------------
  After each phase the engine saves:
    outputs/mapreduce/<job>_map_output.csv     — raw (key, value) pairs from Map
    outputs/mapreduce/<job>_shuffle_output.csv — grouped key → count of values
    outputs/mapreduce/<job>_reduce_output.csv  — final aggregated result

These files make the MapReduce data-flow transparent and inspectable.

Design note
-----------
  Python's multiprocessing cannot pickle closures across process boundaries.
  The engine uses ThreadPoolExecutor for the Map phase so any callable works.

Usage
-----
    from src.mapreduce import run_job, results_to_df
"""

import json
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterator, Any
import pandas as pd
import time

from src.utils import setup_logging

logger = setup_logging()

# Where intermediate MapReduce outputs are written
_MR_OUTPUT_DIR: Path | None = None


def set_output_dir(path: Path) -> None:
    """Configure the directory for intermediate MapReduce outputs."""
    global _MR_OUTPUT_DIR
    _MR_OUTPUT_DIR = path
    path.mkdir(parents=True, exist_ok=True)


# ── Type aliases ──────────────────────────────────────────────────────────────
MapFn    = Callable[[pd.Series], Iterator[tuple]]
ReduceFn = Callable[[Any, list], Any]


# ── Thread-safe map worker ────────────────────────────────────────────────────

def _threaded_map_chunk(args):
    """Apply map_fn to a DataFrame chunk. Returns list of (key, value) pairs."""
    chunk, map_fn = args
    pairs = []
    for _, row in chunk.iterrows():
        for key, value in map_fn(row):
            pairs.append((key, value))
    return pairs


def _save_intermediate(job_name: str, phase: str, data: Any) -> None:
    """Persist an intermediate MapReduce phase output to CSV for inspection."""
    if _MR_OUTPUT_DIR is None:
        return
    path = _MR_OUTPUT_DIR / f"{job_name}_{phase}_output.csv"
    try:
        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        elif isinstance(data, list):
            pd.DataFrame(data, columns=["key", "value"]).to_csv(path, index=False)
        elif isinstance(data, dict):
            rows = [(k, json.dumps(v) if isinstance(v, dict) else v) for k, v in data.items()]
            pd.DataFrame(rows, columns=["key", "reduced_value"]).to_csv(path, index=False)
        logger.info(f"[{job_name}] Saved {phase} output → {path.name}")
    except Exception as e:
        logger.debug(f"Could not save intermediate {phase} output: {e}")


# ── Core Engine ───────────────────────────────────────────────────────────────

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

    Uses ThreadPoolExecutor for the Map phase (works with any callable).
    Saves intermediate Map, Shuffle, and Reduce outputs to CSV if an
    output directory has been configured via set_output_dir().

    Args:
        df:          Input data.
        map_fn:      Function(row) -> Iterator[(key, value)]
        reduce_fn:   Function(key, [values]) -> result
        n_workers:   Parallel threads for Map phase (default: CPU count).
        chunk_size:  Rows per map worker.
        job_name:    Label for logging and output file naming.

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
    _save_intermediate(job_name, "map", all_pairs[:5000])  # sample to keep files small

    # ── Phase 2: SHUFFLE (group by key) ──────────────────────────────────────
    grouped: dict = defaultdict(list)
    for key, value in all_pairs:
        grouped[key].append(value)

    t2 = time.time()
    logger.info(f"[{job_name}] Shuffle done: {len(grouped):,} unique keys ({t2 - t1:.2f}s)")
    # Save shuffle summary (key → value_count)
    shuffle_summary = {k: len(v) for k, v in grouped.items()}
    _save_intermediate(job_name, "shuffle", shuffle_summary)

    # ── Phase 3: REDUCE ───────────────────────────────────────────────────────
    results = {}
    for key, values in grouped.items():
        results[key] = reduce_fn(key, values)

    t3 = time.time()
    logger.info(f"[{job_name}] Reduce done ({t3 - t2:.2f}s) | Total wall: {t3 - t0:.2f}s")
    _save_intermediate(job_name, "reduce", results)

    return results


def results_to_df(results: dict, key_col: str, value_col: str) -> pd.DataFrame:
    """Convert a MapReduce results dict to a sorted DataFrame."""
    df = pd.DataFrame(
        list(results.items()),
        columns=[key_col, value_col],
    )
    return df.sort_values(value_col, ascending=False).reset_index(drop=True)
