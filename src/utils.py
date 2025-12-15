"""
Utility helpers for logging, file I/O, and directory management.
"""

import logging
import sys
from pathlib import Path

from src.config import (
    PARQUET_DIR,
    WAREHOUSE_DIR,
    QUERY_RESULTS_DIR,
    CHARTS_DIR,
)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the project-wide logger."""
    logger = logging.getLogger("mini_warehouse")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


def ensure_directories() -> None:
    """Create all output directories if they don't exist."""
    for d in [PARQUET_DIR, WAREHOUSE_DIR, QUERY_RESULTS_DIR, CHARTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def detect_separator(filepath: Path) -> str:
    """Auto-detect the CSV separator by reading the first line."""
    from src.config import CSV_SEPARATORS

    with open(filepath, "r", encoding="utf-8") as f:
        first_line = f.readline()

    best_sep = ","
    best_count = 0
    for sep in CSV_SEPARATORS:
        count = first_line.count(sep)
        if count > best_count:
            best_count = count
            best_sep = sep
    return best_sep
