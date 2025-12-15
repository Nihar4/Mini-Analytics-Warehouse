"""
Configuration for the Mini Analytics Warehouse.

Defines paths, column mappings, and default settings.
"""

from pathlib import Path

# ── Project Paths ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
PARQUET_DIR = PROCESSED_DIR / "events_parquet"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "analytics.duckdb"

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
QUERY_RESULTS_DIR = OUTPUTS_DIR / "query_results"
CHARTS_DIR = OUTPUTS_DIR / "charts"

SQL_DIR = PROJECT_ROOT / "sql"

# ── Column Mapping ───────────────────────────────────────────────────────────
# Maps common raw column names (lowered, stripped) to standard analytical names.
# The ETL will try to match incoming columns to these patterns.
COLUMN_ALIASES = {
    # timestamp-related
    "year": "year",
    "month": "month",
    "day": "day",
    "date": "event_date",
    "timestamp": "event_time",
    "event_time": "event_time",
    "event_timestamp": "event_time",
    "datetime": "event_time",

    # identifiers
    "session id": "session_id",
    "session_id": "session_id",
    "sessionid": "session_id",
    "user_id": "user_id",
    "userid": "user_id",
    "user id": "user_id",
    "visitor_id": "user_id",

    # event / page
    "order": "event_sequence",
    "event_type": "event_type",
    "event type": "event_type",
    "action": "event_type",
    "page": "page_number",
    "page_number": "page_number",

    # product / category
    "page 1 (main category)": "category_id",
    "page 1 main category": "category_id",
    "main_category": "category_id",
    "category": "category_id",
    "page 2 (clothing model)": "product_id",
    "page 2 clothing model": "product_id",
    "clothing_model": "product_id",
    "product_id": "product_id",
    "product id": "product_id",
    "page_id": "page_id",

    # attributes
    "colour": "colour",
    "color": "colour",
    "location": "location",
    "model photography": "model_photography",
    "model_photography": "model_photography",

    # pricing
    "price": "price",
    "price 2": "price_secondary",
    "price_2": "price_secondary",
    "revenue": "price",
    "amount": "price",

    # geography / device
    "country": "country",
    "region": "region",
    "device": "device",
    "device_type": "device",
}

# Columns required for the pipeline to run (at minimum)
REQUIRED_COLUMNS = ["session_id"]

# Nice-to-have columns that enable specific analytics
OPTIONAL_ANALYTICS_COLUMNS = {
    "price": "revenue analytics",
    "country": "geography breakdown",
    "product_id": "top products analysis",
    "category_id": "category analytics",
    "device": "device breakdown",
    "event_type": "event type analytics",
}

# ── Category Labels ──────────────────────────────────────────────────────────
CATEGORY_LABELS = {
    1: "Trousers",
    2: "Skirts",
    3: "Blouses",
    4: "Sale",
}

# ── CSV Parsing Defaults ─────────────────────────────────────────────────────
CSV_SEPARATORS = [",", ";", "\t", "|"]
DEFAULT_ENCODING = "utf-8"
