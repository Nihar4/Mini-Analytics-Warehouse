# Mini Analytics Warehouse

A mini analytics warehouse built in Python that transforms raw clickstream CSV data into partitioned Parquet datasets, enables fast SQL-based analytics with DuckDB, and implements a custom parallel **MapReduce engine** — all running locally.

---

## Why This Project Matters

Modern analytics teams work with data warehouses like BigQuery, Snowflake, and Redshift — backed by distributed processing frameworks like MapReduce, Apache Spark, and Dataflow. This project recreates that workflow locally:

- **Raw ingestion** → ETL pipeline → **clean analytical schema**
- **Partitioned Parquet** storage (like BigQuery's date-partitioned tables)
- **SQL query layer** via DuckDB (an embedded OLAP database)
- **Materialized aggregate tables** for precomputed metrics
- **Custom MapReduce engine** implementing the Map → Shuffle → Reduce pattern

It demonstrates real-world analytics engineering skills without cloud infrastructure costs.

---

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  Raw CSV    │────▶│  ETL Pipeline   │────▶│  Parquet Files   │
│ (data/raw/) │     │  scripts/etl.py │     │  partitioned by  │
└─────────────┘     └─────────────────┘     │   event_date     │
                           │                └────────┬─────────┘
                    • Column mapping                  │
                    • Type casting          ┌─────────┴──────────────────────┐
                    • Derived columns       │                                │
                    • Data validation       ▼                                ▼
                                   ┌──────────────┐              ┌────────────────────┐
                                   │  DuckDB SQL  │              │  MapReduce Engine  │
                                   │  Warehouse   │              │  (ThreadPool)      │
                                   │              │              │                    │
                                   │ • Aggregate  │              │ • Map phase        │
                                   │   tables     │              │ • Shuffle phase    │
                                   │ • SQL queries│              │ • Reduce phase     │
                                   │ • CSV output │              │ • Chained jobs     │
                                   └──────────────┘              └────────────────────┘
```

---

## Dataset Expectations

Place a CSV file in `data/raw/`. The pipeline auto-detects separators (`,`, `;`, `\t`, `|`) and maps common column names automatically.

### Supported Column Patterns

| Column Type       | Accepted Names                                              |
|-------------------|-------------------------------------------------------------|
| **Timestamp**     | `timestamp`, `event_time`, `date`, or `year`+`month`+`day` |
| **User/Session**  | `session_id`, `user_id`, `visitor_id`                       |
| **Event Type**    | `event_type`, `action`                                      |
| **Product**       | `product_id`, `page 2 (clothing model)`                     |
| **Category**      | `category`, `page 1 (main category)`                        |
| **Price**         | `price`, `revenue`, `amount`                                |
| **Geography**     | `country`, `region`                                         |
| **Device**        | `device`, `device_type`                                     |

### Sample CSV Format

```csv
year;month;day;order;country;session ID;page 1 (main category);page 2 (clothing model);colour;location;model photography;price;price 2;page
2008;4;1;1;29;1;1;A13;1;5;1;28;2;1
2008;4;1;2;29;1;1;A16;1;6;1;33;2;1
```

If a column is missing, the pipeline gracefully skips related analytics — no crashes.

---

## Project Structure

```
mini-analytics-warehouse/
├── data/
│   ├── raw/                        # Drop your CSV here
│   ├── processed/
│   │   └── events_parquet/         # Partitioned Parquet output
│   └── warehouse/
│       └── analytics.duckdb        # Materialized DuckDB database
├── outputs/
│   ├── query_results/              # CSV exports (SQL + MapReduce results)
│   └── charts/                     # PNG charts
├── scripts/
│   ├── etl.py                      # ETL pipeline CLI
│   ├── build_warehouse.py          # Build DuckDB aggregate tables
│   ├── run_queries.py              # Run SQL analytics queries
│   ├── run_mapreduce.py            # Run MapReduce analytics jobs
│   └── analysis.py                 # Interactive analysis with plots
├── sql/
│   ├── total_events_by_day.sql
│   ├── daily_active_users.sql
│   ├── top_event_types.sql
│   ├── top_products.sql
│   ├── revenue_by_category.sql
│   ├── country_device_breakdown.sql
│   ├── hourly_traffic.sql
│   ├── monthly_trends.sql
│   └── session_depth.sql
├── src/
│   ├── __init__.py
│   ├── config.py                   # Paths, column mappings, settings
│   ├── utils.py                    # Logging, file helpers
│   ├── schema.py                   # Column mapping & validation
│   ├── transformations.py          # Data cleaning & Parquet writing
│   ├── warehouse.py                # DuckDB query & aggregate layer
│   ├── mapreduce.py                # MapReduce engine (Map/Shuffle/Reduce)
│   └── mr_jobs.py                  # Analytics jobs for MapReduce engine
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Setup

### Prerequisites

- Python 3.9+
- pip

### Install

```bash
git clone https://github.com/Nihar4/Mini-Analytics-Warehouse.git
cd mini-analytics-warehouse

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### Add Your Data

```bash
cp your-dataset.csv data/raw/events.csv
```

---

## Usage

### Step 1 — Run the ETL Pipeline

```bash
python scripts/etl.py --input data/raw/events.csv
```

- Loads and validates the CSV
- Auto-maps columns to a standard analytical schema
- Creates derived columns: `event_date`, `event_hour`, `event_month`, `day_of_week`, `category_name`
- Writes **partitioned Parquet files** to `data/processed/events_parquet/`

### Step 2 — Build the Warehouse

```bash
python scripts/build_warehouse.py
```

Creates `data/warehouse/analytics.duckdb` with materialized aggregate tables:

| Table              | Content                                |
|--------------------|----------------------------------------|
| `daily_metrics`    | Events, sessions, revenue per day      |
| `top_products`     | Most viewed products with revenue      |
| `country_metrics`  | Activity breakdown by country          |
| `category_metrics` | Events and revenue by product category |

### Step 3 — Run SQL Analytics Queries

```bash
# All queries
python scripts/run_queries.py

# Specific query
python scripts/run_queries.py --query daily_active_users

# More rows
python scripts/run_queries.py --query top_products --show 25
```

Results saved as CSVs in `outputs/query_results/`.

### Step 4 — Run MapReduce Jobs

```bash
# All MapReduce jobs
python scripts/run_mapreduce.py

# Specific job
python scripts/run_mapreduce.py --job top_products

# Compare MapReduce vs DuckDB SQL timings
python scripts/run_mapreduce.py --compare
```

Available MapReduce jobs:

| Job                   | Description                                           |
|-----------------------|-------------------------------------------------------|
| `total_events_by_day` | Count events per day                                  |
| `daily_active_users`  | Unique sessions per day                               |
| `revenue_by_category` | Revenue, count, and avg price per category            |
| `top_products`        | Top products by views, sessions, revenue              |
| `country_breakdown`   | Events and sessions by country                        |
| `session_depth`       | Two-stage chained job: pages-per-session distribution |

### Step 5 — Interactive Analysis with Charts

```bash
python scripts/analysis.py
```

Prints dataset summaries and saves 4 charts to `outputs/charts/`.

---

## Sample Output

### Daily Active Users (SQL)

```
event_date  active_users
2008-04-01           477
2008-04-02           480
2008-04-03           276
```

### Revenue by Category (MapReduce)

```
category_name  total_revenue  event_count  avg_price
     Trousers      2323692.0        49742      46.71
       Skirts      1966199.0        38408      51.19
      Blouses      1554334.0        38577      40.29
         Sale      1403951.0        38747      36.23
```

### MapReduce vs DuckDB Timing

```
Job                          MapReduce   DuckDB SQL  Winner
------------------------------------------------------------
total_events_by_day           1.673s      0.059s    DuckDB
daily_active_users            1.793s      0.056s    DuckDB
revenue_by_category           1.711s      0.053s    DuckDB
country_breakdown             1.824s      0.056s    DuckDB
```

> DuckDB wins single-machine benchmarks due to vectorized columnar execution.
> MapReduce shines at scale across distributed nodes — the same pattern powers
> Hadoop, Google's original MapReduce paper, and Apache Spark.

---

## Key Technical Concepts

### Partitioned Parquet Storage

Data is written as Parquet files partitioned by `event_date`, mirroring BigQuery's date-partitioned tables. Benefits:

- Efficient date-range queries via partition pruning
- Columnar compression for fast aggregations
- Open standard readable by Spark, DuckDB, Pandas, and any analytics tool

### Analytical Schema Design

Raw messy column names are mapped to a clean, consistent schema using a configurable alias dictionary in `src/config.py`. Derived columns (`event_hour`, `event_month`, `day_of_week`, `category_name`) enrich the data for analytics without modifying the raw source.

### DuckDB SQL Analytics

[DuckDB](https://duckdb.org/) is an embedded OLAP database that queries Parquet files directly — no server needed. It provides:

- Full SQL with window functions and analytical aggregations
- Direct Parquet reading via `read_parquet()` with Hive partitioning support
- In-process, zero-configuration
- Vectorized columnar execution — very fast on local datasets

### MapReduce Engine

The custom MapReduce engine in `src/mapreduce.py` implements the classic three-phase pipeline:

```
Phase 1 — MAP  (parallel ThreadPoolExecutor)
  Each data chunk → emit (key, value) pairs
  e.g.  row → ("2008-04-01", 1)
           ↓  165,474 pairs

Phase 2 — SHUFFLE  (in-memory grouping)
  Group all values by key
  {"2008-04-01": [1, 1, 1, ...], "2008-04-02": [...]}
           ↓  135 unique keys

Phase 3 — REDUCE  (aggregation per key)
  sum([1, 1, 1, ...]) → 3181 total events
```

The `session_depth` job demonstrates **two-stage chained MapReduce**:

- **Stage 1:** `(session_id, 1)` → reduce → pages per session per user
- **Stage 2:** `(page_count, 1)` → reduce → distribution of session depths

### Materialized Aggregate Tables

Precomputed aggregate tables in DuckDB mirror how production warehouses use summary tables to serve dashboard queries without re-scanning millions of raw rows on every request.

---

## Inspired by Analytics Warehouses

This project is inspired by the architecture of cloud analytics warehouses like **Google BigQuery**, **Snowflake**, and **Amazon Redshift**, and batch processing systems like **Hadoop MapReduce** and **Apache Spark**. It replicates core engineering patterns — ETL pipelines, partitioned storage, SQL analytics, aggregate tables, and distributed processing — in a lightweight, local Python environment.

It is **not** a replacement for production systems, but demonstrates the same engineering principles at a learnable and portable scale.

---

## Resume Bullets

- Built a mini analytics warehouse in Python that transformed raw clickstream CSV data into partitioned Parquet datasets and enabled SQL-based analytics with DuckDB.
- Implemented a custom parallel MapReduce engine (Map → Shuffle → Reduce) with ThreadPoolExecutor, replicating the core pattern used by Hadoop/Spark for distributed analytics.
- Designed ETL pipelines and analytical schemas for event data, generating aggregate warehouse tables for daily metrics, user activity, and category-level insights.
- Benchmarked MapReduce vs DuckDB SQL across multiple analytics queries to demonstrate trade-offs between distributed batch processing and vectorized columnar execution.

---

## License

MIT
