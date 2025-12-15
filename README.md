# Mini Analytics Warehouse

A mini analytics warehouse built in Python that transforms raw clickstream CSV data into partitioned Parquet datasets and enables fast SQL-based analytics with DuckDB — all running locally.

---

## Why This Project Matters

Modern analytics teams work with data warehouses like BigQuery, Snowflake, and Redshift. This project recreates that workflow locally:

- **Raw ingestion** → ETL pipeline → **clean analytical schema**
- **Partitioned Parquet** storage (like BigQuery's clustered tables)
- **SQL query layer** via DuckDB (an embedded OLAP database)
- **Materialized aggregate tables** for precomputed metrics

It demonstrates real-world analytics engineering skills without cloud infrastructure costs.

---

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Raw CSV     │────▶│  ETL Pipeline   │────▶│  Parquet Files   │────▶│  DuckDB     │
│  (data/raw/) │     │  (scripts/etl)  │     │  (partitioned    │     │  Warehouse  │
│              │     │                 │     │   by event_date) │     │  + SQL      │
└─────────────┘     └─────────────────┘     └──────────────────┘     └─────────────┘
                           │                                                │
                    • Column mapping                                 • Aggregate tables
                    • Type casting                                   • Reusable SQL
                    • Derived columns                                • CSV exports
                    • Data validation                                • Charts
```

---

## Dataset Expectations

Place a CSV file in `data/raw/`. The pipeline auto-detects separators (`,`, `;`, `\t`, `|`) and maps common column names.

### Supported Column Patterns

| Column Type      | Accepted Names                                             |
| ---------------- | ---------------------------------------------------------- |
| **Timestamp**    | `timestamp`, `event_time`, `date`, or `year`+`month`+`day` |
| **User/Session** | `session_id`, `user_id`, `visitor_id`                      |
| **Event Type**   | `event_type`, `action`                                     |
| **Product**      | `product_id`, `page 2 (clothing model)`                    |
| **Category**     | `category`, `page 1 (main category)`                       |
| **Price**        | `price`, `revenue`, `amount`                               |
| **Geography**    | `country`, `region`                                        |
| **Device**       | `device`, `device_type`                                    |

### Sample CSV Format

```csv
year;month;day;order;country;session ID;page 1 (main category);page 2 (clothing model);colour;location;model photography;price;price 2;page
2008;4;1;1;29;1;1;A13;1;5;1;28;2;1
2008;4;1;2;29;1;1;A16;1;6;1;33;2;1
```

If a column isn't present, the pipeline gracefully skips the related analytics — no crashes.

---

## Project Structure

```
mini-analytics-warehouse/
├── data/
│   ├── raw/                    # Drop your CSV here
│   ├── processed/
│   │   └── events_parquet/     # Partitioned Parquet output
│   └── warehouse/
│       └── analytics.duckdb    # Materialized DuckDB database
├── outputs/
│   ├── query_results/          # CSV exports of query results
│   └── charts/                 # PNG charts
├── scripts/
│   ├── etl.py                  # ETL pipeline CLI
│   ├── build_warehouse.py      # Build DuckDB aggregate tables
│   ├── run_queries.py          # Run SQL analytics queries
│   └── analysis.py             # Interactive analysis with plots
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
│   ├── config.py               # Paths, column mappings, settings
│   ├── utils.py                # Logging, file helpers
│   ├── schema.py               # Column mapping & validation
│   ├── transformations.py      # Data cleaning & Parquet writing
│   └── warehouse.py            # DuckDB query & aggregate layer
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
git clone https://github.com/yourusername/mini-analytics-warehouse.git
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

### Step 1: Run the ETL Pipeline

```bash
python scripts/etl.py --input data/raw/events.csv
```

This will:

- Load and validate the CSV
- Map columns to a standard analytical schema
- Clean data and create derived columns (`event_date`, `event_hour`, `event_month`, `day_of_week`)
- Write **partitioned Parquet files** to `data/processed/events_parquet/`

### Step 2: Build the Warehouse

```bash
python scripts/build_warehouse.py
```

This creates a **DuckDB database** at `data/warehouse/analytics.duckdb` with materialized aggregate tables:

- `daily_metrics` — events, sessions, revenue per day
- `top_products` — most viewed products
- `country_metrics` — activity by country
- `category_metrics` — activity and revenue by category

### Step 3: Run Analytical Queries

```bash
# Run all queries
python scripts/run_queries.py

# Run a specific query
python scripts/run_queries.py --query daily_active_users

# Show more rows
python scripts/run_queries.py --query top_products --show 25
```

Results are saved as CSVs in `outputs/query_results/`.

### Step 4: Interactive Analysis (optional)

```bash
python scripts/analysis.py
```

Runs a guided analysis with data summaries and generates charts in `outputs/charts/`.

---

## Sample Output

### Daily Metrics

```
 event_date  total_events  unique_sessions  total_revenue
 2008-04-01          1565              165       50432.00
 2008-04-02          1482              198       47281.00
 2008-04-03          1390              178       44170.00
```

### Top Products

```
 product_id  category_name  view_count  unique_sessions  avg_price
        B2        Skirts        4521            2103       57.00
       A13       Trousers       3892            1847       28.00
```

---

## Key Technical Concepts

### Partitioned Parquet Storage

Data is stored as Parquet files partitioned by `event_date`, similar to how BigQuery partitions tables by date. This enables:

- Efficient date-range queries (partition pruning)
- Columnar compression for fast aggregations
- Standard open format readable by any analytics tool

### Analytical Schema Design

Raw messy column names are mapped to a clean, consistent schema using a configurable alias dictionary. Derived columns (`event_hour`, `event_month`, `day_of_week`, `category_name`) enrich the data for analytics.

### DuckDB SQL Analytics

[DuckDB](https://duckdb.org/) is an embedded OLAP database that queries Parquet files directly — no server needed. It provides:

- Full SQL support with analytical functions
- Direct Parquet and CSV reading
- In-process, zero-configuration
- Performance comparable to dedicated columnar databases for local datasets

### Local Warehouse Workflow

The materialized aggregate tables pattern mirrors how production warehouses precompute common queries into summary tables for dashboard performance.

---

## Inspired by Analytics Warehouses

This project is inspired by the architecture of cloud analytics warehouses like **Google BigQuery**, **Snowflake**, and **Amazon Redshift**. It replicates key patterns — ETL pipelines, partitioned storage, SQL analytics, and aggregate tables — in a lightweight, local Python environment.

It is **not** a replacement for production warehouses but demonstrates the same engineering principles at a learnable scale.

---

## License

MIT
