<div align="center">

# Mini BigQuery — Local Analytics Warehouse

**A production-style analytics warehouse built in Python.**
Ingest raw event data → ETL pipeline → partitioned Parquet storage → SQL analytics with DuckDB → parallel MapReduce engine.

Inspired by Google BigQuery, Apache Hadoop MapReduce, and modern data lakehouse architectures.

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://python.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-OLAP%20Engine-yellow)](https://duckdb.org)
[![Parquet](https://img.shields.io/badge/Storage-Parquet-orange)](https://parquet.apache.org)
[![MapReduce](https://img.shields.io/badge/Processing-MapReduce-green)](#mapreduce-engine)

</div>

---

## What Is This?

A local data engineering project that replicates the core architecture of a cloud analytics warehouse — without any cloud infrastructure.

| Cloud Concept | This Project |
|---|---|
| BigQuery table partitioning | Parquet files partitioned by `event_date` |
| BigQuery SQL analytics | DuckDB embedded OLAP SQL engine |
| Dataflow / Dataproc ETL | Python ETL pipeline with schema mapping |
| Hadoop MapReduce jobs | Custom parallel MapReduce engine in Python |
| Data warehouse tables | DuckDB materialized aggregate tables |
| Query result exports | CSV outputs per query |

The project processes **165,000+ real e-commerce clickstream events** (April–August 2008) through every layer of the stack.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          RAW DATA LAYER                                  │
│   data/raw/events.csv  (CSV, semi-colon delimited, 165K+ rows)           │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │  python scripts/etl.py
                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          ETL PIPELINE  (src/transformations.py)          │
│                                                                          │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────────────────────┐   │
│  │ Column       │   │ Type casting │   │ Derived columns            │   │
│  │ mapping      │──▶│ + validation │──▶│ event_date, event_hour,    │   │
│  │ (auto-detect)│   │              │   │ event_month, category_name │   │
│  └──────────────┘   └──────────────┘   └────────────────────────────┘   │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     PARTITIONED STORAGE LAYER                            │
│                                                                          │
│   data/processed/events_parquet/                                         │
│   ├── event_date_str=2008-04-01/  ← partition 1                         │
│   ├── event_date_str=2008-04-02/  ← partition 2                         │
│   └── ... (135 date partitions)                                          │
│                                                                          │
│   Format: Apache Parquet (columnar, compressed)                          │
│   Partitioning: by event_date (mirrors BigQuery date partitioning)       │
└──────────────┬───────────────────────────────┬───────────────────────────┘
               │                               │
               ▼                               ▼
┌──────────────────────────┐     ┌─────────────────────────────────────────┐
│   DUCKDB SQL LAYER        │     │   MAPREDUCE ENGINE                      │
│   (src/warehouse.py)      │     │   (src/mapreduce.py + src/mr_jobs.py)   │
│                           │     │                                         │
│  analytics.duckdb         │     │   Phase 1 — MAP (ThreadPoolExecutor)    │
│  ├── daily_metrics        │     │     row → emit (key, value) pairs        │
│  ├── top_products         │     │                                         │
│  ├── country_metrics      │     │   Phase 2 — SHUFFLE (group by key)      │
│  └── category_metrics     │     │     {"key": [v1, v2, v3, ...]}          │
│                           │     │                                         │
│  9 reusable SQL files     │     │   Phase 3 — REDUCE (aggregate)          │
│  in sql/                  │     │     key → final result                  │
│                           │     │                                         │
│  python scripts/          │     │   Saves intermediates:                  │
│    run_queries.py         │     │   outputs/mapreduce/*_map_output.csv    │
│    build_warehouse.py     │     │   outputs/mapreduce/*_shuffle_output.csv│
└──────────────────────────┘     │   outputs/mapreduce/*_reduce_output.csv │
                                  └─────────────────────────────────────────┘
```

---

## MapReduce Engine

The core engineering highlight of this project.

### How Map → Shuffle → Reduce works

```
INPUT: 165,474 event rows
       ─────────────────

PHASE 1 — MAP  (8 parallel threads)
  Each thread processes a 10,000-row shard and emits key-value pairs.

  Thread 1:  row → ("Trousers", 46.71)
  Thread 2:  row → ("Skirts",   52.00)
  Thread 3:  row → ("Trousers", 33.00)
  Thread 4:  row → ("Sale",     28.00)
  ...
  Output: 165,474 (key, value) pairs saved to *_map_output.csv

PHASE 2 — SHUFFLE  (group by key)
  All pairs from all threads are grouped by key.

  "Trousers" → [46.71, 33.00, 62.00, ...]   (49,742 values)
  "Skirts"   → [52.00, 67.00, ...]           (38,408 values)
  "Blouses"  → [40.00, 18.00, ...]           (38,577 values)
  "Sale"     → [28.00, 23.00, ...]           (38,747 values)
  Output: shuffle summary saved to *_shuffle_output.csv

PHASE 3 — REDUCE  (aggregate each key's values)
  "Trousers" → sum=2,323,692  count=49,742  avg=46.71
  "Skirts"   → sum=1,966,199  count=38,408  avg=51.19
  "Blouses"  → sum=1,554,334  count=38,577  avg=40.29
  "Sale"     → sum=1,403,951  count=38,747  avg=36.23
  Output: final results saved to *_reduce_output.csv + loaded into DuckDB
```

### Implemented MapReduce Jobs

| Job | Map emits | Reduce computes | Stage |
|-----|-----------|-----------------|-------|
| `event_count_by_type` | `(event_type, 1)` | `sum` | Single |
| `country_event_count` | `(country, {events, session})` | `sum + distinct` | Single |
| `revenue_by_category` | `(category, price)` | `sum/count/avg/min/max` | Single |
| `total_events_by_day` | `(event_date, 1)` | `sum` | Single |
| `daily_active_users` | `(event_date, session_id)` | `len(set)` | Single |
| `top_products` | `(product_id, {count, revenue, session})` | `aggregate` | Single |
| `session_depth` | Stage 1: `(session_id, 1)` → Stage 2: `(depth, 1)` | `sum` × 2 | **Chained** |

---

## Project Structure

```
mini-analytics-warehouse/
├── data/
│   ├── raw/                            # Input CSV files
│   ├── processed/
│   │   └── events_parquet/             # Parquet, partitioned by event_date
│   └── warehouse/
│       └── analytics.duckdb            # DuckDB warehouse database
├── outputs/
│   ├── query_results/                  # SQL + MapReduce result CSVs
│   ├── charts/                         # PNG visualizations
│   └── mapreduce/                      # Intermediate MapReduce phase outputs
│       ├── <job>_map_output.csv        # Raw emitted (key, value) pairs
│       ├── <job>_shuffle_output.csv    # Grouped key → value counts
│       ├── <job>_reduce_output.csv     # Final aggregated result
│       └── mr_results.duckdb           # MapReduce results in DuckDB
├── scripts/
│   ├── etl.py                          # ETL pipeline
│   ├── build_warehouse.py              # Build DuckDB aggregate tables
│   ├── run_queries.py                  # SQL analytics query runner
│   ├── run_mapreduce.py                # MapReduce job runner
│   └── analysis.py                     # Interactive analysis + charts
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
│   ├── config.py                       # Paths, column mappings, settings
│   ├── utils.py                        # Logging, directory helpers
│   ├── schema.py                       # Column auto-mapping and validation
│   ├── transformations.py              # Data cleaning, Parquet writer
│   ├── warehouse.py                    # DuckDB aggregate table builder
│   ├── mapreduce.py                    # MapReduce engine (Map/Shuffle/Reduce)
│   └── mr_jobs.py                      # All MapReduce job definitions
├── requirements.txt
└── .gitignore
```

---

## Setup

```bash
git clone https://github.com/Nihar4/Mini-Analytics-Warehouse.git
cd mini-analytics-warehouse

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## Running the Full Pipeline

### Step 1 — ETL: Ingest and Transform

```bash
python scripts/etl.py --input data/raw/events.csv
```

- Auto-detects CSV separator and maps column names to standard schema
- Builds derived columns: `event_date`, `event_hour`, `event_month`, `day_of_week`, `category_name`
- Writes **135 date-partitioned Parquet files** to `data/processed/events_parquet/`

### Step 2 — Build the SQL Warehouse

```bash
python scripts/build_warehouse.py
```

Creates `data/warehouse/analytics.duckdb` with precomputed aggregate tables:
`daily_metrics`, `top_products`, `country_metrics`, `category_metrics`

### Step 3 — Run SQL Queries

```bash
python scripts/run_queries.py                          # all 9 queries
python scripts/run_queries.py --query daily_active_users
python scripts/run_queries.py --query top_products --show 25
```

### Step 4 — Run MapReduce Jobs

```bash
python scripts/run_mapreduce.py                        # all 7 jobs
python scripts/run_mapreduce.py --job revenue_by_category
python scripts/run_mapreduce.py --job event_count_by_type
python scripts/run_mapreduce.py --job country_event_count
python scripts/run_mapreduce.py --compare              # benchmark vs DuckDB
```

Each job saves three intermediate CSV files showing the Map, Shuffle, and Reduce phases explicitly.

### Step 5 — Analysis and Charts

```bash
python scripts/analysis.py
```

---

## Sample Outputs

### MapReduce: Revenue by Category

```
Map phase:    165,474 (category, price) pairs emitted
Shuffle phase: 4 unique category keys
Reduce phase:  sum / count / avg / min / max per category

category_name  total_revenue  event_count  avg_price
     Trousers    2,323,692.0       49,742      46.71
       Skirts    1,966,199.0       38,408      51.19
      Blouses    1,554,334.0       38,577      40.29
         Sale    1,403,951.0       38,747      36.23
```

### MapReduce: Country Event Count

```
country  total_events  unique_sessions
     29        133,963           19,582
      9         18,003            2,261
     24          4,091              527
```

### MapReduce vs DuckDB Timing

```
Job                          MapReduce   DuckDB SQL  Winner
------------------------------------------------------------
event_count_by_type           1.96s       0.06s    DuckDB
country_event_count           1.72s       0.05s    DuckDB
revenue_by_category           1.93s       0.05s    DuckDB
total_events_by_day           1.67s       0.06s    DuckDB
```

> DuckDB wins locally — vectorized columnar execution is extremely fast on a single machine.
> MapReduce wins at distributed scale: Google processed petabytes using this same
> Map → Shuffle → Reduce pattern. The architecture is identical to Hadoop MapReduce
> and is the conceptual foundation of Apache Spark's RDD transformations.

---

## Key Concepts

### Partitioned Parquet Storage
Data is written as Parquet files partitioned by `event_date`, mirroring BigQuery's date-partitioned table structure. Queries over a date range only read the relevant partitions (partition pruning), not the full dataset.

### MapReduce Pattern
The Map → Shuffle → Reduce pattern was described in Google's 2004 paper *"MapReduce: Simplified Data Processing on Large Clusters"*. This project implements the same three phases locally:
- **Map**: emit `(key, value)` pairs from each row — parallelized across threads
- **Shuffle**: group all values by key — the "sort and group" step
- **Reduce**: apply an aggregation function to each key's value list

Intermediate outputs of all three phases are saved as inspectable CSV files in `outputs/mapreduce/`.

### DuckDB Embedded OLAP
DuckDB is an in-process OLAP database that reads Parquet files directly using vectorized columnar execution. It is used here as both a query layer over the Parquet lake and as a persistent store for MapReduce results.

### Chained MapReduce
The `session_depth` job demonstrates multi-stage MapReduce: the output of Stage 1 (pages per session) becomes the input to Stage 2 (histogram of session depths). This is the same technique used in multi-stage Hadoop jobs and Spark chained transformations.

---

## Inspired By

| System | Pattern borrowed |
|--------|-----------------|
| Google BigQuery | Date-partitioned table storage, SQL analytics layer |
| Google MapReduce (2004 paper) | Map → Shuffle → Reduce pipeline |
| Apache Hadoop | Distributed job execution, intermediate output persistence |
| Apache Spark | Chained transformations, in-memory shuffle |
| Amazon Redshift / Snowflake | Materialized aggregate tables, column-oriented storage |

This project is **not** a replacement for any of these systems. It is a local, single-machine implementation of the same architectural patterns — designed to demonstrate understanding of distributed data processing at a learnable scale.

---

## Resume Bullets

- Engineered a local analytics warehouse in Python implementing Map→Shuffle→Reduce pipelines over 165K+ event rows with intermediate output persistence and DuckDB result loading.
- Built a partitioned Parquet data lake (135 date partitions) with an ETL pipeline featuring auto-detection of CSV schemas, column normalization, and derived analytical columns.
- Designed 7 MapReduce jobs (event counts, country breakdown, revenue aggregation, session depth histogram) and benchmarked against DuckDB SQL to demonstrate architectural trade-offs.
- Implemented chained two-stage MapReduce for session depth analysis, mirroring multi-stage Hadoop job patterns used in production distributed systems.

---

## License

MIT
