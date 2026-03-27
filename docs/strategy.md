# Pipeline Strategy

## Overview

The project runs two parallel pipelines:
- **`local-pipeline/`** — local development using DuckDB
- **`gcp-pipeline/`** — production using Google Cloud Platform (GCS + BigQuery)

Both follow the same 4-layer architecture: **ingestion → load → staging → analytics**

---

## Layer Strategies

### Ingestion (`ingestion.*`)

- **Type**: `python` (script, no connection/materialization)
- **What it does**: Downloads raw data from the Eurostat API and saves it as Parquet files
  - Local: saves to `/workspace/data/datalake/`
  - GCP: saves to GCS bucket (`gs://<bucket>/`)
- **Idempotency**: full overwrite on each run (Parquet file is replaced)

### Load (`load.*`)

- **Type**: `python` with `connection` + `materialization: type: table`
- **What it does**: Reads raw Parquet, transforms wide→long format using `pandas.melt()`, returns a DataFrame to Bruin for ingestion into the database
- **Strategy**: default (`type: table`) — Bruin/dlt performs a full replace on each run
- **Why not merge**: dlt (used internally by Bruin for Python assets) has a bug where it raises a terminal error on `create_dataset()` if the dataset already exists, making `strategy: merge` unreliable for Python assets
- **Why melt() here**: BigQuery does not support dynamic column pivoting (`UNPIVOT COLUMNS(*)`), so the wide→long transformation must happen in Python before the data reaches SQL

### Staging (`staging.*`)

- **Type**: `bq.sql` (BigQuery SQL)
- **Strategy**: `merge` on `(country, year)` as primary key
- **What it does**: Applies final transformations (e.g. pivoting `age_at_marriage` by `indic_de` for female/male split, aggregating `income_quintile` by sex)
- **Why merge**: SQL assets use BigQuery's native `MERGE INTO` statement — no dlt involved, no 409 bug. Running the pipeline multiple times will update existing rows rather than duplicating them
- **Prerequisite**: tables must exist before the first merge run. Use `terraform apply` + one initial run with `strategy: create+replace` (or the tables are created on first run automatically by Bruin)

### Analytics (`analytics.relationships`)

- **Type**: `bq.sql`
- **Strategy**: `merge` on `(country, year)` as primary key
- **Partition**: `year_date` (DATE column computed as `DATE(year, 12, 31)`) — enables time-based partition pruning in BigQuery
- **Cluster**: `country` — optimises queries that filter or group by country
- **What it does**: Final JOIN across all staging tables producing one wide analytical table
- **Why DATE for partition**: BigQuery time-based partitioning requires a DATE or TIMESTAMP column; integer year columns require RANGE partitioning (less ergonomic for Looker Studio)

---

## Materialization Strategy Summary

| Layer | Asset type | Strategy | Primary key | Notes |
|-------|-----------|----------|-------------|-------|
| ingestion | python (script) | — | — | Overwrites Parquet on GCS/local |
| load | python + dlt | table (full replace) | — | dlt merge bug workaround |
| staging | bq.sql | merge | (country, year) | BQ native MERGE, idempotent |
| analytics | bq.sql | merge + partition + cluster | (country, year) | Optimised for Looker Studio |

---

## Infrastructure (Terraform)

Terraform manages:
- **GCS bucket**: `<project_id>-data-lake` — stores raw and intermediate Parquet files, with a 30-day lifecycle rule
- **BigQuery datasets**: `load`, `staging`, `analytics` — Bruin creates tables automatically on first run

Terraform does **not** manage individual tables or schemas — Bruin owns table lifecycle within each dataset.

---

## Known Issues / Design Decisions

- **dlt 409 bug**: dlt does not pass `exists_ok=True` when calling `create_dataset()` on BigQuery. If multiple load assets run in parallel, the second asset to start fails because the dataset was already created by the first. Workaround: `load.*` assets use `type: table` (no explicit strategy), relying on the default full-replace behaviour.
- **STRUCT schema issue**: `load_table_from_dataframe()` in some configurations wraps all columns in a STRUCT. Resolved by using a GCS intermediate Parquet file with `load_table_from_uri()` instead.
- **BigQuery staging alias**: staging SQL queries that reference a table with the same name as a column (e.g. `income_quintile`) require a table alias to avoid ambiguity in `CASE WHEN` expressions.
