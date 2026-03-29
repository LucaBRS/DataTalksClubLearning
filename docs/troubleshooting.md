# Troubleshooting

## 1. Bruin Python Asset Format
**Problem:** `Failed to create asset` — the `@bruin` block was not recognized.
**Solution:** The block requires opening and closing tags:
```python
"""@bruin
...
@bruin"""
```

---

## 2. Pipeline Not Found
**Problem:** `Failed to find the pipeline this task belongs to`
**Solution:** Create `pipeline.yml` in the project root.

---

## 3. Invalid Asset Type
**Problem:** `Invalid asset type 'duckdb'`
**Solution:** The correct type for DuckDB SQL assets is `duckdb.sql`.

---

## 4. YAML Indentation in `@bruin` Block
**Problem:** `yaml: mapping values are not allowed in this context`
**Solution:** All fields must start at column 0 — no leading spaces before `name`, `type`, etc.

---

## 5. Docker Volume Permissions on Windows
**Problem:** Cannot create `.venv` or `uv.lock` inside a Windows-mounted volume — `Permission denied`.
**Solution:** Use `Dockerfile.bruin` with `RUN /home/bruin/.bruin/uv sync` at build time. The `.venv` is created inside the image, not in the volume.

---

## 6. `uv` Not Found in Container
**Problem:** `uv: not found` when running commands at container startup.
**Solution:** `uv` is embedded in Bruin at `/home/bruin/.bruin/uv` — it is not available as a standalone command in the entrypoint shell PATH.

---

## 7. `eurostat` Module Not Found
**Problem:** `ModuleNotFoundError: No module named 'eurostat'` — Bruin ran assets in isolation ignoring `pyproject.toml`.
**Solution:** Upgrade Bruin from `v0.11.363` to `v0.11.493`, which correctly reads `pyproject.toml` dependencies.

---

## 8. `duckdb.db` Created as a Directory
**Problem:** Docker created `duckdb.db` as a directory instead of a file.
**Solution:** Delete it with `rm -rf duckdb.db` and re-run the pipeline.

---

## 9. DuckDB Lock Conflict
**Problem:** Parallel assets tried to write to the same DuckDB file — `IO Error: Could not set lock`.
**Solution:** Run `bruin run --workers 1` to force sequential execution. Note: the `concurrency` field in `pipeline.yml` only works on Bruin Cloud, not locally.

---

## 10. UNPIVOT Syntax Error
**Problem:** `Parser Error: syntax error at or near "WHERE"`
**Solution:** Correct DuckDB UNPIVOT syntax:
```sql
SELECT country, year, value
FROM table
UNPIVOT (value FOR year IN (col1, col2, ...))
WHERE value IS NOT NULL
```

---

## 11. Year Columns with `_` Prefix
**Problem:** DuckDB renames numeric columns by adding a `_` prefix (e.g. `_2013`).
**Solution:**
```sql
CAST(REPLACE(year, '_', '') AS INTEGER) AS year
```

---

## 12. Dynamic UNPIVOT
**Problem:** Manually listing every year column in UNPIVOT is not scalable.
**Solution:** DuckDB supports `COLUMNS(* EXCLUDE (...))` to dynamically select all columns except the excluded ones:
```sql
UNPIVOT table
ON COLUMNS(* EXCLUDE (freq, indic_de, country))
INTO NAME year VALUE value
```

---

## 13. Income Quintile — F/M Split Location and Aggregation Function
**Problem:** Splitting `income_quintile` into `income_quintile_f` and `income_quintile_m` in the analytics layer caused column duplication due to the double JOIN on `sex = 'F'` and `sex = 'M'`.
**Solution:** Move the pivot to the staging layer using conditional aggregation:
```sql
SELECT
    country,
    CAST(REPLACE(year, '_', '') AS INTEGER) AS year,
    AVG(CASE WHEN sex = 'F' THEN income_quintile END) AS income_quintile_f,
    AVG(CASE WHEN sex = 'M' THEN income_quintile END) AS income_quintile_m
FROM (
    UNPIVOT ingestion.income_quintile
    ON COLUMNS(* EXCLUDE (freq, age, sex, unit, country))
    INTO NAME year VALUE income_quintile
)
WHERE sex IN ('F', 'M')
  AND income_quintile IS NOT NULL
GROUP BY country, year
```
`AVG` is preferred over `MAX` for semantic accuracy — even though the result is identical (only one non-null value per group), `AVG` better communicates the intent.

---

## 14. BigQuery Does Not Support Dynamic UNPIVOT
**Problem:** BigQuery does not support `UNPIVOT COLUMNS(*)` or dynamic column selection like DuckDB. Trying to pivot year columns in SQL fails.
**Solution:** Move the wide→long transformation to the Python load asset using `pandas.melt()` before the data reaches BigQuery:
```python
year_cols = [c for c in df.columns if c not in id_cols]
df = df.melt(id_vars=id_cols, value_vars=year_cols, var_name='year', value_name='value')
```

---

## 15. `strategy: merge` Fails — Table Does Not Exist
**Problem:** `Not found: Table staging.xxx` — Bruin's `strategy: merge` uses BigQuery's native `MERGE INTO`, which requires the target table to already exist.
**Solution:** Define all BigQuery tables in Terraform (`tables.tf`) and run `terraform apply` before the first pipeline run. This pre-creates all staging and analytics tables with the correct schema.

---

## 16. dlt 409 Conflict on Parallel Load Assets
**Problem:** When multiple Python load assets run in parallel, the second asset to start raises a `409 Conflict` error on `create_dataset()` because the dataset was already created by the first.
**Solution:** Python load assets use `type: table` (no explicit `strategy`), which avoids the dlt merge path. SQL staging assets use `strategy: merge` instead — they do not have this issue since they use BigQuery's native MERGE.

---

## 17. BigQuery Schema Change Not Applied by `terraform apply`
**Problem:** After changing a column type in `tables.tf` (e.g. `INTEGER` → `FLOAT`), running `terraform apply` does not update the existing table — BigQuery rejects in-place schema changes for incompatible type conversions.
**Solution:** Force Terraform to destroy and recreate just that table:
```bash
terraform apply -replace=google_bigquery_table.staging_accidents -auto-approve
```
Or delete the table manually first:
```bash
bq rm -f staging.accidents
terraform apply -target=google_bigquery_table.staging_accidents -auto-approve
```

---

## 18. Staging Table Empty After Pipeline Run
**Problem:** A staging table exists but has 0 rows after a successful pipeline run. No error is raised.
**Cause:** The load asset filters on column values (e.g. `nace_r2 == 'TOTAL'`, `age == 'Y15-74'`) that do not exist in the actual dataset — all rows are filtered out silently.
**Solution:** Before finalising filter values, inspect the raw Parquet file in a notebook:
```python
import pandas as pd
df = pd.read_parquet("data/datalake/hours_worked.parquet")
print(df['nace_r2'].unique())
print(df['age'].unique())
print(df['wstatus'].unique())
```
Then update the filters in the load asset accordingly.

---

## 19. `there's no secret with the name 'main_db'`
**Problem:** Local pipeline fails with `there's no secret with the name 'main_db'` when connecting to DuckDB.
**Cause:** The `.bruin.yml` connection name does not match the `connection:` field declared in the asset's `@bruin` block.
**Solution:** Ensure the connection name in `.bruin.yml` matches exactly what the asset declares:
```yaml
# .bruin.yml
connections:
  duckdb:
    - name: "main_db"   # must match the asset
```
```python
# asset @bruin block
connection: main_db
```

---

## 20. Primary Keys Required for Merge Idempotency
**Problem:** Re-running the pipeline inserts duplicate rows instead of updating existing ones.
**Solution:** All `strategy: merge` assets must declare a `primary_key` in the `@bruin` materialization block:
```yaml
materialization:
  type: table
  strategy: merge
  primary_key:
    - country
    - year
```
This tells Bruin to generate a `MERGE INTO ... ON (country, year)` statement, updating matching rows and inserting new ones.
