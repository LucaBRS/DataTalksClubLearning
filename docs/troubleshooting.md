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
`AVG` is preferred over `MAX` for semantic accuracy — even though the result is identical (only one non-null value per group), `AVG` better communicates the intent of the aggregation.
