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

---

## 21. GitHub Actions: Docker Volume Permission Denied on `.gitignore`
**Problem:** `Failed to load the config file at '/workspace/.bruin.yml': open /workspace/.gitignore: permission denied` — Bruin reads `.gitignore` as part of config loading, but the Docker container (running as the `bruin` user) cannot read the file mounted from the Actions runner.
**Cause:** `actions/checkout` creates files owned by the runner user. When Docker mounts `.gitignore` and `.git` as volumes, the container's non-root user does not have read permission on them.
**Solution:** Add a permission fix step before `docker compose up`:
```yaml
- name: Fix file permissions
  run: sudo chmod -R 777 .git .gitignore
```
`sudo` is available on GitHub Actions runners and bypasses ownership restrictions. Safe to use since the runner is an ephemeral, isolated VM.

---

## 22. `GOOGLE_CREDENTIALS` Truncated or Malformed in Docker Container
**Problem:** `ValueError: Could not deserialize key data. ASN.1 parsing error: short data` — the RSA private key inside the credentials is invalid or truncated.
**Cause:** The service account JSON stored in the GitHub Secret has multiple lines (JSON formatting with real newlines). When written to `.env` with `echo`, Docker's `env_file` parser reads only the first line — the rest of the JSON is lost.
**Solution:** The `GOOGLE_CREDENTIALS` secret must be a **single-line minified JSON**. Generate it with:
```bash
python -c "import json; print(json.dumps(json.load(open('service-account.json'))))"
```
The `\n` escape sequences inside the `private_key` field are preserved as literal backslash-n in the minified JSON — `json.loads()` correctly converts them to real newlines when parsing.

---

## 23. Shell Strips Quotes from JSON Secrets (`echo "...${{ secrets.X }}"`)
**Problem:** Debug output shows `GOOGLE_CREDENTIALS starts with: {type: service_account` — the JSON keys have no quotes.
**Cause:** `echo "GOOGLE_CREDENTIALS=${{ secrets.GOOGLE_CREDENTIALS }}"` — GitHub Actions expands `${{ secrets.X }}` first, injecting the raw JSON into the shell command. The shell then interprets the `"` inside the JSON as closing the outer double-quoted string, stripping them.
**Solution:** Pass the secret as an environment variable via `env:` and reference it as a regular shell variable:
```yaml
- name: Create .env
  env:
    GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
  run: echo "GOOGLE_CREDENTIALS=$GOOGLE_CREDENTIALS" >> .env
```
Shell variable expansion (`$GOOGLE_CREDENTIALS`) does not re-parse the value, so quotes are preserved.

---

## 24. `${VAR}` References in `BRUIN_YML` Secret Expanded to Empty by Bash
**Problem:** `.bruin.yml` inside the container has `project_id:` and `service_account_json:` empty after the workflow creates the file.
**Cause:** `echo "${{ secrets.BRUIN_YML }}" > .bruin.yml` — GitHub expands the secret content inline, then bash sees `${GCP_PROJECT_ID}` and `${GOOGLE_CREDENTIALS}` in the resulting string and expands them as shell variables (which are not set on the runner → empty string).
**Solution:** Same `env:` pattern — pass the secret as an environment variable so bash does not re-expand its contents:
```yaml
- name: Create .bruin.yml
  env:
    BRUIN_YML: ${{ secrets.BRUIN_YML }}
  run: echo "$BRUIN_YML" > .bruin.yml
```
The `${GCP_PROJECT_ID}` references inside the file are preserved as literal text and resolved by Bruin at runtime from the container's environment variables.
