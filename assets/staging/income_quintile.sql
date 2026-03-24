/* @bruin
name: staging.income_quintile
type: duckdb.sql
connection: local_duckdb
depends:
  - ingestion.income_quintile
materialization:
  type: table
@bruin */
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
ORDER BY country, year;