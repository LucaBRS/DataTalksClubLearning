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
    sex,
    age,
    unit,
    CAST(REPLACE(year, '_', '') AS INTEGER) AS year,
    income_quintile
FROM
    (
        UNPIVOT ingestion.income_quintile ON COLUMNS(* EXCLUDE (freq,age,sex,unit,country)) INTO NAME year VALUE income_quintile
    )
WHERE
    income_quintile IS NOT NULL