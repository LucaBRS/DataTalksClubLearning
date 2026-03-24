/* @bruin
name: staging.age_at_marriage
type: duckdb.sql
connection: local_duckdb
depends:
  - ingestion.age_at_marriage
materialization:
  type: table
@bruin */
SELECT
    country,
    CAST(REPLACE(year, '_', '') AS INTEGER) AS year,
    age_at_marriage
FROM
    (
        UNPIVOT ingestion.age_at_marriage ON COLUMNS(* EXCLUDE (freq, indic_de, country)) INTO NAME year VALUE age_at_marriage
    )
WHERE
    age_at_marriage IS NOT NULL