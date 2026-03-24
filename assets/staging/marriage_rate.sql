/* @bruin
name: staging.marriage_rate
type: duckdb.sql
connection: local_duckdb
depends:
  - ingestion.marriage_rate
materialization:
  type: table
@bruin */
SELECT
    country,
    CAST(REPLACE(year, '_', '') AS INTEGER) AS year,
    marriage_rate
FROM
    (
        UNPIVOT ingestion.marriage_rate ON COLUMNS(* EXCLUDE (freq, indic_de, country)) INTO NAME year VALUE marriage_rate
    )
WHERE
    marriage_rate IS NOT NULL