/* @bruin
name: staging.divorce_rate
type: duckdb.sql
connection: local_duckdb
depends:
  - ingestion.divorce_rate
materialization:
  type: table
@bruin */
SELECT
    country,
    CAST(REPLACE(year, '_', '') AS INTEGER) AS year,
    divorce_rate
FROM
    (
        UNPIVOT ingestion.divorce_rate ON COLUMNS(* EXCLUDE (freq, indic_de, country)) INTO NAME year VALUE divorce_rate
    )
WHERE
    divorce_rate IS NOT NULL