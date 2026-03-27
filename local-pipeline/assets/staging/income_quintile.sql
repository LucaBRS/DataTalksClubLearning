/* @bruin
name: staging.income_quintile
type: duckdb.sql
connection: main_db
depends:
  - load.income_quintile
materialization:
  type: table
@bruin */
SELECT
    country,
    year,
    AVG(CASE WHEN sex = 'F' THEN income_quintile END) AS income_quintile_f,
    AVG(CASE WHEN sex = 'M' THEN income_quintile END) AS income_quintile_m
FROM load.income_quintile
GROUP BY country, year
ORDER BY country, year