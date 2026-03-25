/* @bruin
name: staging.income_quintile
type: bq.sql
connection: gcp_conn
depends:
  - load.income_quintile
materialization:
  type: table
@bruin */

SELECT
    t.country,
    t.year,
    AVG(CASE WHEN t.sex = 'F' THEN t.income_quintile END) AS income_quintile_f,
    AVG(CASE WHEN t.sex = 'M' THEN t.income_quintile END) AS income_quintile_m
FROM load.income_quintile AS t
GROUP BY t.country, t.year
ORDER BY t.country, t.year
