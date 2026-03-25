/* @bruin
name: staging.age_at_marriage
type: bq.sql
connection: gcp_conn
depends:
  - load.age_at_marriage
materialization:
  type: table
@bruin */

SELECT country, year, age_at_marriage
FROM load.age_at_marriage
