/* @bruin
name: staging.divorce_rate
type: bq.sql
connection: gcp_conn
depends:
  - load.divorce_rate
materialization:
  type: table
@bruin */

SELECT country, year, divorce_rate
FROM load.divorce_rate
