/* @bruin
name: staging.divorce_rate
type: bq.sql
connection: gcp_conn
depends:
  - load.divorce_rate
materialization:
  type: table
  strategy: merge
columns:
  - name: country
    primary_key: true
  - name: year
    primary_key: true
  - name: divorce_rate
@bruin */

SELECT country, year, divorce_rate
FROM load.divorce_rate
