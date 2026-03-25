/* @bruin
name: staging.marriage_rate
type: bq.sql
connection: gcp_conn
depends:
  - load.marriage_rate
materialization:
  type: table
  strategy: merge
columns:
  - name: country
    primary_key: true
  - name: year
    primary_key: true
  - name: marriage_rate
@bruin */

SELECT country, year, marriage_rate
FROM load.marriage_rate
