/* @bruin
name: staging.age_at_marriage
type: bq.sql
connection: gcp_conn
depends:
  - load.age_at_marriage
materialization:
  type: table
  strategy: merge
columns:
  - name: country
    primary_key: true
  - name: year
    primary_key: true
  - name: age_at_marriage
@bruin */

SELECT country, year, age_at_marriage
FROM load.age_at_marriage
