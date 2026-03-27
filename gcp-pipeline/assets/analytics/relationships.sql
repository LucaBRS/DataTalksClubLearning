/* @bruin
name: analytics.relationships
type: bq.sql
connection: gcp_conn
depends:
  - staging.marriage_rate
  - staging.divorce_rate
  - staging.age_at_marriage
  - staging.income_quintile
materialization:
  type: table
  strategy: merge
  partition_by: year_date
  cluster_by:
    - country
columns:
  - name: country
    primary_key: true
  - name: year
    primary_key: true
  - name: year_date
  - name: marriage_rate
  - name: divorce_rate
  - name: age_at_marriage_f
  - name: age_at_marriage_m
  - name: income_quintile_f
  - name: income_quintile_m
@bruin */

SELECT
    DATE(m.year, 12, 31) AS year_date,

    m.country,
    m.year,
    m.marriage_rate,
    d.divorce_rate,
    a.age_at_marriage_f,
    a.age_at_marriage_m,
    iq.income_quintile_f,
    iq.income_quintile_m
FROM staging.marriage_rate m
LEFT JOIN staging.divorce_rate d
    ON m.country = d.country AND m.year = d.year
LEFT JOIN staging.age_at_marriage a
    ON m.country = a.country AND m.year = a.year
LEFT JOIN staging.income_quintile iq
    ON m.country = iq.country AND m.year = iq.year


