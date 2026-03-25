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
  partition_by: year_date
  cluster_by:
    - country
@bruin */

SELECT
    m.country,
    m.year,
    DATE(m.year, 12, 31) AS year_date,
    m.marriage_rate,
    d.divorce_rate,
    a.age_at_marriage,
    iq.income_quintile_f,
    iq.income_quintile_m
FROM staging.marriage_rate m
LEFT JOIN staging.divorce_rate d
    ON m.country = d.country AND m.year = d.year
LEFT JOIN staging.age_at_marriage a
    ON m.country = a.country AND m.year = a.year
LEFT JOIN staging.income_quintile iq
    ON m.country = iq.country AND m.year = iq.year
