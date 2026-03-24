/* @bruin
name: analytics.relationships
type: duckdb.sql
connection: local_duckdb
depends:
  - staging.marriage_rate
  - staging.divorce_rate
  - staging.age_at_marriage
  - staging.income_quintile
materialization:
  type: table
@bruin */

SELECT
    m.country,
    m.year,
    m.marriage_rate,
    d.divorce_rate,
    a.age_at_marriage,
    f.income_quintile AS income_quintile_f,
    mo.income_quintile AS income_quintile_m
FROM staging.marriage_rate m
LEFT JOIN staging.divorce_rate d
    ON m.country = d.country AND m.year = d.year
LEFT JOIN staging.age_at_marriage a
    ON m.country = a.country AND m.year = a.year
LEFT JOIN staging.income_quintile f
    ON m.country = f.country AND m.year = f.year AND f.sex = 'F'
LEFT JOIN staging.income_quintile mo
    ON m.country = mo.country AND m.year = mo.year AND mo.sex = 'M'

order by m.country, m.year
