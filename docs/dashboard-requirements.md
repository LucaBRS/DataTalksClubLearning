# Dashboard Requirements

## Context

This is a DataTalksClub DE Zoomcamp capstone project analyzing the relationship between
**economic inequality** and **relationship indicators** (marriage, divorce, age at first marriage)
across European countries from 2005 to 2024.

The pipeline ingests data from the Eurostat API, transforms it with Bruin (Python + BigQuery SQL),
and stores the final analytical table in BigQuery.

---

## Data Source

**Table**: `test-data-eng-course.analytics.relationships` (BigQuery)

**Tool**: Looker Studio connected directly to BigQuery

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `country` | STRING | ISO 2-letter country code (e.g. `IT`, `DE`, `FR`) |
| `year` | INTEGER | Year (2005–2024) |
| `year_date` | DATE | DATE(year, 12, 31) — used for time-based partitioning |
| `marriage_rate` | FLOAT | Crude marriage rate — marriages per 1,000 inhabitants |
| `divorce_rate` | FLOAT | Crude divorce rate — divorces per 1,000 inhabitants |
| `age_at_marriage_f` | FLOAT | Mean age at first marriage — women |
| `age_at_marriage_m` | FLOAT | Mean age at first marriage — men |
| `income_quintile_f` | FLOAT | S80/S20 income quintile ratio — women (higher = more inequality) |
| `income_quintile_m` | FLOAT | S80/S20 income quintile ratio — men |

> **Note**: not all country/year combinations have data for all columns (LEFT JOINs in the pipeline).

---

## Dashboard Requirements (DataTalksClub grading)

- Minimum **2 tiles** (charts)
- At least **1 chart showing distribution of categorical data** (e.g. by country)
- At least **1 chart showing distribution across a time axis**
- Charts must have titles and axis labels

---

## Research Questions

The project tries to answer:

1. Do countries with higher income inequality have **higher divorce rates**?
2. How has the **gap between male and female income** evolved alongside marriage and divorce trends?
3. Is the **gap between male and female age at marriage** narrowing over time?

---

## Suggested Charts

| # | Type | X axis | Y axis / Metric | Breakdown |
|---|------|--------|-----------------|-----------|
| 1 | Time series (line) | `year_date` | `marriage_rate`, `divorce_rate` | by `country` (filter) |
| 2 | Scatter plot | `income_quintile_f` | `divorce_rate` | one dot per country, latest year |
| 3 | Bar chart (grouped) | `country` | `age_at_marriage_f` vs `age_at_marriage_m` | side by side |
| 4 | Time series (line) | `year_date` | `income_quintile_f` vs `income_quintile_m` | by `country` (filter) |

Charts 1 + 2 cover the minimum grading requirement (time + categorical).

---

## Filters to add

- **Country selector** (multi-select) — filter all charts by country
- **Year range slider** — filter all charts by year range
