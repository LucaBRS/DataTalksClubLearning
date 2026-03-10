/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table


columns:
  - name: report_date
    type: date
    description: Date of the report (aggregation key)
    primary_key: true
    checks:
      - name: not_null

  - name: vendor_id
    type: integer
    description: Taxi vendor identifier (1=Yellow, 2=Green)
    primary_key: true
    checks:
      - name: not_null

  - name: payment_type_name
    type: string
    description: Payment type category
    primary_key: true

  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative

  - name: unique_trip_count
    type: bigint
    description: Number of unique trip IDs
    checks:
      - name: non_negative

  - name: total_revenue
    type: decimal
    description: Total fare amount in dollars
    checks:
      - name: non_negative

  - name: total_tips
    type: decimal
    description: Total tips in dollars
    checks:
      - name: non_negative

  - name: total_amount_charged
    type: decimal
    description: Total charged amount in dollars
    checks:
      - name: non_negative

  - name: total_distance_miles
    type: decimal
    description: Total distance traveled in miles
    checks:
      - name: non_negative

  - name: avg_fare_amount
    type: decimal
    description: Average fare per trip
    checks:
      - name: non_negative

  - name: avg_tip_amount
    type: decimal
    description: Average tip per trip
    checks:
      - name: non_negative

  - name: avg_trip_distance
    type: decimal
    description: Average trip distance in miles
    checks:
      - name: non_negative

  - name: avg_trip_duration_minutes
    type: decimal
    description: Average trip duration in minutes
    checks:
      - name: non_negative

  - name: avg_passenger_count
    type: decimal
    description: Average passengers per trip
    checks:
      - name: non_negative

  - name: avg_speed_mph
    type: decimal
    description: Average trip speed in mph
    checks:
      - name: non_negative

  - name: tips_to_fare_ratio
    type: decimal
    description: Ratio of tips to total fare amount

  - name: report_timestamp
    type: timestamp
    description: When the report was generated

custom_checks:
  - name: daily_revenue_positive
    description: Ensure each day has positive revenue
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM reports.trips_report
      WHERE total_revenue <= 0 AND trip_count > 0
    value: 1

  - name: tips_less_than_total
    description: Ensure tips don't exceed total fare
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM reports.trips_report
      WHERE total_tips > total_revenue
    value: 1

@bruin */

-- Daily trip report aggregating revenue, trips, and operational metrics by vendor and payment type
-- Aggregation level: Day x Vendor x Payment Type

SELECT
  CAST(t.pickup_datetime AS DATE) AS report_date,
  t.vendor_id,
  t.payment_type_name,

  -- Trip volume metrics
  COUNT(*) AS trip_count,
  COUNT(DISTINCT t.trip_id) AS unique_trip_count,

  -- Revenue metrics
  COALESCE(SUM(t.fare_amount), 0) AS total_revenue,
  COALESCE(SUM(t.tip_amount), 0) AS total_tips,
  COALESCE(SUM(t.total_amount), 0) AS total_amount_charged,

  -- Distance metrics
  COALESCE(SUM(t.trip_distance), 0) AS total_distance_miles,

  -- Average per-trip metrics
  COALESCE(AVG(t.fare_amount), 0) AS avg_fare_amount,
  COALESCE(AVG(t.tip_amount), 0) AS avg_tip_amount,
  COALESCE(AVG(t.trip_distance), 0) AS avg_trip_distance,
  COALESCE(AVG(t.trip_duration_minutes), 0) AS avg_trip_duration_minutes,
  COALESCE(AVG(t.passenger_count), 0) AS avg_passenger_count,
  COALESCE(AVG(t.speed_mph), 0) AS avg_speed_mph,

  -- Derived metrics
  CASE
    WHEN COALESCE(SUM(t.fare_amount), 0) > 0
      THEN COALESCE(SUM(t.tip_amount), 0) / COALESCE(SUM(t.fare_amount), 0)
    ELSE 0
  END AS tips_to_fare_ratio,

  CURRENT_TIMESTAMP AS report_timestamp

FROM staging.trips t
WHERE CAST(t.pickup_datetime AS DATE) >= CAST('{{ start_datetime }}' AS DATE)
  AND CAST(t.pickup_datetime AS DATE) < CAST('{{ end_datetime }}' AS DATE)

GROUP BY
  CAST(t.pickup_datetime AS DATE),
  t.vendor_id,
  t.payment_type_name

ORDER BY
  report_date DESC,
  vendor_id,
  payment_type_name;