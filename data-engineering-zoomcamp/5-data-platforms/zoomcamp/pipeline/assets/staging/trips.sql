/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

custom_checks:
  - name: row_count_positive
    description: Ensure that the staging table has at least one row
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1

@bruin */

-- Staging: Clean, deduplicate, and enrich NYC taxi trip data
--
-- This layer:
-- 1. Deduplicates ingestion records (append strategy can create duplicates)
-- 2. Validates and cleans data (removes nulls, invalid values)
-- 3. Enriches with payment type lookup
-- 4. Adds calculated metrics

WITH deduplicated_trips AS (
  -- Deduplicate by picking the most recent extraction of each trip
  SELECT * EXCLUDE (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY trip_id
        ORDER BY extracted_at DESC
      ) AS rn
    FROM ingestion.trips
    WHERE pickup_datetime >= CAST('{{ start_datetime }}' AS TIMESTAMP)
      AND pickup_datetime < CAST('{{ end_datetime }}' AS TIMESTAMP)
  ) s
  WHERE rn = 1
),

cleaned_trips AS (
  -- Validate data quality and remove invalid records
  SELECT
    t.trip_id,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,

    -- Validate passenger count (must be positive)
    CASE
      WHEN t.passenger_count > 0 THEN t.passenger_count
      ELSE 1
    END AS passenger_count,

    -- Validate trip distance (must be non-negative)
    CASE
      WHEN t.trip_distance >= 0 THEN t.trip_distance
      ELSE NULL
    END AS trip_distance,

    -- Validate fare amount (must be non-negative)
    CASE
      WHEN t.fare_amount >= 0 THEN t.fare_amount
      ELSE NULL
    END AS fare_amount,

    -- Validate tip amount (must be non-negative)
    CASE
      WHEN t.tip_amount >= 0 THEN t.tip_amount
      ELSE 0
    END AS tip_amount,

    -- Validate total amount (must be non-negative)
    CASE
      WHEN t.total_amount >= 0 THEN t.total_amount
      ELSE NULL
    END AS total_amount,

    t.payment_type_id,
    COALESCE(pl.payment_type_name, 'UNKNOWN') AS payment_type_name,

    -- Calculate trip duration in minutes
    CAST((epoch_ms(t.dropoff_datetime) - epoch_ms(t.pickup_datetime)) AS BIGINT) / 60000.0 AS trip_duration_minutes,

    -- Calculate speed (miles per hour)
    CASE
      WHEN (epoch_ms(t.dropoff_datetime) - epoch_ms(t.pickup_datetime)) > 0
           AND t.trip_distance IS NOT NULL
      THEN t.trip_distance / (
        (CAST((epoch_ms(t.dropoff_datetime) - epoch_ms(t.pickup_datetime)) AS BIGINT) / 60000.0) / 60.0
      )
      ELSE NULL
    END AS speed_mph,

    t.extracted_at,
    CURRENT_TIMESTAMP AS processed_at

  FROM deduplicated_trips t
  LEFT JOIN ingestion.payment_lookup pl
    ON CAST(t.payment_type_id AS BIGINT) = CAST(pl.payment_type_id AS BIGINT)

  -- Filter records with valid key fields
  WHERE t.trip_id IS NOT NULL
    AND t.pickup_datetime IS NOT NULL
    AND t.dropoff_datetime IS NOT NULL
    AND t.dropoff_datetime > t.pickup_datetime
),

final_trips AS (
  -- Final selection with row numbers for monitoring/debugging
  SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_datetime DESC, trip_id) AS _bruin_row_num,
    *
  FROM cleaned_trips
)

SELECT * EXCLUDE (_bruin_row_num)
FROM final_trips;