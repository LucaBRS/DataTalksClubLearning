-- question 3 : 8,007

SELECT COUNT(*)
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;


--  question 4 : 2025-11-14

SELECT lpep_pickup_datetime::date,max(trip_distance) as max_trip
FROM green_taxi_data
WHERE trip_distance < 100
  AND lpep_pickup_datetime::date IN (
    DATE '2025-11-14',
    DATE '2025-11-20',
    DATE '2025-11-23',
    DATE '2025-11-25'
  )
  GROUP BY  lpep_pickup_datetime::date

  ORDER BY max_trip DESC;


-- question 5: East Harlem North

SELECT
  g.lpep_pickup_datetime::date AS pickup_date,
  g."PULocationID",
  z."Zone",
  COUNT(*) AS total_t
FROM green_taxi_data AS g
LEFT JOIN taxi_zone_data AS z
  ON g."PULocationID" = z."LocationID"
WHERE g.lpep_pickup_datetime::date = DATE '2025-11-18'
GROUP BY
  g.lpep_pickup_datetime::date,
  g."PULocationID",
  z."Zone"
ORDER BY total_t DESC;


-- question 6: Yorkville West

SELECT

  z_do."Zone" AS dropoff_zone,
  MAX(COALESCE(g.tip_amount, 0)) AS tip_sum
FROM green_taxi_data AS g

LEFT JOIN taxi_zone_data AS z_pu
  ON g."PULocationID" = z_pu."LocationID"

LEFT JOIN taxi_zone_data AS z_do
  ON g."DOLocationID" = z_do."LocationID"

WHERE g.lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
  AND g.lpep_pickup_datetime <  TIMESTAMP '2025-12-01'
  AND z_pu."Zone" = 'East Harlem North'

GROUP BY z_do."Zone"

ORDER BY tip_sum DESC
;