select station_id, name from `bigquery-public-data.new_york.citibike_stations` LIMIT 100;

create or replace external table `test-data-eng-course.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test-bucket-module-3/data/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE `test-data-eng-course.ny_taxi.yellow_taxi_not_partitioned`
AS
SELECT * FROM `test-data-eng-course.ny_taxi.external_yellow_tripdata`;


-- quuestion 1 asware c '20,332,093'
SELECT count(*)
FROM `test-data-eng-course.ny_taxi.external_yellow_tripdata`;


-- query for conting disinct PULocationIDs question 2 answare b '0 MB for the External Table and 155.12 MB for the Materialized Table'
select count(distinct PULocationID) from `test-data-eng-course.ny_taxi.external_yellow_tripdata`;

select count(distinct PULocationID) from`test-data-eng-course.ny_taxi.yellow_taxi_not_partitioned`;

--question 4 answare d '8333'
SELECT count(fare_amount) from`test-data-eng-course.ny_taxi.external_yellow_tripdata` where fare_amount=0;

--question 5 answare a

CREATE OR REPLACE TABLE `test-data-eng-course.ny_taxi.yellow_tripdata_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `test-data-eng-course.ny_taxi.external_yellow_tripdata`;

-- test with partitions
-- question 6 b '310.24 MB for non-partitioned table and 26.84 MB for the partitioned table'

select distinct(VendorID) from `test-data-eng-course.ny_taxi.yellow_taxi_not_partitioned`
where date(tpep_dropoff_datetime)  between '2024-03-01' and '2024-03-15';

select distinct(VendorID) from `test-data-eng-course.ny_taxi.yellow_tripdata_part_clust`
where date(tpep_dropoff_datetime)  between '2024-03-01' and '2024-03-15';


--question 7 c 'GCP Bucket'


SELECT count(*)
FROM `test-data-eng-course.ny_taxi.yellow_tripdata_part_clust`;

SELECT count(*)
FROM `test-data-eng-course.ny_taxi.yellow_taxi_not_partitioned`;