-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_dataset.green_nyc_taxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris =['gs://divij-zoomcamp-2024-gcs-bucket/raw/green_tripdata_2022-*.parquet']
);

-- 840402 rows
SELECT COUNT(*) FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022;

-- create internal table
CREATE OR REPLACE TABLE nyc_taxi_dataset.green_nyc_taxi_2022_np AS
SELECT * FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022;

-- create partioned internal table
CREATE OR REPLACE TABLE nyc_taxi_dataset.green_nyc_taxi_2022_p
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID AS
SELECT * FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022;

-- DISTINCT PUlocationIDs
SELECT DISTINCT(PULocationID)
FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022;

-- DISTINCT PUlocationIDs ON INTERNAL TABLE
--0 MB for the External Table and 6.41MB for the Materialized Table
SELECT DISTINCT(PULocationID)
FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022_np;

-- FARE AMOUNT 0
--1622 rows
select count(*)  
FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022_np
WHERE fare_amount = 0;

-- IMPACT OF PARTITION
-- Scanning 12.82MB of data
SELECT DISTINCT(VendorID)
FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022_np
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- Scanning 1.12MB of data
SELECT DISTINCT(VendorID)
FROM divij-zoomcamp-2024.nyc_taxi_dataset.green_nyc_taxi_2022_p
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';