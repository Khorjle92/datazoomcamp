# datazoomcamp homework week 3

# SETUP
CREATE OR REPLACE EXTERNAL TABLE ny_taxi.external_greentaxi2022
OPTIONS(
  format = 'parquet',
  uris = ['gs://mage_zoomcamp_khorjle/green_tripdata_2022/green_tripdata_2022-*.parquet']
);

# Q1
SELECT Count(*) from ny_taxi.external_greentaxi2022;

# Q2
-- For external table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.external_greentaxi2022;

-- For native BigQuery table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.normal_green

# Q3
-- Q3 records with fare_amount 0
SELECT COUNT(*) FROM ny_taxi.external_greentaxi2022 
WHERE fare_amount = 0;

# Q4
-- Partition by date, cluster by PULocationID


-- Create Partitioned by date, cluster by PULocationID
CREATE OR REPLACE TABLE ny_taxi.greentaxi2022_partition_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER By PULocationID AS 
SELECT * FROM `ny_taxi.external_greentaxi2022`

# Q5
-- Non Partitioned table Q5
SELECT DISTINCT PULocationID AS distinct_PULocationIDs
FROM ny_taxi.normal_green
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

--  Partitioned table Q5
SELECT DISTINCT PULocationID AS distinct_PULocationIDs
FROM ny_taxi.greentaxi2022_partition_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

# Q6
-- GCP

# Q7
-- False, not everytime.

# Q8
--8
SELECT Count(*) From ny_taxi.normal_green
0 MB. The information is already registered in the table's storage info (number of rows). there is no querying needed. 