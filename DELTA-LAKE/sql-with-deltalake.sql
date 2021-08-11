-- Databricks notebook source
-- DBTITLE 1,1 - CREATE DATABASE
CREATE DATABASE IF NOT EXISTS dbacademy;
USE dbacademy

-- COMMAND ----------

-- DBTITLE 1,2 - Configure the Number of Shuffle Partitions
-- What is partitions shuffle?
SET spark.sql.shuffle.partitions=8

-- COMMAND ----------

-- DBTITLE 1,Removes Silver files from DBFS
-- MAGIC %fs
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

-- DBTITLE 1,Create Silver Parquet-based Data Lake Table
-- We create the table using the Create Table As Select (CTAS) Spark SQL pattern. 
-- We could create delta tables directly from our tables by replacing USING PARQUET to USING DELTA
USE dbacademy;

DROP TABLE IF EXISTS health_tracker_silver;      -- ensures that if we run this again, it won't fail
                                                          
CREATE TABLE health_tracker_silver                        
USING PARQUET                                             
PARTITIONED BY (p_device_id)                     -- column used to partition the data
LOCATION "/dbacademy/DLRS/healthtracker/silver"  -- location where the parquet files will be saved
AS (                                                      
  SELECT name,                                   -- query used to transform the raw data
         heartrate,                                       
         CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,  
         CAST(FROM_UNIXTIME(time) AS DATE) AS dte,        
         device_id AS p_device_id                         
  FROM health_tracker_data_2020_01   
)


-- COMMAND ----------

-- DBTITLE 1,Counting
SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- DBTITLE 1,Convert an Existing Parquet-based Data Lake Table to a Silver Delta table
DESCRIBE DETAIL health_tracker_silver

-- COMMAND ----------

-- DBTITLE 1,Step 1: Convert the Files to Delta Files
-- First, we'll convert the files in place to Parquet files. The conversion creates a Delta Lake transaction log that tracks the files. Now, the directory is a --  -- directory of Delta files.
CONVERT TO DELTA 
  parquet.`/dbacademy/DLRS/healthtracker/silver` 
  PARTITIONED BY (p_device_id double)

-- COMMAND ----------

-- DBTITLE 1,Step 2: Register the Silver Delta Table
-- Next, we will register the table in the Metastore. The Spark SQL command will automatically infer the data schema by reading the footers of the Delta files.
DROP TABLE IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/dbacademy/DLRS/healthtracker/silver";

DESCRIBE DETAIL health_tracker_silver;

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- DBTITLE 1,Removes Gold files from DBFS
-- MAGIC %fs 
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics

-- COMMAND ----------

-- DBTITLE 1,Create a Gold Delta Table from Silver
DROP TABLE IF EXISTS health_tracker_user_analytics;

CREATE TABLE health_tracker_user_analytics
USING DELTA
LOCATION '/dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics'
AS (
  SELECT p_device_id, 
         AVG(heartrate) AS avg_heartrate,
         STD(heartrate) AS std_heartrate,
         MAX(heartrate) AS max_heartrate 
  FROM health_tracker_silver GROUP BY p_device_id
)

-- COMMAND ----------

SELECT * FROM health_tracker_user_analytics

-- COMMAND ----------

-- DBTITLE 1,Append Files to an Existing Silver Table
-- REFERENCES
-- https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-dml-insert.html

INSERT INTO health_tracker_silver
SELECT name,
       heartrate,
       CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,
       CAST(FROM_UNIXTIME(time) AS DATE) AS dte,
       device_id as p_device_id
FROM health_tracker_data_2020_02

-- COMMAND ----------

-- DBTITLE 1,Time Travel
-- REFERENCES : https://docs.databricks.com/delta/quick-start.html#query-an-earlier-version-of-the-table-time-travel&language-python
-- Delta Lake time travel allows you to query an older snapshot of a Delta table.

-- FIRST VERSION OF DATA. IF YOU DONT SPECIFY VERSION CLAUSE WILL RETURN ALL DATA ,the latest version and includes the new records added.
SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

--GET BY RANGE DATE
--SELECT COUNT(*) FROM health_tracker_silver TIMESTAMP AS OF '2021-07-21 00:00:00'

-- COMMAND ----------

SELECT p_device_id, month(dte) as month, COUNT(*) as total FROM health_tracker_silver VERSION AS OF 1
group by p_device_id,month
order by p_device_id,month
-- is missing 95 data from device 4
-- each device should emit 24 hour X 31 days for january = 744 X 5(devices) = 3720
-- for February : 24 hour X 29 days = 696 x 5 (devices)                     = 3480   ---> total = 7200 but he have 7105

-- COMMAND ----------

SELECT month(dte) as month, COUNT(*) as total FROM health_tracker_silver VERSION AS OF 1
group by month
order by month

-- COMMAND ----------

SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id

-- COMMAND ----------

SELECT * FROM health_tracker_silver WHERE p_device_id IN (3, 4)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
)

-- COMMAND ----------

select * from broken_readings

-- COMMAND ----------

SELECT SUM(broken_readings_count) FROM broken_readings

-- COMMAND ----------

SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver

-- COMMAND ----------



CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
)

-- COMMAND ----------

--select * from updates
SELECT COUNT(*) FROM updates

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inserts 
AS (
    SELECT name, 
    heartrate,
    CAST(FROM_UNIXTIME(time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(time) AS date) AS dte,
    device_id
    FROM health_tracker_data_2020_02_01
   )

-- COMMAND ----------

select * from inserts

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    )

-- COMMAND ----------

-- DBTITLE 1,Merge data into Delta Silver
MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 2

-- COMMAND ----------

-- DBTITLE 1,Delta lake Silver History
-- Delta Lake uses a transaction log to keep track of all activity on a given table.
-- By default, table history is retained for 30 days.
-- The DESCRIBE HISTORY Spark SQL command provides provenance information, 
-- including the operation, user, and so on, for each write to a table. 
DESCRIBE HISTORY health_tracker_silver 

-- COMMAND ----------

-- DBTITLE 1,Delete records
DELETE FROM health_tracker_silver where p_device_id = 4

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

-- DBTITLE 1,View the files in the Transaction Log associate by Delta silver Table
-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /dbacademy/DLRS/healthtracker/silver/_delta_log

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 1,Recover the Lost Data
-- when we delete record ,we can recover form last previous version before delete
CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
  SELECT NULL AS name, heartrate, time, dte, p_device_id 
  FROM health_tracker_silver VERSION AS OF 2
  WHERE p_device_id = 4
)

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

-- DBTITLE 1,Counting after recovery
SELECT COUNT(*) FROM health_tracker_silver 

-- COMMAND ----------

-- DBTITLE 1,Vacuum Table to Remove Old Files
-- The VACUUM Spark SQL command can be used to solve this problem. 
-- The VACUUM command recursively vacuums directories associated with the Delta table and removes files that are no longer in the latest state of the transaction -- -- log for that table and that are older than a retention threshold. The default threshold is 7 days.


SET spark.databricks.delta.retentionDurationCheck.enabled = false


VACUUM health_tracker_silver RETAIN 0 Hours

-- COMMAND ----------


