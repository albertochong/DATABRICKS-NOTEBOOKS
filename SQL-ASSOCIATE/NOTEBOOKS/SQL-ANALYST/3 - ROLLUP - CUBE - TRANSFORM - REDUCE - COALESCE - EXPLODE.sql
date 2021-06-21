-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Applications of SQL on Databricks
-- MAGIC 1. Create Table from Parquet uploaded on filestore
-- MAGIC 
-- MAGIC 1. Describe : detail and extended
-- MAGIC 
-- MAGIC 1. Examine and Explode Nested Objects
-- MAGIC 
-- MAGIC 1. Manage Tables and Views : 
-- MAGIC    1. Common Table Expressions (CTE)
-- MAGIC    1. Create Table As Select (CTAS)
-- MAGIC    
-- MAGIC 1. Cache Table
-- MAGIC 
-- MAGIC 1. Working with Array Data
-- MAGIC    1. Applied Higher Order Functions 
-- MAGIC       1. TRANSFORM
-- MAGIC       1. REDUCE
-- MAGIC       
-- MAGIC 1. Pivots, Rollups, Cubes and Coalesce 

-- COMMAND ----------

-- DBTITLE 1,Create Table from Parquet uploaded on filestore 
DROP TABLE IF EXISTS dc_data_raw;
CREATE TABLE dc_data_raw
USING parquet
OPTIONS (
  PATH "/FileStore/tables/data_centers_q2_q3_snappy.parquet"
);

-- COMMAND ----------

SELECT * FROM dc_data_raw LIMIT 1

-- COMMAND ----------

-- view strucuture of table
describe detail dc_data_raw

-- COMMAND ----------

-- DBTITLE 1,Examine and Explode Nested Objects
SELECT dc_id,explode(source) FROM dc_data_raw 

-- COMMAND ----------

-- DBTITLE 1,Manage Tables and Views : CTE
WITH explode_source
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM dc_data_raw
  )
SELECT key,
  dc_id,
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
  
FROM explode_source;

-- COMMAND ----------

-- DBTITLE 1,Create Table As Select (CTAS)
DROP TABLE IF EXISTS device_data;

CREATE TABLE device_data 
USING delta
PARTITIONED BY (device_type)
WITH explode_source
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM dc_data_raw
  )
SELECT 
  dc_id,
  key `device_type`, 
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
  
FROM explode_source;

-- COMMAND ----------

SELECT * FROM device_data

-- COMMAND ----------

DESCRIBE EXTENDED device_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC > Cache Table
-- MAGIC 
-- MAGIC The table device_data  contains all the information we'll need for the remainder of the lesson. One useful way to optimize your queries is by caching tables that you will reuse over and over. Caching places the contents of a table into the memory (or local disk) of your cluster. This can make subsequent reads faster. Keep in mind that caching  may be an expensive operation as it needs to first cache all of the data before it can operate on it. Caching is useful if you plan to query the same data repeatedly. 

-- COMMAND ----------

-- DBTITLE 1,Cache Table
CACHE TABLE device_data

-- COMMAND ----------

-- DBTITLE 1,Working with Array Data: Applied Higher Order Functions ----> TRANSFORM AND REDUCE
-- Transformhe temperature in degrees Celsius to Fahrenheit. the formula is : f = ((temperature ª 9)/5) + 32
SELECT 
  dc_id,
  device_type, 
  temps,
  TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) AS `temps_Fahrenheit`, --foreach element on array apply calculation
  co2_level
FROM device_data;

-- COMMAND ----------

-- average co2_level
CREATE OR REPLACE TEMPORARY VIEW co2_levels_temporary
AS
SELECT
    dc_id, 
    device_type,
    co2_level,
    REDUCE(co2_level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_level))) as average_co2_level
  FROM device_data
  SORT BY average_co2_level DESC;

-- COMMAND ----------

select * from co2_levels_temporary

-- COMMAND ----------

-- DBTITLE 1,Use Pivot Tables
 -- Pivot tables are supported in Spark SQL. A pivot table allows you to transform rows into columns and group by any data field. 
 -- Let's try viewing our data by device to see which type emits the most CO2 on average. 
 
SELECT * FROM (
  SELECT device_type, average_co2_level 
  FROM co2_levels_temporary
)
PIVOT (
  ROUND(AVG(average_co2_level), 2) AS avg_co2 
  FOR device_type IN ('sensor-ipad', 'sensor-inest', 
    'sensor-istick', 'sensor-igauge')
  );

-- COMMAND ----------

-- DBTITLE 1,Rollups
-- Rollups are operators used with the GROUP BY clause.  
-- They allow you to summarize data based on the columns passed to the ROLLUP operator.  
-- In this example, we've calculated the average temperature across all data centers and across all devices as well as the overall average at each data center ----- (dc_id) 

SELECT 
  dc_id,
  device_type,
  ROUND(AVG(average_co2_level)) AS avg_co2_level 
FROM co2_levels_temporary
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type

-- COMMAND ----------

-- DBTITLE 1,Coalesce 
-- We can use the COALESCE function to substitute a new title in for each null value we introduced. 
SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(average_co2_level))  AS avg_co2_level
FROM co2_levels_temporary
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type

-- COMMAND ----------

-- DBTITLE 1,Cube
SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(average_co2_level))  AS avg_co2_level
FROM co2_levels_temporary
GROUP BY CUBE (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------


