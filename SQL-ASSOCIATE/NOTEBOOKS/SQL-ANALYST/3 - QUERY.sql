-- Databricks notebook source
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

SELECT dc_id,explode(source) FROM dc_data_raw 

-- COMMAND ----------


