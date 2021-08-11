# Databricks notebook source
# DBTITLE 1,1 - CREATE DATABASE
username = "achong"
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")
health_tracker = f"/dbacademy/{username}/DLRS/healthtracker/"

# COMMAND ----------

# DBTITLE 1,2 -  Configure the Number of Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# DBTITLE 1,3 - Download the data to the driver
# MAGIC %sh
# MAGIC 
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2_late.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json

# COMMAND ----------

# DBTITLE 1,4 - Verify the downloads
# MAGIC %sh ls

# COMMAND ----------

# DBTITLE 1,5 - Move the data to the raw directory
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_1.json", 
              health_tracker + "raw/health_tracker_data_2020_1.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2.json", 
              health_tracker + "raw/health_tracker_data_2020_2.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2_late.json", 
              health_tracker + "raw/health_tracker_data_2020_2_late.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_3.json", 
              health_tracker + "raw/health_tracker_data_2020_3.json")

# COMMAND ----------

# DBTITLE 1,6 - Load the data
# - Load the data as a Spark DataFrame from the raw directory. This is done using the .format("json") option.  
file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
 
health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# In production dont display data, only in dev to check data
display(health_tracker_data_2020_1_df)

# COMMAND ----------

# DBTITLE 1,7 - Remove files in the /dbacademy/DLRS/healthtracker/processed directory
dbutils.fs.rm(health_tracker + "processed", recurse=True)

# COMMAND ----------

# DBTITLE 1,8 - General Function to Transform one DataFrame
from pyspark.sql.functions import col, from_unixtime
 
def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))                     ### cast double time column to unixtime
    .withColumnRenamed("device_id", "p_device_id")                 ### rename a column device_id
    .withColumn("time", col("time").cast("timestamp"))             ### cast column to to timestamp
    .withColumn("dte", col("time").cast("date"))                   ### cast column to date and rename
    .withColumn("p_device_id", col("p_device_id").cast("integer")) ### cast column p_device_id form long to integer
    .select("dte", "time", "heartrate", "name", "p_device_id")     ### select our column
    )

# COMMAND ----------

# DBTITLE 1,9 - Transform DataFrame with Function
processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# DBTITLE 1,9 - Write the Files to the processed directory
# - Note that we are partitioning the data by device id.
# - OBS : we can write directly to delta format

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "processed"))

# COMMAND ----------

# DBTITLE 1,10 - Register the table in the metastore
# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed                        
# MAGIC USING PARQUET                
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

# DBTITLE 1,11 - Counting records
# - Per best practice, we have created a partitioned table. However, if you create a partitioned table from existing data, Spark SQL does not automatically discover #   the partitions and register them in the Metastore this why count = 0. 
health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# DBTITLE 1,12 - Register the partitions and Counting again 
# MAGIC %sql
# MAGIC MSCK REPAIR TABLE health_tracker_processed;
# MAGIC 
# MAGIC select count(*) from health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Table Fundamental
# MAGIC 
# MAGIC #### Delta table consists of main things:
# MAGIC 
# MAGIC 1. The data files kept in object storage (AWS S3, Azure Data Lake Storage)
# MAGIC 2. The Delta Transaction Log saved with the data files in object storage 
# MAGIC 3. A table registered in the Metastore. This step is optional 
# MAGIC 4. You can create a Delta table by :
# MAGIC > a) Convert parquet files using the Delta Lake API <br>
# MAGIC > b) Write new files using the Spark DataFrame writer with .format("delta")

# COMMAND ----------

# DBTITLE 1,13 - Describe the health_tracker_processed table
# MAGIC %sql
# MAGIC --Before we convert the health_tracker_processed table, let's use the DESCRIBE DETAIL Spark SQL command to display the attributes of the table.
# MAGIC DESCRIBE extended health_tracker_processed

# COMMAND ----------

# DBTITLE 1,Convert a Parquet Table to a Delta Table
# MAGIC %md
# MAGIC 
# MAGIC # Convert a Parquet Table to a Delta Table
# MAGIC > When working with Delta Lake on Databricks, Parquet files can be converted in-place to Delta files. Next, we will convert the Parquet-based data lake table we created previously into a Delta table. In doing so, we are defining the single source of truth at the heart of our EDSS. 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://chongtech.blob.core.windows.net/chongtechdatabricksimages/deltatables.png'>
# MAGIC   

# COMMAND ----------

# DBTITLE 1,14 - Convert the files to Delta files
from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}processed`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# DBTITLE 1,14 - Register the Delta table
# MAGIC %sql
# MAGIC --At this point, the files containing our records have been converted to Delta files. The Metastore, however, has not been updated to reflect the change. To change this we re-register the table in the Metastore. The Spark SQL command will automatically infer the data schema by reading the footers of the Delta files. 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC USING DELTA
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------


