# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Relational Entities on Databricks
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC **In this lab, you will create and explore interactions between various relational entities, including:**
# MAGIC 
# MAGIC - Databases
# MAGIC - Tables (managed and unmanaged)
# MAGIC - Views (views, temp views, and global temp views)
# MAGIC 
# MAGIC **Resources**
# MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
# MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
# MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
# MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
# MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This script includes logic to declare a user-specific database. We won't be using this database for this lesson.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview of the Data
# MAGIC 
# MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celcius. The schema for the table:
# MAGIC 
# MAGIC |ColumnName  | DataType| Description|
# MAGIC |------------|---------|------------|
# MAGIC |NAME        |string   | Station name |
# MAGIC |STATION     |string   | Unique ID |
# MAGIC |LATITUDE    |float    | Latitude |
# MAGIC |LONGITUDE   |float    | Longitude |
# MAGIC |ELEVATION   |float    | Elevation |
# MAGIC |DATE        |date     | YYYY-MM-DD |
# MAGIC |UNIT        |string   | Temperature units |
# MAGIC |TAVG        |float    | Average temperature |
# MAGIC 
# MAGIC The path to the data is provided below.

# COMMAND ----------

source = "dbfs:/mnt/training/weather/StationData/stationData.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL allows queries directly against Parquet directories.

# COMMAND ----------

display(spark.sql(f"""
  SELECT * 
  FROM parquet.`{source}`
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure a `dbname` Variable
# MAGIC This ensures we won't conflict with other databases and will make it easier to write checks with Python later in the lesson.
# MAGIC 
# MAGIC Suggested format: `<your_initials>_<some_integer>`, e.g., `foo_123`

# COMMAND ----------

# ANSWER
dbname = "foo_123" # NOTE: if multiple users are in the same workspace, make sure you change the solution code before running to have a unique database name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Database
# MAGIC 
# MAGIC Create a database in the default location using the `dbname` from the last cell.

# COMMAND ----------

# ANSWER
spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change to Your New Database
# MAGIC 
# MAGIC `USE` your newly created database.

# COMMAND ----------

# ANSWER
spark.sql(f"USE {dbname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Managed Table
# MAGIC Use a CTAS statement to create a managed table named `weather_managed`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE TABLE weather_managed AS
# MAGIC SELECT * 
# MAGIC FROM parquet.`dbfs:/mnt/training/weather/StationData/stationData.parquet`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an Unmanaged Table
# MAGIC 
# MAGIC Unmanaged tables can be registered directly to existing directories of files. Create a table called `weather_unmanaged` using the provided location.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE TABLE weather_unmanaged 
# MAGIC USING parquet
# MAGIC LOCATION "dbfs:/mnt/training/weather/StationData/stationData.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Table Details
# MAGIC Use the SQL command `DESCRIBE EXTENDED table_name` to examine the two weather tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED weather_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED weather_unmanaged

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following helper code to extract and compare the table locations.

# COMMAND ----------

def getTableLocation(tableName):
  return spark.sql(f"DESCRIBE EXTENDED {tableName}").select("data_type").filter("col_name = 'Location'").collect()[0][0]

# COMMAND ----------

managedTablePath = getTableLocation("weather_managed")
unmanagedTablePath = getTableLocation("weather_unmanaged")

print(f"""The weather_managed table is saved at: 

    {managedTablePath}

The weather_unmanaged table is saved at:

    {unmanagedTablePath}""")

# COMMAND ----------

# MAGIC %md
# MAGIC List the contents of these directories to confirm that data exists in both locations.

# COMMAND ----------

dbutils.fs.ls(managedTablePath)

# COMMAND ----------

dbutils.fs.ls(unmanagedTablePath)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Check Directory Contents after Dropping Database and All Tables
# MAGIC The `CASCADE` keyword will accomplish this.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> You will encounter an error when listing your `managedTablePath`

# COMMAND ----------

# ANSWER
spark.sql(f"DROP DATABASE {dbname} CASCADE")

# COMMAND ----------

dbutils.fs.ls(managedTablePath)

# COMMAND ----------

dbutils.fs.ls(unmanagedTablePath)

# COMMAND ----------

# MAGIC %md
# MAGIC **This highlights the main differences between managed and unmanaged tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
# MAGIC 
# MAGIC Files for unmanaged tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **Unmanaged tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Database with a Specified Path
# MAGIC 
# MAGIC Assuming you dropped your database in the last step, you can use the same `dbname`.

# COMMAND ----------

spark.sql(f"CREATE DATABASE {dbname} LOCATION '{userhome}/{dbname}'")

# COMMAND ----------

# MAGIC %md
# MAGIC Recreate your `weather_managed` table in this new database and print out the location of this table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE TABLE weather_managed AS
# MAGIC SELECT * 
# MAGIC FROM parquet.`dbfs:/mnt/training/weather/StationData/stationData.parquet`

# COMMAND ----------

getTableLocation("weather_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC While here we're using the `userhome` directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views and their Scoping
# MAGIC 
# MAGIC Using the provided `AS` clause, register:
# MAGIC - a view named `celcius`
# MAGIC - a temporary view named `celcius_temp`
# MAGIC - a global temp view named `celcius_global`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW celcius
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW celcius_temp
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW celcius_global
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")

# COMMAND ----------

# MAGIC %md
# MAGIC Views will be displayed alongside tables when listing from the catalog.

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %md
# MAGIC Note the following:
# MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
# MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
# MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the `global_temp` database**. The `global_temp` database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.celcius_global

# COMMAND ----------

# MAGIC %md
# MAGIC While no job was triggered when defining these view, a job is triggered _each time_ a query is executed against the view.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up
# MAGIC Drop the database and all tables to clean up your workspace.

# COMMAND ----------

#python
spark.sql(f"DROP DATABASE {dbname} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synopsis
# MAGIC 
# MAGIC In this lab we:
# MAGIC - Created and deleted databases
# MAGIC - Explored behavior of managed and unmanaged tables
# MAGIC - Learned about the scoping of views

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
