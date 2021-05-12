# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC > https://docs.databricks.com/delta/quick-start.html#id9  
# MAGIC > https://docs.microsoft.com/en-us/azure/databricks/delta/  
# MAGIC > https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM gold_reviews

# COMMAND ----------

# DBTITLE 1,Display Table History
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY gold_reviews

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM gold_reviews VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Optimize a Table
# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE gold_reviews

# COMMAND ----------

# DBTITLE 1,Z-Order by Columns
# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE gold_reviews ZORDER BY (store_name)

# COMMAND ----------

# DBTITLE 1,Snapshots
# MAGIC %sql
# MAGIC 
# MAGIC VACUUM gold_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --11.30.0844
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT store_name, COUNT(*) AS Q
# MAGIC FROM gold_reviews
# MAGIC GROUP BY store_name
# MAGIC ORDER BY store_name DESC
# MAGIC 
# MAGIC --7.63 secs
# MAGIC --3.40 secs

# COMMAND ----------


