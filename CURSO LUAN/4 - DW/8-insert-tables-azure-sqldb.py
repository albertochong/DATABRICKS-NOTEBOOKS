# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks e Azure SQL DB [Inserting Data into Database]
# MAGIC 
# MAGIC > conectando no Azure SQL Database  
# MAGIC > inserindo tabelas
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > **connection string:**  
# MAGIC > jdbc:sqlserver://owshq.database.windows.net:1433;database=sales;user=luanmoreno@owshq;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
# MAGIC 
# MAGIC > {your_password_here} = qq11ww22!!@@  
# MAGIC > com.microsoft.azure:azure-sqldb-spark:1.0.2.  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > https://docs.databricks.com/spark/latest/data-sources/sql-databases.html  
# MAGIC > https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases-azure.html  
# MAGIC > https://github.com/Azure/azure-sqldb-spark  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > **Step by Step**  
# MAGIC > 1 - Download do Conector [Maven] = https://search.maven.org/search?q=a:azure-sqldb-spark [azure-sqldb-spark-1.0.2.jar]  
# MAGIC > 2 - Importar Biblioteca para o Azure Databricks [Cluster]  
# MAGIC > 3 - Execução do Notebook    
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Utilizando o Banco de Dados 
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,Mostrando Tabelas
# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# DBTITLE 1,Lendo o Delta Lake [gold_reviews] em Scala
# MAGIC %scala
# MAGIC 
# MAGIC val df_gold_reviews = spark.table("gold_reviews")

# COMMAND ----------

# DBTITLE 1,Mostrando Registros
# MAGIC %scala
# MAGIC 
# MAGIC display(df_gold_reviews)

# COMMAND ----------

# DBTITLE 1,Criando Tabela com ColumnStore Index para Inserção e Consulta de Dados Rápida
# MAGIC %md
# MAGIC 
# MAGIC > sql server  
# MAGIC > somente a partir da **premium**
# MAGIC 
# MAGIC DROP TABLE gold_reviews
# MAGIC 
# MAGIC CREATE TABLE gold_reviews  
# MAGIC (  
# MAGIC review_id VARCHAR(50),  
# MAGIC business_id VARCHAR(50),  
# MAGIC user_id VARCHAR(50),  
# MAGIC review_stars BIGINT,  
# MAGIC review_useful BIGINT,  
# MAGIC store_name VARCHAR(100),  
# MAGIC store_city VARCHAR(100),  
# MAGIC store_state VARCHAR(100),  
# MAGIC store_category VARCHAR(50),  
# MAGIC store_review_count BIGINT,  
# MAGIC store_stars FLOAT,  
# MAGIC user_name VARCHAR(50),  
# MAGIC user_average_stars FLOAT,  
# MAGIC user_importance VARCHAR(20)  
# MAGIC );  
# MAGIC 
# MAGIC CREATE CLUSTERED COLUMNSTORE INDEX cci_gold_reviews ON gold_reviews   

# COMMAND ----------

# DBTITLE 1,Verificando Biblioteca para Conexão com Azure SQL DB
# MAGIC %scala
# MAGIC 
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# DBTITLE 1,Configurando Variáveis
# MAGIC %scala
# MAGIC 
# MAGIC val jdbcHostname = "owshq.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "sales"
# MAGIC 
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"luanmoreno")
# MAGIC connectionProperties.put("password", s"qq11ww22!!@@")

# COMMAND ----------

# DBTITLE 1,Verificando Driver do SQL Server
# MAGIC %scala
# MAGIC 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# DBTITLE 1,Tentando Ler Tabela Vazia do Azure SQL DB
# MAGIC %scala
# MAGIC 
# MAGIC val read_df_gold_reviews = spark.read.jdbc(jdbcUrl, "gold_reviews", connectionProperties)

# COMMAND ----------

# DBTITLE 1,Truncando Tabela
# MAGIC %scala
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val query = """
# MAGIC               |TRUNCATE TABLE gold_reviews;
# MAGIC             """.stripMargin
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"               -> "owshq.database.windows.net",
# MAGIC   "databaseName"      -> "sales",
# MAGIC   "user"              -> "luanmoreno",
# MAGIC   "password"          -> "qq11ww22!!@@",
# MAGIC   "queryCustom"       -> query
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(config)

# COMMAND ----------

# DBTITLE 1,Mapeamento de Conversão de Tipos para JDBC
# MAGIC %md
# MAGIC 
# MAGIC https://db.apache.org/ojb/docu/guides/jdbc-types.html

# COMMAND ----------

# DBTITLE 1,Insert into Azure SQL DB = 9.17 minutes
# MAGIC %scala
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC  
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> "owshq.database.windows.net",
# MAGIC   "databaseName" -> "sales",
# MAGIC   "dbTable"      -> "gold_reviews",
# MAGIC   "user"         -> "luanmoreno",
# MAGIC   "password"     -> "qq11ww22!!@@"
# MAGIC ))
# MAGIC 
# MAGIC df_gold_reviews.write.mode(SaveMode.Overwrite).sqlDB(config)

# COMMAND ----------

# DBTITLE 1,Insert into Azure SQL DB with Bulk Copy = 4.99 minutes
# MAGIC %scala
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"               -> "owshq.database.windows.net",
# MAGIC   "databaseName"      -> "sales",
# MAGIC   "user"              -> "luanmoreno",
# MAGIC   "password"          -> "qq11ww22!!@@", 
# MAGIC   "dbTable"           -> "gold_reviews", 
# MAGIC   "bulkCopyBatchSize" -> "2500",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "600"
# MAGIC ))
# MAGIC 
# MAGIC df_gold_reviews.bulkCopyToSqlDB(config)

# COMMAND ----------


