# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC > os arquivos dentro do data lake estão no format JSON, como explicado anteriormente para que você possa ganhar em otimização  
# MAGIC > é extremamente importante que você sempre realize a conversão dos tipos de dados, se estiver trabalhando com spark então definitivamente   
# MAGIC > utilize apache parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Dimensão de User [Usuário] com Delta Lake [Dw]
# MAGIC 
# MAGIC > Neste caso como temos os dados armazenados no Data Lake iremos criar a seguinte estrutura:
# MAGIC <br>
# MAGIC 
# MAGIC > **Staging [Bronze]** = ingestão dos dados da mesma forma que está na fonte de dados    
# MAGIC > **Transformação [Silver]** = aplicar as transformações para refinarmos a tabela  
# MAGIC > **Dw [Gold]** = criar a tabela que está tratada e preparada  para Business
# MAGIC 
# MAGIC <br>
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dw-delta-lake-silver.png'>
# MAGIC   
# MAGIC > **1** - Ingestão [Fase 1]  
# MAGIC > **2** - Exploração [Fase 2]     
# MAGIC > **3** - Transformação [Fase 3]  
# MAGIC 
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)
# MAGIC 
# MAGIC 
# MAGIC > geralmente segregamos o Delta Lake em 3 partes  
# MAGIC 
# MAGIC > 1 - Bronze = **Staging**  
# MAGIC > 2 - Silver = **Transformações**  
# MAGIC > 3 - Gold = **Dw/DataSet** 
# MAGIC 
# MAGIC 
# MAGIC <img width="800" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-lake-store.png'>
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Explicitar o schema automaticamente sem ter que definir manualmente
# leio apenas um ficheiro para conhecer o schema e grava lo.
# Excluo aqui eventual mudança de schema e respectivo tratamento
df_product_nos_transactions = spark.read.json("dbfs:/mnt/nos/raw_data/nos-1625754392560-2.json")

# grava o schema adquirido
df_product_nos_transaction_schema = df_product_nos_transactions.schema

# imprime schema
df_product_nos_transaction_schema

# COMMAND ----------

# DBTITLE 1,Definir um watcher que vai ouvir a pasta Raw data como stream para processamento dos dados
# Os dados sao processados na ordem de modificaçao do ficheiro caso nao se escifique latestFirst
# Essa abordagem é perfeita quando não temos pastas gigantes porque cada vez que o "watcher" vai ver se tem dados ele lista todos os ficheiros para depois
# ver o que tem de novo para processar. Umas da slaternativas ´pode ser particionar a pasta por datas, dia, hora ect etc
# Para pastas grandes a opçao Autoloader que é uma feature advance aplica se melhor.

#try:
 # (

df_stream_produto_nos_bronze = (
    spark 
    .readStream 
    .schema(df_product_nos_transaction_schema) 
    .format("json") 
    .option("header", "true") 
    .option("maxFilesPerTrigger", 1) # process one file by trigger
    .option("cleanSource", "delete") 
    .load("/mnt/nos/raw_data/*.json") # read only json mas pod eler uma regex com ano,dia e mes por exemplo
)
 #)
#except AnalysisException as error:
  #print("Analysis Exception:")
  #print(error)

# verifica se é um stream
df_stream_produto_nos_bronze.isStreaming    

#df_stream_produto_nos_bronze.createOrReplaceTempView("wv_product_nos")
                             

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Stage 1 - Bronze Table
# MAGIC 
# MAGIC > Ao trazermos os dados raw do datalke para tabelas bronze faz com que ganhemos toda a optimização do delta lake como por exemplo:
# MAGIC * 1 - tabelas deltas sao guardados em ficheiros parquet
# MAGIC * 2 - time travel
# MAGIC * 3 - Esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Inicialização do Processo de Streaming do Data Lake para Tabela Delta Lake Bronze - Dados Crus
df_stream_produto_nos_bronze.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/delta/nos/_checkpoints/tbl_product_nos").option("mergeSchema", "true").start("/delta/tbl_product_nos")

#.trigger(Trigger.ProcessingTime("1 minute"))

# COMMAND ----------

# DBTITLE 1,Count Near Real Time. - Tabela Bronze Criada 
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/tbl_product_nos`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/tbl_product_nos` limit 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Stage 2 - Silver Table
# MAGIC > Quando uma modificação é realizada movemos para a **silver table**
# MAGIC > ela é a tabela responsável por receber informações que foram transformadas no **ETL** = filtros, limpeza e melhorias
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="300" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Leitura dos dados da Tabela Bronze e Aplicar algumas Tranformações e inserir numa Tabela Silver
from pyspark.sql.functions import col, concat_ws, expr, split, when, year, month, dayofmonth, dayofweek
#from pyspark.sql.types import DataType IntegerType
#import databricks.koalas as ks

df_stream_produto_nos_silver = spark.readStream.format("delta").load("/delta/tbl_product_nos")

df_tranformed_silver = df_stream_produto_nos_silver.select(
col("httprequest_headers.deviceId").alias("DeviceId"),
col("httprequest_headers.deviceType").alias("DeviceType"),
col("httpresponse_duration").alias("ResponseDuration"),
col("httpresponse_status").alias("ResponseStatus"),
split("httprequest_uri", "/", 6).getItem(5).alias("VideoType"),
when(split("httprequest_uri", "/", 6).getItem(5) == "stb", "Set-Top Box")
                  .otherwise("Mobiles Devices").alias("VideoTypeDescription"),  
split("httprequest_uri", "/", 6).getItem(3).alias("TypeofContent"),
when(split("httprequest_uri", "/", 6).getItem(3).startswith("RECO"),"Recording TV")
.when(split("httprequest_uri", "/", 6).getItem(3).startswith("PAST"),"Catch-Up TV")
.when(split("httprequest_uri", "/", 6).getItem(3).startswith("VODT"),"Video Clube Trailer")
.when(split("httprequest_uri", "/", 6).getItem(3).startswith("VODM"),"Video Clube Movie")
.otherwise("Live TV").alias("TypeofContentDescription"),
year(col("timestamp")).alias("Year"),
month(col("timestamp")).alias("Month"),
dayofmonth(col("timestamp")).alias("Day")
)

#display(df_tranformed_silver)

# write data to silver table
df_tranformed_silver.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/delta/nos/_checkpoints/tbl_product_nos_silver").start("/delta/tbl_product_nos_silver")

# COMMAND ----------

# DBTITLE 1,Count Near Real Time. - Tabela Silver Criada e Refinada
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/tbl_product_nos_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/tbl_product_nos_silver` limit 2

# COMMAND ----------

# DBTITLE 1,Criaçao da Tabela gold a apartir da Silver Refinada
df_stream_produto_nos_gold = spark.readStream.format("delta").load("/delta/tbl_product_nos_silver").writeStream.format("delta").outputMode("append").option("checkpointLocation", "/delta/nos/_checkpoints/tbl_product_nos_gold").start("/mnt/nos/gold_data/tbl_product_nos_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC --use DB_NOS_APP

# COMMAND ----------

# DBTITLE 1,Criaçao da Tabela gold a apartir da Silver Refinada com SQL
# MAGIC %sql
# MAGIC --CREATE TABLE IF NOT EXISTS tbl_product_nos_gold 
# MAGIC --USING delta
# MAGIC --LOCATION '/mnt/nos/gold_data'
# MAGIC --COMMENT 'All data to business team query'
# MAGIC --AS
# MAGIC --  SELECT *
# MAGIC --FROM delta.`/delta/tbl_product_nos_silver` 

# COMMAND ----------

# DBTITLE 1,Count Near Real Time. - Tabela Gold Criada e Preparada para Analytycs
# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM delta.`/mnt/nos/gold_data/tbl_product_nos_gold`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/mnt/nos/gold_data/tbl_product_nos_gold` limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select TypeofContentDescription as Conteudo, count(TypeofContentDescription) as QuantidadeVisualizacoes
# MAGIC from delta.`/mnt/nos/gold_data/tbl_product_nos_gold`
# MAGIC group by TypeofContentDescription
# MAGIC order by QuantidadeVisualizacoes desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select TypeofContentDescription as Conteudo, count(ResponseStatus) NumeroErrosReposta
# MAGIC from delta.`/mnt/nos/gold_data/tbl_product_nos_gold`
# MAGIC where ResponseStatus = 500
# MAGIC group by TypeofContentDescription
# MAGIC order by NumeroErrosReposta desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select TypeofContentDescription as Conteudo, count(ResponseDuration) TempoReposta
# MAGIC from delta.`/mnt/nos/gold_data/tbl_product_nos_gold`
# MAGIC group by TypeofContentDescription
# MAGIC order by TempoReposta desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select TypeofContentDescription as Conteudo, count(TypeofContentDescription) as MaisVisto, Year, Month
# MAGIC from delta.`/mnt/nos/gold_data/tbl_product_nos_gold`
# MAGIC group by Year, Month,TypeofContentDescription
# MAGIC order by MaisVisto desc, year asc, month asc

# COMMAND ----------

#dbutils.fs.rm('dbfs:/mnt/nos/gold_data',recurse=True)
#dbutils.fs.rm('dbfs:/delta',recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --show tables
# MAGIC --describe extended tbl_product_nos_gold

# COMMAND ----------


