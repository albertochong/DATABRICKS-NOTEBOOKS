# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AWS S3 Storage e Azure Databricks
# MAGIC > configuração do aws S3 storage com o Azure Databricks  
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/blob_storage_azure_databricks.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > Configuração do acesso ao Aws S3 Storage  
# MAGIC > https://docs.databricks.com/data/data-sources/aws/amazon-s3.html
# MAGIC 
# MAGIC <br>
# MAGIC > Em muitos casos o **Databricks** irá se conectar com um *Data Lake* para ingerir os dados do mesmo para o processamento, poderiamos usar o *databricks command-line interface (CLI)* para realizar essa configuração e assim a mesma não ficaria exposta dentro do Notebook.
# MAGIC 
# MAGIC > Esse tipo de configuração é essencial para porque irá evitar qualquer brecha de segurança no Data Lake. o Databricks irá guardar essas informações no *vault* de segurança aonde o mesmo é criptografado.

# COMMAND ----------

# DBTITLE 1,Pasta no S3 Storage Raw Data
access_key = ""
secret_key = ""
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "appnos"
mount_name = "nos"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)

# COMMAND ----------

# DBTITLE 1,Listar Conteúdo da Raw Zone
#display(dbutils.fs.mounts())
#display(dbutils.fs.ls("/mnt/nos/raw_data/raw_data"))
#dbutils.fs.unmount("/mnt/nos")
%fs ls "dbfs:/mnt/nos"
#display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/nos/raw_data/raw_data"

# COMMAND ----------


