# Databricks notebook source
# MAGIC %md ## Set up Connection to Kafka

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{get_json_object, json_tuple}
# MAGIC  
# MAGIC var streamingInputDF = 
# MAGIC   spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<server:ip")
# MAGIC     .option("subscribe", "topic1")     
# MAGIC     .option("startingOffsets", "latest")  
# MAGIC     .option("minPartitions", "10")  
# MAGIC     .option("failOnDataLoss", "true")
# MAGIC     .load()

# COMMAND ----------

# MAGIC %md ## streamingInputDF.printSchema
# MAGIC 
# MAGIC   root <br><pre>
# MAGIC    </t>|-- key: binary (nullable = true) <br>
# MAGIC    </t>|-- value: binary (nullable = true) <br>
# MAGIC    </t>|-- topic: string (nullable = true) <br>
# MAGIC    </t>|-- partition: integer (nullable = true) <br>
# MAGIC    </t>|-- offset: long (nullable = true) <br>
# MAGIC    </t>|-- timestamp: timestamp (nullable = true) <br>
# MAGIC    </t>|-- timestampType: integer (nullable = true) <br>

# COMMAND ----------

# MAGIC %md ## Sample Message
# MAGIC <pre>
# MAGIC {
# MAGIC </t>"city": "<CITY>", 
# MAGIC </t>"country": "United States", 
# MAGIC </t>"countryCode": "US", 
# MAGIC </t>"isp": "<ISP>", 
# MAGIC </t>"lat": 0.00, "lon": 0.00, 
# MAGIC </t>"query": "<IP>", 
# MAGIC </t>"region": "CA", 
# MAGIC </t>"regionName": "California", 
# MAGIC </t>"status": "success", 
# MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00", 
# MAGIC </t>"zip": "38917" 
# MAGIC }

# COMMAND ----------

# MAGIC %md ## GroupBy, Count

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC var streamingSelectDF = 
# MAGIC   streamingInputDF
# MAGIC    .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))
# MAGIC     .groupBy($"zip") 
# MAGIC     .count()

# COMMAND ----------

# MAGIC %md ## Window

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC var streamingSelectDF = 
# MAGIC   streamingInputDF
# MAGIC    .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"), get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"))
# MAGIC    .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
# MAGIC    .count()

# COMMAND ----------

# MAGIC %md ## Memory Output

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.ProcessingTime
# MAGIC 
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .format("memory")        
# MAGIC     .queryName("isphits")     
# MAGIC     .outputMode("complete") 
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()

# COMMAND ----------

# MAGIC %md ## Console Output

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.ProcessingTime
# MAGIC 
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .format("console")        
# MAGIC     .outputMode("complete") 
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()

# COMMAND ----------

# MAGIC %md ## File Output with Partitions

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC var streamingSelectDF = 
# MAGIC   streamingInputDF
# MAGIC    .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"),    get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"), date_format(get_json_object(($"value").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))
# MAGIC     .groupBy($"zip") 
# MAGIC     .count()
# MAGIC     .as[(String, String)]

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.ProcessingTime
# MAGIC 
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .format("parquet")
# MAGIC     .option("path", "/mnt/sample/test-data")
# MAGIC     .option("checkpointLocation", "/mnt/sample/check")
# MAGIC     .partitionBy("zip", "day")
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()

# COMMAND ----------

# MAGIC %md ##### Create Table

# COMMAND ----------

# MAGIC %sql CREATE EXTERNAL TABLE  test_par
# MAGIC     (hittime string)
# MAGIC     PARTITIONED BY (zip string, day string)
# MAGIC     STORED AS PARQUET
# MAGIC     LOCATION '/mnt/sample/test-data'

# COMMAND ----------

# MAGIC %md ##### Add Partition

# COMMAND ----------

# MAGIC %sql ALTER TABLE test_par ADD IF NOT EXISTS
# MAGIC     PARTITION (zip='38907', day='08.02.2017') LOCATION '/mnt/sample/test-data/zip=38907/day=08.02.2017'

# COMMAND ----------

# MAGIC %md ##### Select

# COMMAND ----------

# MAGIC %sql select * from test_par

# COMMAND ----------

# MAGIC %md ## JDBC Sink

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql._
# MAGIC 
# MAGIC class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String)] {
# MAGIC       val driver = "com.mysql.jdbc.Driver"
# MAGIC       var connection:Connection = _
# MAGIC       var statement:Statement = _
# MAGIC       
# MAGIC     def open(partitionId: Long,version: Long): Boolean = {
# MAGIC         Class.forName(driver)
# MAGIC         connection = DriverManager.getConnection(url, user, pwd)
# MAGIC         statement = connection.createStatement
# MAGIC         true
# MAGIC       }
# MAGIC 
# MAGIC       def process(value: (String, String)): Unit = {
# MAGIC         statement.executeUpdate("INSERT INTO zip_test " + 
# MAGIC                 "VALUES (" + value._1 + "," + value._2 + ")")
# MAGIC       }
# MAGIC 
# MAGIC       def close(errorOrNull: Throwable): Unit = {
# MAGIC         connection.close
# MAGIC       }
# MAGIC    }

# COMMAND ----------

# MAGIC %scala
# MAGIC val url="jdbc:mysql://<mysqlserver>:3306/test"
# MAGIC val user ="user"
# MAGIC val pwd = "pwd"
# MAGIC 
# MAGIC val writer = new JDBCSink(url,user, pwd)
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .foreach(writer)
# MAGIC     .outputMode("update")
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()

# COMMAND ----------

# MAGIC %md ## Kafka Sink

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import kafkashaded.org.apache.kafka.clients.producer._
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC 
# MAGIC 
# MAGIC  class  KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)] {
# MAGIC       val kafkaProperties = new Properties()
# MAGIC       kafkaProperties.put("bootstrap.servers", servers)
# MAGIC       kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
# MAGIC       kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
# MAGIC       val results = new scala.collection.mutable.HashMap[String, String]
# MAGIC       var producer: KafkaProducer[String, String] = _
# MAGIC 
# MAGIC       def open(partitionId: Long,version: Long): Boolean = {
# MAGIC         producer = new KafkaProducer(kafkaProperties)
# MAGIC         true
# MAGIC       }
# MAGIC 
# MAGIC       def process(value: (String, String)): Unit = {
# MAGIC           producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
# MAGIC       }
# MAGIC 
# MAGIC       def close(errorOrNull: Throwable): Unit = {
# MAGIC         producer.close()
# MAGIC       }
# MAGIC    }

# COMMAND ----------

# MAGIC %scala
# MAGIC val topic = "<topic2>"
# MAGIC val brokers = "<server:ip>"
# MAGIC 
# MAGIC val writer = new KafkaSink(topic, brokers)
# MAGIC 
# MAGIC val query =
# MAGIC   streamingSelectDF
# MAGIC     .writeStream
# MAGIC     .foreach(writer)
# MAGIC     .outputMode("update")
# MAGIC     .trigger(ProcessingTime("25 seconds"))
# MAGIC     .start()
