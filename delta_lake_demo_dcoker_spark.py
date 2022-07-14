# Databricks notebook source
import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.pandas as ps

# COMMAND ----------

# ran on jupyter/pyspark-notebook:spark-3.2.1
pyspark.__version__

# COMMAND ----------

!pip install delta-spark

# COMMAND ----------

# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#                     .appName('deltalake_demo') \
#                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#                     .getOrCreate()

from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

# COMMAND ----------

spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

data = [(1001,'Apple Mac Book',11,"I"),(1002,"Apple IPhone 11",22,"I"),(1003,'Redmi Note 4',33, 'I'),(1004,'Dell Inspiron',44,'I')]
columns = ["product_id","product_name","stocks_left","row_flag"]
# product_sdf = spark.createDataFrame(data).toDF(*columns)
product_sdf = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

product_sdf.show()

# COMMAND ----------

product_sdf.coalesce(2).write.mode("overwrite").format("delta").save("output_data/products")

# COMMAND ----------

products_dlt_sdf = spark.read.format("delta").load("output_data/products")

# COMMAND ----------

products_dlt = DeltaTable.forPath(spark, "output_data/products")

# COMMAND ----------

products_dlt.history().columns

# COMMAND ----------

products_dlt.history().selectExpr("version","timestamp","userName","operation","operationParameters","operationMetrics").show(truncate=False)

# COMMAND ----------


