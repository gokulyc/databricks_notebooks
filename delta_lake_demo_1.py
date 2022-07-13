# Databricks notebook source
import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.pandas as ps

# COMMAND ----------

pyspark.__version__

# COMMAND ----------

spark.conf.get("spark.sql.execution.arrow.enabled")

# COMMAND ----------



# COMMAND ----------

data = [(1001,'Apple Mac Book',11,"I"),(1002,"Apple IPhone 11",22,"I"),(1003,'Redmi Note 4',33, 'I'),(1004,'Dell Inspiron',44,'I')]
columns = ["product_id","product_name","stocks_left","row_flag"]
# product_sdf = spark.createDataFrame(data).toDF(*columns)
product_sdf = spark.createDataFrame(data, schema=columns)

# COMMAND ----------


display(product_sdf)

# COMMAND ----------

product_sdf.coalesce(2).write.mode("overwrite").format("delta").saveAsTable("target_table")
# .save("dbfs:/FileStore/output_data/products_delta")

# COMMAND ----------

display(product_sdf)

# COMMAND ----------

data_1 = [(1001, 'Apple Mac Book',110, "U"),(1002,'Apple IPhone 11',2,"U"),(1003,"Redmi Note 4",3,"U"),(1004,"Dell Inspiron",0,"D")]
merge_product_df = spark.createDataFrame(data_1, schema=columns)
display(merge_product_df)

# COMMAND ----------

# product_sdf.createOrReplaceTempView("target_table")
merge_product_df.createOrReplaceTempView("src_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from src_table;

# COMMAND ----------

output = spark.sql("""
MERGE INTO target_table 
using  src_table
on src_table.product_id = target_table.product_id
When Matched and src_table.row_flag='U'
Then 
Update set *
When Matched and src_table.row_flag='D'
Then delete
When not Matched and src_table.row_flag='I'
then insert *
""")

# COMMAND ----------

display(output)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from target_table;

# COMMAND ----------


