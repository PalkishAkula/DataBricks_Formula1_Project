# Databricks notebook source
constructor_schema ="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df=spark.read \
    .schema(constructor_schema) \
    .json("/mnt/formula1dlgen2/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df =constructor_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructor_final_df= constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/constructors

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgen2/processed/constructors"))