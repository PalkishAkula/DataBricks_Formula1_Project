# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

races_df =spark.read \
    .option("header", "true") \
    .schema(races_schema) \
    .csv("/mnt/formula1dlgen2/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgen2/processed/races"))