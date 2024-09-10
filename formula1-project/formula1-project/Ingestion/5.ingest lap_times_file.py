# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv("/mnt/formula1dlgen2/raw/lap_times")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/lap_times

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgen2/processed/lap_times"))