# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiline", True) \
    .json("/mnt/formula1dlgen2/raw/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgen2/processed/pit_stops"))