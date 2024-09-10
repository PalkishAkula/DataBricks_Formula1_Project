# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dlgen2/raw/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying;

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlgen2/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl/processed/qualifying'))
