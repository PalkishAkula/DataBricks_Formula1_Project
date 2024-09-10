# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

name_schema =  StructType(fields=[StructField("forename", StringType(),True),
                                   StructField("surname", StringType(),True)])

# COMMAND ----------

drivers_schema= StructType(fields=[StructField("driverId", IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number", IntegerType(),True),
                                    StructField("code", StringType(),True),
                                    StructField("name",name_schema),
                                    StructField("dob", DateType(),True),
                                    StructField("nationality", StringType(),True),
                                    StructField("url", StringType(),True)])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json("/mnt/formula1dlgen2/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

driver_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgen2/processed/drivers"))