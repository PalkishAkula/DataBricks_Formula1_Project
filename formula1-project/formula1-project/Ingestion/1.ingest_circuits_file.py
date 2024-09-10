# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema =  StructType(fields=[StructField("circuitId",IntegerType(),False),
                                       StructField("circuitRef",StringType(),True),
                                       StructField("name",StringType(),True),
                                       StructField("location",StringType(),True),
                                       StructField("country",StringType(),True),
                                       StructField("lat",DoubleType(),True),
                                       StructField("lng",DoubleType(),True),
                                       StructField("alt",IntegerType(),True),
                                       StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude")

# COMMAND ----------


circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 


# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgen2/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)