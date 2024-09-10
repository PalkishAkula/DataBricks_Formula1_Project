# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

driver_standing_df = race_results_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"),
        count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import *
driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings;