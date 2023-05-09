# Databricks notebook source
import requests
from pyspark.sql.functions import *
from slugify import slugify

# COMMAND ----------

url = "https://api-data-engineer-test-3bqvkbbykq-uc.a.run.app/token=KH38INTDFRZE"
response = requests.request("GET", url)
dfApiCompatibleVehicles = spark.createDataFrame(response.json())

# COMMAND ----------

dfApiCompatibleVehicles = dfApiCompatibleVehicles.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

for c in dfApiCompatibleVehicles.columns:
    dfApiCompatibleVehicles = dfApiCompatibleVehicles.withColumnRenamed(c, slugify(c))

# COMMAND ----------

(dfApiCompatibleVehicles.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/compatiblevehiclestablebronze/") 
  .saveAsTable("compatiblevehiclestablebronze"))
