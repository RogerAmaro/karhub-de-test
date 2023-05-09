# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

dfAliasBronze = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/aliastablebronze"))

# COMMAND ----------

for c in dfAliasBronze.columns:
    dfAliasBronze = dfAliasBronze.withColumnRenamed(c, slugify(c))

# COMMAND ----------

(dfAliasBronze.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/silver/aliastablesilver/") 
  .saveAsTable("aliastablesilver"))

# COMMAND ----------

# %sql

# select * from aliastablesilver
