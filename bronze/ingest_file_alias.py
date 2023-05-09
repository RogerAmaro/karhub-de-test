# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

DfAlias = (spark.read.format("csv")
        .option("header", True)
        .load("gs://karhubdatalake/leading/Karhub-alias.csv"))

# COMMAND ----------

DfAlias = DfAlias.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

for c in DfAlias.columns:
    DfAlias = DfAlias.withColumnRenamed(c, slugify(c))

# COMMAND ----------

DfAlias.display()

# COMMAND ----------

(DfAlias.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/aliastablebronze/") 
  .saveAsTable("aliastablebronze"))
