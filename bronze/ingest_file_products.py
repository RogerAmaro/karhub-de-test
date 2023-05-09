# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

import pyspark.pandas as pd
dfProdutos  = pd.read_excel("gs://karhubdatalake/leading/karhub_autoparts_1.xlsx").to_spark()
dfProdutos.display()

# COMMAND ----------

dfProdutos = dfProdutos.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

for c in dfProdutos.columns:
    dfProdutos = dfProdutos.withColumnRenamed(c, slugify(c))

# COMMAND ----------

(dfProdutos.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/produtostablebronze/") 
  .saveAsTable("produtostablebronze"))
