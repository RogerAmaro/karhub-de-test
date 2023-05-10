# Databricks notebook source
# DBTITLE 1,Camada silver dos dados de alias.
# MAGIC %md
# MAGIC faz a leitura da tabela de alias da camada bronze, aplica o slug e então salva os dados na camada silver.

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

# lendo os dados da tabela de alias da camada bronze.

dfAliasBronze = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/aliastablebronze"))

# COMMAND ----------

# aplicando slug nos nomes das colunas

for c in dfAliasBronze.columns:
    dfAliasBronze = dfAliasBronze.withColumnRenamed(c, slugify(c))

# COMMAND ----------

# grava os dados na camada silver

(dfAliasBronze.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/silver/aliastablesilver/") 
  .saveAsTable("aliastablesilver"))
