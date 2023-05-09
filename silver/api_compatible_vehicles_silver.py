# Databricks notebook source
from pyspark.sql.functions import * 
from delta.tables import *

# COMMAND ----------

dfApiCompatibleVehiclesBronze = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/compatiblevehiclestablebronze"))

# COMMAND ----------

dfApiCompatibleVehiclesBronze = dfApiCompatibleVehiclesBronze.withColumn("complemento", explode(split(dfApiCompatibleVehiclesBronze.complemento, "\|")))


# COMMAND ----------

dfApiCompatibleVehiclesBronze = dfApiCompatibleVehiclesBronze.withColumn("id", sha2(concat("ano",'complemento',"codigo-fabricante", 'marca', 'modelo'),256))

# COMMAND ----------

dfCompatibleVehiclestableSilver = DeltaTable.forPath(spark, 'gs://karhubdatalake/silver/compatiblevehiclestablesilver')
dfCompatibleVehiclestableSilver.alias('compatiblevehiclestable') \
  .merge(
    dfApiCompatibleVehiclesBronze.alias('compatiblevehiclestableUpdates'),
    'compatiblevehiclestable.id = compatiblevehiclestableUpdates.id'
  ) \
  .whenMatchedUpdate(set =
    {
      "ano": "compatiblevehiclestableUpdates.ano",
      "complemento": "compatiblevehiclestableUpdates.complemento",
      "`codigo-fabricante`": "compatiblevehiclestableUpdates.`codigo-fabricante`",
      "fabricante": "compatiblevehiclestableUpdates.fabricante",
      "marca": "compatiblevehiclestableUpdates.marca",
      "modelo": "compatiblevehiclestableUpdates.modelo",
      "`dt-insert`": "compatiblevehiclestableUpdates.`dt-insert`",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
       "ano": "compatiblevehiclestableUpdates.ano",
      "complemento": "compatiblevehiclestableUpdates.complemento",
      "`codigo-fabricante`": "compatiblevehiclestableUpdates.`codigo-fabricante`",
      "fabricante": "compatiblevehiclestableUpdates.fabricante",
      "marca": "compatiblevehiclestableUpdates.marca",
      "modelo": "compatiblevehiclestableUpdates.modelo",
      "`dt-insert`": "compatiblevehiclestableUpdates.`dt-insert`",
      "id":"compatiblevehiclestableUpdates.id",
    }
  ) \
  .execute()
