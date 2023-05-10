# Databricks notebook source
# DBTITLE 1,Upsert nos dados da camada silver da tabela de veículos compatíveis.
# MAGIC %md
# MAGIC recuperado os dados de veiculos compativeis salvos na camada bronze, e então explode a coluna complemento em novas linhas, cria um id unico e então faz o upsert dos dados na camada silver

# COMMAND ----------

from pyspark.sql.functions import * 
from delta.tables import *

# COMMAND ----------

# lendo os dados ta tabela de veiculos compativeis na camada bronze.

dfApiCompatibleVehiclesBronze = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/compatiblevehiclestablebronze"))

# COMMAND ----------

# sobrescreve a coluna 'complemento' com o resultado do split dos dados + explode, tranformando cada item divido por '|' em uma nova linha.

dfApiCompatibleVehiclesBronze = dfApiCompatibleVehiclesBronze.withColumn("complemento", explode(split(dfApiCompatibleVehiclesBronze.complemento, "\|")))

# COMMAND ----------

# podem ser adicionados aos dados de retorno da api, novos dados, adições e exclusões, desse modo faz-se necessário um campo de id que seja unico para podermos 
# perfomar um upsert nos dados da camada silver, desse modo sempre estaremos com os mesmo dados da api.
# o campo de id é criado concatenando o campo ano, complemento, codigo-fabricante, marca e modelo, e então é criado um hash da string resultante

dfApiCompatibleVehiclesBronze = dfApiCompatibleVehiclesBronze.withColumn("id", sha2(concat("ano",'complemento',"codigo-fabricante", 'marca', 'modelo'),256))

# COMMAND ----------

# upsert nos dados da tabela da camada silver, isso só é possível pois o arquivo no formato delta adiciona uma camada de abstração no arquivo parquet trazendo a possibilidades de termos transações ACID


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
