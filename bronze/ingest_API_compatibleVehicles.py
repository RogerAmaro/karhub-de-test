# Databricks notebook source
# DBTITLE 1,Ingestão dos dados da API
# MAGIC %md
# MAGIC esse notebook é responsável por buscar os dados de veículos compatíveis na API e então cria um datafram, adiciona um campo de timestamp e grava os dados em uma tabela delta na primeira camada, a cadama bronze, os arquivos nessa estapa do processo estão 'as-is', uma única tranformação é necessária para adequar os nomes das colunas retirando os caracteres especiais e aplicando o slug na coluna para uma melhor padronizaçao e adequação aos padrões exigidos pelo arquivo delta.

# COMMAND ----------

import requests
from pyspark.sql.functions import *
from slugify import slugify

# COMMAND ----------

# cria a requisição para a API e cria um dataframe com o resultado

url = "https://api-data-engineer-test-3bqvkbbykq-uc.a.run.app/token=KH38INTDFRZE"
response = requests.request("GET", url)
dfApiCompatibleVehicles = spark.createDataFrame(response.json())

# COMMAND ----------

# cria a coluna dt_insert com o timestamp atual

dfApiCompatibleVehicles = dfApiCompatibleVehicles.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

# aplica slug no nome da coluna, isso é necessário pois as colunas de um arquivo parquet que utiliza delta não devem conter caracteres especiais.

for c in dfApiCompatibleVehicles.columns:
    dfApiCompatibleVehicles = dfApiCompatibleVehicles.withColumnRenamed(c, slugify(c))

# COMMAND ----------

# grava o dataframe no formato delta e cria uma tabela no databricks para consultas em sql

(dfApiCompatibleVehicles.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/compatiblevehiclestablebronze/") 
  .saveAsTable("compatiblevehiclestablebronze"))
