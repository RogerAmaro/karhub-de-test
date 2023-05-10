# Databricks notebook source
# DBTITLE 1,Ingestão do arquivo de produtos.
# MAGIC %md
# MAGIC
# MAGIC faz a ingestão do arquivo de produtos, cria um dataframe e adiciona ele o campo de dt_insert como timestemp da ingestão, padroniza a nomenclatura das colunas e então salva os dados na camada bronze do datalake.

# COMMAND ----------

# import datas bibliotecas necessárias

from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

# utiliza a pyspark pandas para poder fazer a leitura do formato xlsx, cria uma daframe

import pyspark.pandas as pd
dfProdutos  = pd.read_excel("gs://karhubdatalake/leading/karhub_autoparts_1.xlsx").to_spark()

# COMMAND ----------

# cria a coluna dt_insert como timestamp da ingestão.

dfProdutos = dfProdutos.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

# slugfy nas colunas do daframe para remover caracteres especiais.

for c in dfProdutos.columns:
    dfProdutos = dfProdutos.withColumnRenamed(c, slugify(c))

# COMMAND ----------

# salva os dados na camada bronze do datalake e cria uma tabela para consultas sql no databricks

(dfProdutos.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/produtostablebronze/") 
  .saveAsTable("produtostablebronze"))
