# Databricks notebook source
# DBTITLE 1,Ingestão aquivo de alias.
# MAGIC %md
# MAGIC Ingestão do arquivo CSV que localiza-se na camada leading zone, armazenado no google cloud storage, um dataframe é criado e então adicionado o campo 
# MAGIC dt_time com o timestemp e então os dados são salvos na camada bronze no formato delta.

# COMMAND ----------

# import das bibliotecas necessárias

from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

# lê o arquivo CSV e cria uma dataframe

DfAlias = (spark.read.format("csv")
        .option("header", True)
        .load("gs://karhubdatalake/leading/Karhub-alias.csv"))

# COMMAND ----------

# adiciona a coluna dt_insert com o timestamp da ingestão do arquivo

DfAlias = DfAlias.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

# padronização da nomenclatura nas colunas dp dataframe

for c in DfAlias.columns:
    DfAlias = DfAlias.withColumnRenamed(c, slugify(c))

# COMMAND ----------

# salva os dados em formato delta na camada bronze do datalake e cria uma tabela para consultas sql no databricks

(DfAlias.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/bronze/aliastablebronze/") 
  .saveAsTable("aliastablebronze"))
