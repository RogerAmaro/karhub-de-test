# Databricks notebook source
# DBTITLE 1,Pivot nos campos nome-atributo e valor-atributo
# MAGIC %md
# MAGIC
# MAGIC cria um id único para a tabela de produtos, depois faz o pivot dos campos nome-atributo e valor-atributo, convert algumas colunas para float e subistirui virgulas por pontos nos dados, por fim salva os dados na camada silver.

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

# recupera a tabela de produtos da camada bronze e então cria uma dataframe

dfProdutosTable = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/produtostablebronze"))

# COMMAND ----------

# cria o campo de id único concatenando todas as colunas do dataframe e então cria um hash

dfProdutosTable = dfProdutosTable.withColumn("id", sha2(concat(*dfProdutosTable.columns),256))

# COMMAND ----------

# faz o pivot dos campos nome-atributo e valor-atributo.

dfProdutosTable = dfProdutosTable.groupBy('nome-sku', 'fabricante','codigo', 'composicao', 'categoria').pivot("nome-atributo").agg(first("valor-atributo"))

# COMMAND ----------

# cria o campo dt-insert com o timestamp

dfProdutosTable = dfProdutosTable.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

# cria uma lista de colunas que serão transformadas em float

columns_to_float = ["altura-cm", "comprimento-cm", "largura-cm", "numero-de-espirais", "peso-bruto-kg", "peso-liquido-kg"]

# COMMAND ----------

# transforma as colunas na lista 'columns_to_float' em float e remove as vírgula dessas colunas subistituindo-as por pontos.


for c in dfProdutosTable.columns:
    slug_column = slugify(c)
    dfProdutosTable = dfProdutosTable.withColumnRenamed(c, slug_column)
    if slug_column in columns_to_float:
        dfProdutosTable = dfProdutosTable.withColumn(slug_column, regexp_replace(slug_column, ',', '.').cast('float'))


# COMMAND ----------

# grava os dados resultantes na camada silver.


(dfProdutosTable.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/silver/produtostablesilver/") 
  .saveAsTable("produtostablesilver"))
