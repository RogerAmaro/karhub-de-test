# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import * 
from slugify import slugify


# COMMAND ----------

dfProdutosTable = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/bronze/produtostablebronze"))

# COMMAND ----------

dfProdutosTable = dfProdutosTable.withColumn("id", sha2(concat(*dfProdutosTable.columns),256))

# COMMAND ----------

dfProdutosTable.columns

# COMMAND ----------

dfProdutosTable = dfProdutosTable.groupBy('nome-sku', 'fabricante','codigo', 'composicao', 'categoria').pivot("nome-atributo").agg(first("valor-atributo"))

# COMMAND ----------

dfProdutosTable = dfProdutosTable.withColumn("dt_insert", current_timestamp())

# COMMAND ----------

columns_to_float = ["altura-cm", "comprimento-cm", "largura-cm", "numero-de-espirais", "peso-bruto-kg", "peso-liquido-kg"]

# COMMAND ----------

for c in dfProdutosTable.columns:
    slug_column = slugify(c)
    dfProdutosTable = dfProdutosTable.withColumnRenamed(c, slug_column)
    if slug_column in columns_to_float:
        dfProdutosTable = dfProdutosTable.withColumn(slug_column, regexp_replace(slug_column, ',', '.').cast('float'))


# COMMAND ----------

(dfProdutosTable.write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/silver/produtostablesilver/") 
  .saveAsTable("produtostablesilver"))
