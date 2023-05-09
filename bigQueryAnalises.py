# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","cadastro_produto")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from produtostablesilver

# COMMAND ----------

df = spark.read.format("bigquery").option("credentialsFile", "/dbfs/FileStore/gcloud/key/karhub_261f144c9490.json") \
      .option("parentProject", "karhub") \
      .option("project", "karhub") \
      .option("table",'kh_data_engineer_teste_roger_amaro') \
      .load()

# COMMAND ----------

df.filter("produto_numero_de_espirais is not null").display()

# COMMAND ----------

for c in ["produto_peso_bruto_kg", "produto_peso_liquido_kg", "produto_numero_de_espirais"]:
        df = df.withColumn(c, round(col(c).cast("float"), 1))

# COMMAND ----------

## Quantos produtos únicos temos na base?

df.filter("produto_codigo_original_fabricante is not null").select("produto_codigo_original_fabricante").distinct().count()

# COMMAND ----------

#Quantos produtos únicos temos na base por categoria ?

df.groupBy("produto_categoria").agg(countDistinct("produto_codigo_original_fabricante").alias("qtd_produto")).display()


# COMMAND ----------

#Quantos veículos únicos (mesma marca e modelo) temos na base por produto?

df.filter("produto_codigo_original_fabricante is not null").groupBy("produto_codigo_original_fabricante").agg(countDistinct("veiculo_marca", "veiculo_modelo").alias("qtd_veiculos")).display()


# COMMAND ----------

## qual veículo tem maior disponibilidade de peças?

df.filter("veiculo_marca is not null").groupBy("veiculo_marca", "veiculo_modelo").agg(countDistinct("produto_codigo_original_fabricante").alias("qtd_produtos")).select(max("veiculo_marca").alias("marca"), max("veiculo_modelo").alias('modelo'), max('qtd_produtos').alias('quantidade de produtos disponíveis')).display()


# COMMAND ----------

## qual ano possui mais peças disponíveis?
df.filter("veiculo_marca is not null").groupBy("veiculo_ano").agg(countDistinct("produto_codigo_original_fabricante").alias("qtd_produtos")).select(max("veiculo_ano").alias("Veículo Ano"), max('qtd_produtos').alias('quantidade de produtos disponíveis')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

## quantos veículos não possuem nenhuma peça compativel?

df.groupBy("veiculo_marca", "veiculo_modelo").agg(countDistinct("produto_codigo_original_fabricante").alias("qtd_produtos")).display()

