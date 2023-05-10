# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# leitura da tabela alias da camada silver

dfAliasSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/aliastablesilver"))


# COMMAND ----------

# leitura da tabela de veiculos compatíveis e renomeação das colunas adicionado o prefixo 'veiculo' em todas as colunas da tabela


dfCompatibleVehiclesTableSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/compatiblevehiclestablesilver"))
        
for c in dfCompatibleVehiclesTableSilver.columns:
    dfCompatibleVehiclesTableSilver = dfCompatibleVehiclesTableSilver.withColumnRenamed(c, "-".join(["veiculo", c]))

dfCompatibleVehiclesTableSilver = dfCompatibleVehiclesTableSilver.withColumnRenamed("veiculo-codigo-fabricante", "veiculo-codigo-produto-compativel")

# COMMAND ----------

# le a tabela de produtos da camada silver e adiciona o prefixo 'protudo' nas colunas.

dfProdutosTableSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/produtostablesilver"))
        
for c in dfProdutosTableSilver.columns:
    dfProdutosTableSilver = dfProdutosTableSilver.withColumnRenamed(c, "-".join(["produto", c]))



# COMMAND ----------

# para obtermos o código original dos produtos disponivel na tabela de alias, é feito join utilizando a produto-codigo e o alias

dfProdutosTableSilver = (dfProdutosTableSilver
                .join(dfAliasSilver, dfProdutosTableSilver['produto-codigo'] ==  dfAliasSilver['alias'], how='left')
                .select(dfProdutosTableSilver["*"],dfAliasSilver["codigo-fabricante"].alias("produto-codigo-original-fabricante")))

# COMMAND ----------

## agora com o codigo original já disponivel, é feito mais um join entre a tabela resultante do comando 5 e a tabela de veiculos compativeis, utilizamos 
## como chave de cruzamento o codigo original do produto e o veiculo-codigo-produto-compativel

dfProdutosTableSilver  = (dfProdutosTableSilver
                .join(dfCompatibleVehiclesTableSilver, dfProdutosTableSilver["produto-codigo-original-fabricante"]==dfCompatibleVehiclesTableSilver["veiculo-codigo-produto-compativel"],how='left')
            )

# COMMAND ----------

## removendo colunas duplicadas provinientes do join, renomeando colunas e acrescentado as colunas candidate_name e dt-insert


dfProdutosTableSilver = dfProdutosTableSilver.drop("veiculo-codigo-produto-compativel")
dfProdutosTableSilver = dfProdutosTableSilver.withColumnRenamed("produto-codigo", "produto-codigo-alias")
dfProdutosTableSilver = dfProdutosTableSilver.withColumn("candidate_name", lit("Roger Amaro"))
dfProdutosTableSilver = dfProdutosTableSilver.withColumn("dt-insert", current_timestamp())

# COMMAND ----------

# %sql 

# select prods.*, alias.`codigo-fabricante` as `codigo-produto`, cv.* from produtostablesilver as prods 
# LEFT JOIN aliastablesilver as alias 
# on prods.codigo = alias.alias
# left join compatiblevehiclestablesilver as cv
# on alias.`codigo-fabricante` = cv.`codigo-fabricante`


# COMMAND ----------

# salva o resultado na camada gold, utilizando o repartition, esses dados serão carregados no biquery, que não aceita arquivos parquet particionados, oque o repartition faz é colocar os dados em um único arquivo.


(dfProdutosTableSilver.repartition(1)
  .write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/gold/productsandcompatiblevehiclesgold/") 
  .saveAsTable("productsandcompatiblevehiclesgold"))
