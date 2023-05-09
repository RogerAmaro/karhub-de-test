# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dfAliasSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/aliastablesilver"))

dfAliasSilver.display()

# COMMAND ----------

dfCompatibleVehiclesTableSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/compatiblevehiclestablesilver"))
        
for c in dfCompatibleVehiclesTableSilver.columns:
    dfCompatibleVehiclesTableSilver = dfCompatibleVehiclesTableSilver.withColumnRenamed(c, "-".join(["veiculo", c]))

dfCompatibleVehiclesTableSilver = dfCompatibleVehiclesTableSilver.withColumnRenamed("veiculo-codigo-fabricante", "veiculo-codigo-produto-compativel")

# COMMAND ----------

dfCompatibleVehiclesTableSilver.display()

# COMMAND ----------

dfProdutosTableSilver = (spark.read.format("delta")
        .option("header", True)
        .load("gs://karhubdatalake/silver/produtostablesilver"))
        
for c in dfProdutosTableSilver.columns:
    dfProdutosTableSilver = dfProdutosTableSilver.withColumnRenamed(c, "-".join(["produto", c]))

dfProdutosTableSilver.display()

# COMMAND ----------

dfProdutosTableSilver = (dfProdutosTableSilver
                .join(dfAliasSilver, dfProdutosTableSilver['produto-codigo'] ==  dfAliasSilver['alias'], how='left')
                .select(dfProdutosTableSilver["*"],dfAliasSilver["codigo-fabricante"].alias("produto-codigo-original-fabricante")))

# COMMAND ----------

dfProdutosTableSilver  = (dfProdutosTableSilver
                .join(dfCompatibleVehiclesTableSilver, dfProdutosTableSilver["produto-codigo-original-fabricante"]==dfCompatibleVehiclesTableSilver["veiculo-codigo-produto-compativel"],how='left')
            )

# COMMAND ----------

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

(dfProdutosTableSilver.repartition(1)
  .write
  .format("delta")
  .mode("overwrite") 
  .option("overwriteSchema", "true") 
  .option("path", "gs://karhubdatalake/gold/productsandcompatiblevehiclesgold/") 
  .saveAsTable("productsandcompatiblevehiclesgold"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT count(*) from productsandcompatiblevehiclesgold
