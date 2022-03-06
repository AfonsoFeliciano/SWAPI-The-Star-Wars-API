# Databricks notebook source
path_all_calls_transient = "/FileStore/tables/swapi_dev/transient/allcalls/allcalls.json"

# COMMAND ----------

df_all_calls = spark.read.json(path_all_calls_transient)

# COMMAND ----------

display(df_all_calls)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_all_calls_raw = '/FileStore/tables/swapi_dev/raw/allcalls.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_all_calls.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_all_calls_raw)
)