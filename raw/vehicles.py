# Databricks notebook source
path_vehicles_transient = "/FileStore/tables/swapi_dev/transient/vehicles/"

# COMMAND ----------

df_vehicles = spark.read.json(path_vehicles_transient)

# COMMAND ----------

display(df_vehicles)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_vehicles_raw = '/FileStore/tables/swapi_dev/raw/vehicles.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_vehicles.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_vehicles_raw)
)

# COMMAND ----------

