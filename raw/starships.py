# Databricks notebook source
path_starships_transient = "/FileStore/tables/swapi_dev/transient/starships/"

# COMMAND ----------

df_starships = spark.read.json(path_starships_transient)

# COMMAND ----------

display(df_starships)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_starships_raw = '/FileStore/tables/swapi_dev/raw/starships.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_starships.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_starships_raw)
)

# COMMAND ----------

