# Databricks notebook source
path_planets_transient = "/FileStore/tables/swapi_dev/transient/planets/"

# COMMAND ----------

df_planets = spark.read.json(path_planets_transient)

# COMMAND ----------

display(df_planets)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_planets_raw = '/FileStore/tables/swapi_dev/raw/planets.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_planets.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_planets_raw)
)

# COMMAND ----------

