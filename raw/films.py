# Databricks notebook source
path_films_transient = "/FileStore/tables/swapi_dev/transient/films/"

# COMMAND ----------

df_films = spark.read.json(path_films_transient)

# COMMAND ----------

display(df_films)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_films_raw = '/FileStore/tables/swapi_dev/raw/films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_films.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_films_raw)
)
