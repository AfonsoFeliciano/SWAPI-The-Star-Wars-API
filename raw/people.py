# Databricks notebook source
path_people_transient = "/FileStore/tables/swapi_dev/transient/people/"

# COMMAND ----------

df_people = spark.read.json(path_people_transient)

# COMMAND ----------

display(df_people)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_people_raw = '/FileStore/tables/swapi_dev/raw/people.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_people.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_people_raw)
)

# COMMAND ----------

