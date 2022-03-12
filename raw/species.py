# Databricks notebook source
path_species_transient = "/FileStore/tables/swapi_dev/transient/species/"

# COMMAND ----------

df_species = spark.read.json(path_species_transient)

# COMMAND ----------

display(df_species)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_species_raw = '/FileStore/tables/swapi_dev/raw/species.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_species.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_species_raw)
)

# COMMAND ----------

