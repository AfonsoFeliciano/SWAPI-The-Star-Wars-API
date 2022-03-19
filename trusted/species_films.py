# Databricks notebook source
path_species_films_raw = "/FileStore/tables/swapi_dev/raw/species.parquet"

# COMMAND ----------

species_films_df = spark.read.parquet(path_species_films_raw)

# COMMAND ----------

display(species_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de planets_films
species_films_df = (
            species_films_df.select( 
                species_films_df.url,
                species_films_df.films        
    )
)

# COMMAND ----------

display(species_films_df)

# COMMAND ----------

from pyspark.sql.functions import *

species_films_df = (species_films_df
            .withColumn("id_films", explode("films"))
)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

species_films_df = (species_films_df
                .withColumn("id_species",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_films",reverse(split(reverse(col("id_films")),"/").getItem(1)))
                .select(
                    "id_species", 
                    "id_films"      
                )
           )
display(species_films_df)

# COMMAND ----------

# Alteração do schema do dataframe
species_films_df = (
                species_films_df.select(
                    col("id_species").cast('int'), 
                    col("id_films").cast('int')
                )

)

# COMMAND ----------

species_films_df.printSchema()

# COMMAND ----------

display(species_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_species_films = '/FileStore/tables/swapi_dev/trusted/species_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(species_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_species_films)
)