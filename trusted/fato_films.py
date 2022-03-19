# Databricks notebook source
path_fato_films_raw = "/FileStore/tables/swapi_dev/raw/films.parquet"

# COMMAND ----------

fato_films_df = spark.read.parquet(path_fato_films_raw)
display(fato_films_df)

# COMMAND ----------

from pyspark.sql.functions import *

fato_films_df = (fato_films_df
            .withColumn("characters", explode("characters"))
            .withColumn("planets", explode("planets"))
            .withColumn("species", explode("species"))
            .withColumn("starships", explode("starships"))
            .withColumn("vehicles", explode("vehicles"))
)

# COMMAND ----------

display(fato_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de filmes
fato_films_df = (
        fato_films_df.select(
        fato_films_df.url, 
        fato_films_df.vehicles,
        fato_films_df.characters,
        fato_films_df.planets,
        fato_films_df.species, 
        fato_films_df.starships,
        fato_films_df.created,
        fato_films_df.edited  
    )
)

# COMMAND ----------

display(fato_films_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

fato_films_df = (fato_films_df
                .withColumn("id_film",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_vehicles",reverse(split(reverse(col("vehicles")),"/").getItem(1)))
                .withColumn("id_people",reverse(split(reverse(col("characters")),"/").getItem(1)))
                .withColumn("id_planets",reverse(split(reverse(col("planets")),"/").getItem(1)))
                .withColumn("id_species",reverse(split(reverse(col("species")),"/").getItem(1)))
                .withColumn("id_starships",reverse(split(reverse(col("starships")),"/").getItem(1)))
                .select("id_film",  "id_vehicles", "id_people", "id_planets", "id_species", "id_starships")
           )
display(fato_films_df)



# COMMAND ----------

# Alteração do schema do dataframe
fato_films_df = (
                fato_films_df.select(
                    col("id_film").cast('int'), 
                    col("id_vehicles").cast('int'), 
                    col("id_people").cast('int'), 
                    col("id_planets").cast('int'), 
                    col("id_species").cast('int'), 
                    col("id_starships").cast('int')
                )
)

# COMMAND ----------

fato_films_df.printSchema()

# COMMAND ----------

display(fato_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_fato_films = '/FileStore/tables/swapi_dev/trusted/fato_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(fato_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_fato_films)
)
     