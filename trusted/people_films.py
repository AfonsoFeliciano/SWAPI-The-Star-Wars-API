# Databricks notebook source
path_people_films_raw = "/FileStore/tables/swapi_dev/raw/films.parquet"

# COMMAND ----------

people_films_df = spark.read.parquet(path_people_films_raw)
display(people_films_df)

# COMMAND ----------

from pyspark.sql.functions import *

people_films_df = (people_films_df
            .withColumn("characters", explode("characters"))
)

# COMMAND ----------

display(people_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de filmes
people_films_df = (
        people_films_df.select(
        people_films_df.characters, 
        people_films_df.url
    )
)

# COMMAND ----------

display(people_films_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col, monotonically_increasing_id

people_films_df = (people_films_df
                .withColumn("id_people_films", monotonically_increasing_id() + 1)
                .withColumn("id_people",reverse(split(reverse(col("characters")),"/").getItem(1)))
                .withColumn("id_film",reverse(split(reverse(col("url")),"/").getItem(1)))
                .select("id_people_films", "id_people", "id_film")
           )
display(people_films_df)

# COMMAND ----------

# Alteração do schema do dataframe
people_films_df = (
                people_films_df.select(
                    col("id_people_films").cast('int'),
                    col("id_people").cast('int'),
                    col("id_film").cast('int')  
                )
)

# COMMAND ----------

people_films_df.printSchema()

# COMMAND ----------

display(people_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_people_films = '/FileStore/tables/swapi_dev/trusted/people_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(people_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_people_films)
)

# COMMAND ----------

