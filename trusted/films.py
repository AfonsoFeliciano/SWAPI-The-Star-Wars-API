# Databricks notebook source
path_films_raw = "/FileStore/tables/swapi_dev/raw/films.parquet"

# COMMAND ----------

films_df = spark.read.parquet(path_films_raw)
display(films_df)

# COMMAND ----------

#from pyspark.sql.functions import *

#films_df = (films_df
            #.withColumn("characters", explode("characters"))
            #.withColumn("planets", explode("planets"))
            #.withColumn("species", explode("species"))
            #.withColumn("starships", explode("starships"))
            #.withColumn("vehicles", explode("vehicles"))
#)

# COMMAND ----------

display(films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de filmes
films_df = (
        films_df.select(
        films_df.title, 
        films_df.director,
        films_df.producer,
        films_df.opening_crawl,
        films_df.release_date, 
        films_df.episode_id,
        films_df.url, 
        films_df.created,
        films_df.edited  
    )
)

# COMMAND ----------

display(films_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

films_df = (films_df
                .withColumn("id_film",reverse(split(reverse(col("url")),"/").getItem(1)))
                .select("id_film",  "episode_id", "title", "director", "producer", "opening_crawl", "release_date", "url", "created", "edited")
           )
display(films_df)



# COMMAND ----------

# Alteração do schema do dataframe
films_df = (
                films_df.select(
                    col("id_film").cast('int'), 
                    col("episode_id").cast('int'), 
                    col("title").cast('string'), 
                    col("director").cast('string'), 
                    col("producer").cast('string'), 
                    col("opening_crawl").cast('string'), 
                    col("release_date").cast('date'), 
                    col("url").cast('string'), 
                    col("created").cast('timestamp'), 
                    col("edited").cast('timestamp')

                )

)

# COMMAND ----------

films_df.printSchema()

# COMMAND ----------

display(films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_films = '/FileStore/tables/swapi_dev/trusted/films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_films)
)
     