# Databricks notebook source
path_starships_films_raw = "/FileStore/tables/swapi_dev/raw/starships.parquet"

# COMMAND ----------

starships_films_df = spark.read.parquet(path_starships_films_raw)

# COMMAND ----------

display(starships_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de planets_films
starships_films_df = (
            starships_films_df.select( 
                starships_films_df.url,
                starships_films_df.films        
    )
)

# COMMAND ----------

display(starships_films_df)

# COMMAND ----------

from pyspark.sql.functions import *

starships_films_df = (starships_films_df
            .withColumn("id_films", explode("films"))
)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

starships_films_df = (starships_films_df
                .withColumn("id_starships",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_films",reverse(split(reverse(col("id_films")),"/").getItem(1)))
                .select(
                    "id_starships", 
                    "id_films"      
                )
           )
display(starships_films_df)

# COMMAND ----------

# Alteração do schema do dataframe
starships_films_df = (
                starships_films_df.select(
                    col("id_starships").cast('int'), 
                    col("id_films").cast('int')
                )

)

# COMMAND ----------

starships_films_df.printSchema()

# COMMAND ----------

display(starships_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_starships_films = '/FileStore/tables/swapi_dev/trusted/starships_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(starships_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_starships_films)
)

# COMMAND ----------

