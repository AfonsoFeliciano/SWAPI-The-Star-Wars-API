# Databricks notebook source
from pyspark.sql.functions import reverse, split, col, explode

# COMMAND ----------

path_planets_films_raw = "/FileStore/tables/swapi_dev/raw/planets.parquet"

# COMMAND ----------

planets_films_df = spark.read.parquet(path_planets_films_raw)

# COMMAND ----------

display(planets_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de planets_films
planets_films_df = (
            planets_films_df.select( 
                planets_films_df.url,
                planets_films_df.films        
    )
)

# COMMAND ----------

display(planets_films_df)

# COMMAND ----------

planets_films_df = (planets_films_df
            .withColumn("id_films", explode("films"))
)

# COMMAND ----------

planets_films_df = (planets_films_df
                .withColumn("id_planet",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_films",reverse(split(reverse(col("id_films")),"/").getItem(1)))
                .select(
                    "id_planet", 
                    "id_films"      
                )
           )
display(planets_films_df)

# COMMAND ----------

# Alteração do schema do dataframe
planets_films_df = (
                planets_films_df.select(
                    col("id_planet").cast('int'), 
                    col("id_films").cast('int')
                )

)

# COMMAND ----------

planets_films_df.printSchema()

# COMMAND ----------

display(planets_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_planets_films = '/FileStore/tables/swapi_dev/trusted/relacional/planets_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(planets_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_planets_films)
)
     