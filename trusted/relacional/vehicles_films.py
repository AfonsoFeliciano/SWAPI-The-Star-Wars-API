# Databricks notebook source
from pyspark.sql.functions import reverse, split, col, explode

# COMMAND ----------

path_vehicles_films_raw = "/FileStore/tables/swapi_dev/raw/vehicles.parquet"

# COMMAND ----------

vehicles_films_df = spark.read.parquet(path_vehicles_films_raw)

# COMMAND ----------

display(vehicles_films_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de planets_films
vehicles_films_df = (
            vehicles_films_df.select( 
                vehicles_films_df.url,
                vehicles_films_df.films        
    )
)

# COMMAND ----------

display(vehicles_films_df)

# COMMAND ----------

vehicles_films_df = (vehicles_films_df
            .withColumn("id_films", explode("films"))
)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film


vehicles_films_df = (vehicles_films_df
                .withColumn("id_vehicles",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_films",reverse(split(reverse(col("id_films")),"/").getItem(1)))
                .select(
                    "id_vehicles", 
                    "id_films"      
                )
           )
display(vehicles_films_df)

# COMMAND ----------

# Alteração do schema do dataframe
vehicles_films_df = (
                vehicles_films_df.select(
                    col("id_vehicles").cast('int'), 
                    col("id_films").cast('int')
                )

)

# COMMAND ----------

vehicles_films_df.printSchema()

# COMMAND ----------

display(vehicles_films_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_vehicles_films = '/FileStore/tables/swapi_dev/trusted/relacional/vehicles_films.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(vehicles_films_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_vehicles_films)
)

# COMMAND ----------

