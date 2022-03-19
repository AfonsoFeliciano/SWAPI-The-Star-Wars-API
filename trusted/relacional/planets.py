# Databricks notebook source
from pyspark.sql.functions import reverse, split, col, regexp_replace

# COMMAND ----------

path_planets_raw = "/FileStore/tables/swapi_dev/raw/planets.parquet"

# COMMAND ----------

planets_df = spark.read.parquet(path_planets_raw)

# COMMAND ----------

display(planets_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de planets
planets_df = (
            planets_df.select( 
                planets_df.name,
                planets_df.orbital_period,
                planets_df.population,
                planets_df.rotation_period,
                planets_df.surface_water,
                planets_df.terrain,
                planets_df.climate,
                planets_df.diameter,
                planets_df.gravity,
                planets_df.created,
                planets_df.edited,
                planets_df.url        
    )
)

# COMMAND ----------

display(planets_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
planets_df = (planets_df
                .withColumn("id_planet",reverse(split(reverse(col("url")),"/").getItem(1)))
                .select(
                    "id_planet", 
                    "name", 
                    "orbital_period",
                    "population",
                    "rotation_period",
                    "surface_water",
                    "terrain",
                    "climate",
                    "diameter",
                    "gravity",
                    "url",
                    "created",
                    "edited"     
                )
           )
display(planets_df)

# COMMAND ----------

planets_df = (
            planets_df
                    .withColumn('orbital_period', regexp_replace('orbital_period', 'unknown', 'null'))
                    .withColumn('population', regexp_replace('population', 'unknown', 'null'))
                    .withColumn('rotation_period', regexp_replace('rotation_period', 'unknown', 'null'))
                    .withColumn('surface_water', regexp_replace('surface_water', 'unknown', 'null'))
                    .withColumn('terrain', regexp_replace('terrain', 'unknown', 'null'))
                    .withColumn('climate', regexp_replace('climate', 'unknown', 'null'))
                    .withColumn('diameter', regexp_replace('diameter', 'unknown', 'null'))
                    .withColumn('gravity', regexp_replace('gravity', 'unknown', 'null'))
 
       )

# COMMAND ----------

# Alteração do schema do dataframe
planets_df = (
                planets_df.select(
                    col("id_planet").cast('int'), 
                    col("name").cast('string'), 
                    col("orbital_period").cast('float'), 
                    col("population").cast('int'), 
                    col("rotation_period").cast('float'), 
                    col("surface_water").cast('float'), 
                    col("terrain").cast('string'), 
                    col("climate").cast('string'), 
                    col("diameter").cast('float'), 
                    col("gravity").cast('string'), 
                    col("url").cast('string'), 
                    col("created").cast('timestamp'), 
                    col("edited").cast('timestamp')
                )
)

# COMMAND ----------

planets_df.printSchema()

# COMMAND ----------

display(planets_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_planets = '/FileStore/tables/swapi_dev/trusted/relacional/planets.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(planets_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_planets)
)
     