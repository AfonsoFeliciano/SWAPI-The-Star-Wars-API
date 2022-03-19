# Databricks notebook source
path_people_raw = "/FileStore/tables/swapi_dev/raw/people.parquet"

# COMMAND ----------

people_df = spark.read.parquet(path_people_raw)

# COMMAND ----------

display(people_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de people
people_df = (
        people_df.select( 
            people_df.name, 
            people_df.species,
            people_df.homeworld,
            people_df.gender,
            people_df.hair_color,
            people_df.eye_color,
            people_df.skin_color,
            people_df.birth_year,
            people_df.mass,
            people_df.url, 
            people_df.created,
            people_df.edited     
    )
)

# COMMAND ----------

display(people_df)

# COMMAND ----------

from pyspark.sql.functions import *

people_df = (people_df
            .withColumn("species", explode_outer("species"))
)

display(people_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do people
from pyspark.sql.functions import reverse, split, col

people_df = (people_df
                .withColumn("id_people",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_species",reverse(split(reverse(col("species")),"/").getItem(1)))
                .withColumn("id_planets",reverse(split(reverse(col("homeworld")),"/").getItem(1)))
                .select(
                    "id_people", 
                    "id_species",
                    "id_planets",
                    "name", 
                    "gender",
                    "hair_color",
                    "eye_color",
                    "skin_color",
                    "birth_year",
                    "mass",
                    "url", 
                    "created",
                    "edited"    
                )
           )
display(people_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
people_df = (
    
            people_df
                    .withColumn('gender', regexp_replace('gender', 'none', 'null'))
                    .withColumn('gender', regexp_replace('gender', 'n/a', 'null'))
                    .withColumn('gender', regexp_replace('gender', 'unknown', 'null'))
                    .withColumn('hair_color', regexp_replace('hair_color', 'none', 'null'))
                    .withColumn('hair_color', regexp_replace('hair_color', 'n/a', 'null'))
                    .withColumn('hair_color', regexp_replace('hair_color', 'unknown', 'null'))
                    .withColumn('eye_color', regexp_replace('eye_color', 'none', 'null'))
                    .withColumn('eye_color', regexp_replace('eye_color', 'n/a', 'null'))
                    .withColumn('eye_color', regexp_replace('eye_color', 'unknown', 'null'))
                    .withColumn('skin_color', regexp_replace('skin_color', 'none', 'null'))
                    .withColumn('skin_color', regexp_replace('skin_color', 'n/a', 'null'))
                    .withColumn('skin_color', regexp_replace('skin_color', 'unknown', 'null'))
                    .withColumn('birth_year', regexp_replace('birth_year', 'none', 'null'))
                    .withColumn('birth_year', regexp_replace('birth_year', 'n/a', 'null'))
                    .withColumn('birth_year', regexp_replace('birth_year', 'unknown', 'null'))
                    .withColumn('mass', regexp_replace('mass', 'none', 'null'))
                    .withColumn('mass', regexp_replace('mass', 'n/a', 'null'))
                    .withColumn('mass', regexp_replace('mass', 'unknown', 'null'))
                    
       )

# COMMAND ----------

# Alteração do schema do dataframe
people_df = (
                people_df.select(
                    col("id_people").cast('int'), 
                    col("id_species").cast('int'), 
                    col("id_planets").cast('int'), 
                    col("name").cast('string'), 
                    col("gender").cast('string'),
                    col("hair_color").cast('string'),
                    col("eye_color").cast('string'),
                    col("skin_color").cast('string'),
                    col("birth_year").cast('string'),
                    col("mass").cast('float'),
                    col("url").cast('string'), 
                    col("created").cast('timestamp'),
                    col("edited").cast('timestamp')   
                )

)

# COMMAND ----------

people_df.printSchema()

# COMMAND ----------

display(people_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_people = '/FileStore/tables/swapi_dev/trusted/people.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(people_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_people)
)