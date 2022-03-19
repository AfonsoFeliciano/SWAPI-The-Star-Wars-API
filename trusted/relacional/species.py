# Databricks notebook source
path_species_raw = "/FileStore/tables/swapi_dev/raw/species.parquet"

# COMMAND ----------

species_df = spark.read.parquet(path_species_raw)

# COMMAND ----------

display(species_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de species
species_df = (
        species_df.select( 
            species_df.name, 
            species_df.skin_colors,
            species_df.language,
            species_df.hair_colors,
            species_df.eye_colors,
            species_df.designation,
            species_df.classification,
            species_df.average_height,
            species_df.average_lifespan, 
            species_df.url,
            species_df.homeworld,
            species_df.created,  
            species_df.edited  
    )
)

# COMMAND ----------

display(species_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

species_df = (species_df
                .withColumn("id_species",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_planets",reverse(split(reverse(col("homeworld")),"/").getItem(1)))
                .select(
                    "id_species",
                    "id_planets",
                    "name",
                    "skin_colors",
                    "language",
                    "hair_colors",
                    "eye_colors",
                    "designation",
                    "classification",
                    "average_height",
                    "average_lifespan",
                    "url",
                    "created",
                    "edited"
                )
           )
display(species_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
species_df = (
    
            species_df
                    .withColumn('skin_colors', regexp_replace('skin_colors', 'n/a', 'null'))
                    .withColumn('skin_colors', regexp_replace('skin_colors', 'unknown', 'null'))
                    .withColumn('skin_colors', regexp_replace('skin_colors', 'none', 'null'))
                    .withColumn('language', regexp_replace('language', 'n/a', 'null'))
                    .withColumn('language', regexp_replace('language', 'unknown', 'null'))
                    .withColumn('language', regexp_replace('language', 'none', 'null'))
                    .withColumn('hair_colors', regexp_replace('hair_colors', 'n/a', 'null'))
                    .withColumn('hair_colors', regexp_replace('hair_colors', 'unknown', 'null'))
                    .withColumn('hair_colors', regexp_replace('hair_colors', 'none', 'null'))
                    .withColumn('eye_colors', regexp_replace('eye_colors', 'n/a', 'null'))
                    .withColumn('eye_colors', regexp_replace('eye_colors', 'unknown', 'null'))
                    .withColumn('eye_colors', regexp_replace('eye_colors', 'none', 'null'))
                    .withColumn('designation', regexp_replace('designation', 'n/a', 'null'))
                    .withColumn('designation', regexp_replace('designation', 'unknown', 'null'))
                    .withColumn('designation', regexp_replace('designation', 'none', 'null'))
                    .withColumn('classification', regexp_replace('classification', 'n/a', 'null'))
                    .withColumn('classification', regexp_replace('classification', 'unknown', 'null'))
                    .withColumn('classification', regexp_replace('classification', 'none', 'null'))
                    .withColumn('average_height', regexp_replace('average_height', 'n/a', 'null'))
                    .withColumn('average_height', regexp_replace('average_height', 'unknown', 'null'))
                    .withColumn('average_height', regexp_replace('average_height', 'none', 'null'))
                    .withColumn('average_lifespan', regexp_replace('average_lifespan', 'n/a', 'null'))
                    .withColumn('average_lifespan', regexp_replace('average_lifespan', 'unknown', 'null'))
                    .withColumn('average_lifespan', regexp_replace('average_lifespan', 'none', 'null'))
       )

# COMMAND ----------

# Alteração do schema do dataframe
species_df = (
                species_df.select(
                    col("id_species").cast('int'), 
                    col("id_planets").cast('int'),
                    col("name").cast('string'),
                    col("skin_colors").cast('string'),
                    col("language").cast('string'),
                    col("hair_colors").cast('string'),
                    col("designation").cast('string'),
                    col("classification").cast('string'),
                    col("average_height").cast('float'),
                    col("average_lifespan").cast('float'),
                    col("url").cast('string'),
                    col("created").cast('timestamp'),
                    col("edited").cast('timestamp')
                )

)

# COMMAND ----------

species_df.printSchema()

# COMMAND ----------

display(species_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_species = '/FileStore/tables/swapi_dev/trusted/species.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(species_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_species)
)
     

# COMMAND ----------

