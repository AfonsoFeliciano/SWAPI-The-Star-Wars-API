# Databricks notebook source
from pyspark.sql.functions import reverse, split, col, regexp_replace

# COMMAND ----------

path_starships_raw = "/FileStore/tables/swapi_dev/raw/starships.parquet"

# COMMAND ----------

starships_df = spark.read.parquet(path_starships_raw)

# COMMAND ----------

display(starships_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de starships
starships_df = (
        starships_df.select( 
            starships_df.name, 
            starships_df.model,
            starships_df.starship_class,
            starships_df.manufacturer,
            starships_df.hyperdrive_rating,
            starships_df.length,
            starships_df.passengers,
            starships_df.MGLT,
            starships_df.cargo_capacity, 
            starships_df.consumables,
            starships_df.cost_in_credits,  
            starships_df.crew,  
            starships_df.max_atmosphering_speed, 
            starships_df.url,
            starships_df.created,  
            starships_df.edited          
    )
)

# COMMAND ----------

display(starships_df)

# COMMAND ----------

starships_df = (starships_df
                .withColumn("id_starships",reverse(split(reverse(col("url")),"/").getItem(1)))
                .select(
                    "id_starships",
                    "name",
                    "model",
                    "starship_class",
                    "manufacturer",
                    "hyperdrive_rating",
                    "length",
                    "passengers",
                    "MGLT",
                    "cargo_capacity",
                    "consumables",
                    "cost_in_credits",
                    "crew",
                    "max_atmosphering_speed",
                    "url",
                    "created",
                    "edited"

                )
           )
display(starships_df)

# COMMAND ----------

starships_df = (
    
            starships_df
                    .withColumn('hyperdrive_rating', regexp_replace('hyperdrive_rating', 'n/a', 'null'))
                    .withColumn('hyperdrive_rating', regexp_replace('hyperdrive_rating', 'unknown', 'null'))
                    .withColumn('hyperdrive_rating', regexp_replace('hyperdrive_rating', 'none', 'null'))
                    .withColumn('length', regexp_replace('length', 'n/a', 'null'))
                    .withColumn('length', regexp_replace('length', 'unknown', 'null'))
                    .withColumn('length', regexp_replace('length', 'none', 'null'))
                    .withColumn('passengers', regexp_replace('passengers', 'n/a', 'null'))
                    .withColumn('passengers', regexp_replace('passengers', 'unknown', 'null'))
                    .withColumn('passengers', regexp_replace('passengers', 'none', 'null'))
                    .withColumn('passengers', regexp_replace('passengers', ',', ''))
                    .withColumn('MGLT', regexp_replace('MGLT', 'n/a', 'null'))
                    .withColumn('MGLT', regexp_replace('MGLT', 'unknown', 'null'))
                    .withColumn('MGLT', regexp_replace('MGLT', 'none', 'null'))
                    .withColumn('cargo_capacity', regexp_replace('cargo_capacity', 'n/a', 'null'))
                    .withColumn('cargo_capacity', regexp_replace('cargo_capacity', 'unknown', 'null'))
                    .withColumn('cargo_capacity', regexp_replace('cargo_capacity', 'none', 'null'))
                    .withColumn('consumables', regexp_replace('consumables', 'n/a', 'null'))
                    .withColumn('consumables', regexp_replace('consumables', 'unknown', 'null'))
                    .withColumn('consumables', regexp_replace('consumables', 'none', 'null'))
                    .withColumn('cost_in_credits', regexp_replace('cost_in_credits', 'n/a', 'null'))
                    .withColumn('cost_in_credits', regexp_replace('cost_in_credits', 'unknown', 'null'))
                    .withColumn('cost_in_credits', regexp_replace('cost_in_credits', 'none', 'null'))
                    .withColumn('crew', regexp_replace('crew', 'n/a', 'null'))
                    .withColumn('crew', regexp_replace('crew', 'unknown', 'null'))
                    .withColumn('crew', regexp_replace('crew', 'none', 'null'))
                    .withColumn('crew', regexp_replace('crew', ',', ''))
                    .withColumn('max_atmosphering_speed', regexp_replace('max_atmosphering_speed', 'n/a', 'null'))
                    .withColumn('max_atmosphering_speed', regexp_replace('max_atmosphering_speed', 'unknown', 'null'))
                    .withColumn('max_atmosphering_speed', regexp_replace('max_atmosphering_speed', 'none', 'null'))
                    .withColumn('max_atmosphering_speed', regexp_replace('max_atmosphering_speed', 'km', ''))
       )

# COMMAND ----------

# Alteração do schema do dataframe
starships_df = (
                starships_df.select(
                    col("id_starships").cast('int'), 
                    col("name").cast('string'),
                    col("starship_class").cast('string'),
                    col("manufacturer").cast('string'),
                    col("hyperdrive_rating").cast('float'),
                    col("length").cast('float'),
                    col("passengers").cast('int'),
                    col("MGLT").cast('float'),
                    col("cargo_capacity").cast('float'),
                    col("consumables").cast('string'),
                    col("cost_in_credits").cast('float'),
                    col("crew").cast('float'),
                    col("max_atmosphering_speed").cast('float'),
                    col("url").cast('string'),
                    col("created").cast('timestamp'),
                    col("edited").cast('timestamp')    
                )

)

# COMMAND ----------

starships_df.printSchema()

# COMMAND ----------

display(starships_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_starships = '/FileStore/tables/swapi_dev/trusted/relacional/starships.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(starships_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_starships)
)
     