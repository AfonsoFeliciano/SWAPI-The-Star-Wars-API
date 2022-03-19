# Databricks notebook source
path_vehicles_raw = "/FileStore/tables/swapi_dev/raw/vehicles.parquet"

# COMMAND ----------

vehicles_df = spark.read.parquet(path_vehicles_raw)

# COMMAND ----------

display(vehicles_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de vehicles
vehicles_df = (
        vehicles_df.select( 
            vehicles_df.name, 
            vehicles_df.model,
            vehicles_df.vehicle_class,
            vehicles_df.manufacturer,
            vehicles_df.length,
            vehicles_df.max_atmosphering_speed,
            vehicles_df.passengers,
            vehicles_df.cargo_capacity,
            vehicles_df.consumables, 
            vehicles_df.cost_in_credits,
            vehicles_df.crew, 
            vehicles_df.url,
            vehicles_df.created,  
            vehicles_df.edited  
    )
)

# COMMAND ----------

display(vehicles_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col

vehicles_df = (vehicles_df
                .withColumn("id_vehicle",reverse(split(reverse(col("url")),"/").getItem(1)))
                .select(
                    "id_vehicle", 
                    "name", 
                    "model", 
                    "vehicle_class", 
                    "manufacturer", 
                    "length", 
                    "max_atmosphering_speed", 
                    "passengers", 
                    "cargo_capacity", 
                    "consumables", 
                    "cost_in_credits", 
                    "crew", 
                    "url", 
                    "created", 
                    "edited"
                )
           )
display(vehicles_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
vehicles_df = (
    
            vehicles_df
                    .withColumn('length', regexp_replace('length', 'unknown', 'null'))
                    .withColumn('max_atmosphering_speed', regexp_replace('max_atmosphering_speed', 'unknown', 'null'))
                    .withColumn('passengers', regexp_replace('passengers', 'unknown', 'null'))
                    .withColumn('cargo_capacity', regexp_replace('cargo_capacity', 'unknown', 'null'))
                    .withColumn('cargo_capacity', regexp_replace('cargo_capacity', 'none', 'null'))
                    .withColumn('consumables', regexp_replace('consumables', 'unknown', 'null'))
                    .withColumn('consumables', regexp_replace('consumables', 'none', 'null'))
                    .withColumn('cost_in_credits', regexp_replace('cost_in_credits', 'unknown', 'null'))
 
       )

# COMMAND ----------

# Alteração do schema do dataframe
vehicles_df = (
                vehicles_df.select(
                    col("id_vehicle").cast('int'), 
                    col("name").cast('string'), 
                    col("model").cast('string'), 
                    col("vehicle_class").cast('string'), 
                    col("manufacturer").cast('string'), 
                    col("length").cast('float'), 
                    col("max_atmosphering_speed").cast('float'), 
                    col("passengers").cast('int'), 
                    col("cargo_capacity").cast('int'), 
                    col("consumables").cast('string'),
                    col("cost_in_credits").cast('float'), 
                    col("url").cast('string'), 
                    col("created").cast('timestamp'), 
                    col("edited").cast('timestamp'),

                )

)

# COMMAND ----------

vehicles_df.printSchema()

# COMMAND ----------

display(vehicles_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_vehicles = '/FileStore/tables/swapi_dev/trusted/vehicles.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(vehicles_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_vehicles)
)
     
