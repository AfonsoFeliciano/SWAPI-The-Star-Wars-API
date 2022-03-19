# Databricks notebook source
from pyspark.sql.functions import explode, reverse, split, col, monotonically_increasing_id

# COMMAND ----------

path_people_vehicles_raw = "/FileStore/tables/swapi_dev/raw/people.parquet"

# COMMAND ----------

people_vehicles_df = spark.read.parquet(path_people_vehicles_raw)
display(people_vehicles_df)

# COMMAND ----------

people_vehicles_df = (people_vehicles_df
            .withColumn("vehicles", explode("vehicles"))
)

# COMMAND ----------

display(people_vehicles_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de people_vehicles
people_vehicles_df = (
        people_vehicles_df.select(
        people_vehicles_df.url, 
        people_vehicles_df.vehicles
    )
)

# COMMAND ----------

display(people_vehicles_df)

# COMMAND ----------

people_vehicles_df = (people_vehicles_df
                .withColumn("id_people_vehicles", monotonically_increasing_id() + 1)
                .withColumn("id_people",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_vehicles",reverse(split(reverse(col("vehicles")),"/").getItem(1)))
                .select("id_people_vehicles", "id_people", "id_vehicles")
           )
display(people_vehicles_df)

# COMMAND ----------

# Alteração do schema do dataframe
people_vehicles_df = (
                people_vehicles_df.select(
                    col("id_people_vehicles").cast('int'),
                    col("id_people").cast('int'),
                    col("id_vehicles").cast('int')
                )
)

# COMMAND ----------

people_vehicles_df.printSchema()

# COMMAND ----------

display(people_vehicles_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_people_vehicles = '/FileStore/tables/swapi_dev/trusted/relacional/people_vehicles.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(people_vehicles_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_people_vehicles)
)