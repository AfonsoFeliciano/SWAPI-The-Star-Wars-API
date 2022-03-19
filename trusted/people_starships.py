# Databricks notebook source
path_people_starships_raw = "/FileStore/tables/swapi_dev/raw/people.parquet"

# COMMAND ----------

people_starships_df = spark.read.parquet(path_people_starships_raw)
display(people_starships_df)

# COMMAND ----------

from pyspark.sql.functions import *

people_starships_df = (people_starships_df
            .withColumn("starships", explode_outer("starships"))
)

# COMMAND ----------

display(people_starships_df)

# COMMAND ----------

#Seleciona colunas necessárias para a tabela de people starships
people_starships_df = (
        people_starships_df.select(
        people_starships_df.url,
        people_starships_df.starships, 
        
    )
)

# COMMAND ----------

display(people_starships_df)

# COMMAND ----------

#Extração do número da chamada da API para se tornar o ID do film
from pyspark.sql.functions import reverse, split, col, monotonically_increasing_id

people_starships_df = (people_starships_df
                .withColumn("id_people_starships", monotonically_increasing_id() + 1)      
                .withColumn("id_people",reverse(split(reverse(col("url")),"/").getItem(1)))
                .withColumn("id_starships",reverse(split(reverse(col("starships")),"/").getItem(1)))
                .select("id_people_starships", "id_people", "id_starships")
           )
display(people_starships_df)

# COMMAND ----------

# Alteração do schema do dataframe
people_starships_df = (
                people_starships_df.select(
                    col("id_people_starships").cast('int'),
                    col("id_people").cast('int'),
                    col("id_starships").cast('int')
                    
                )
)

# COMMAND ----------

people_starships_df.printSchema()

# COMMAND ----------

display(people_starships_df)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_people_starships = '/FileStore/tables/swapi_dev/trusted/people_starships.parquet'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(people_starships_df.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_people_starships)
)