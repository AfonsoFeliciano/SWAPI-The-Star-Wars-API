# Databricks notebook source
# DBTITLE 1,Definindo o path para leitura dos dados brutos
path_all_calls_raw = "/FileStore/tables/swapi_dev/raw/allcalls.parquet"

# COMMAND ----------

# DBTITLE 1,Leitura e exibição do dataframe
all_calls_df = spark.read.parquet(path_all_calls_raw)
display(all_calls_df)

# COMMAND ----------

# DBTITLE 1,Criando view temporária para normalização dos dados
all_calls_df.createOrReplaceTempView("vw_all_calls")

# COMMAND ----------

# DBTITLE 1,Tratamento dos dados
df_all_calls_trusted = spark.sql("""

    select 'films' as callapi ,films as urlcallapi from vw_all_calls
    union
    select 'people' as callapi, people as urlcallapi from vw_all_calls
    union
    select 'planets' as callapi, planets as urlcallapi from vw_all_calls
    union
    select 'species' as callapi, species as urlcallapi from vw_all_calls
    union
    select 'starships' as callapi, starships as urlcallapi from vw_all_calls
    union
    select 'vehicles' as callapi, vehicles as urlcallapi from vw_all_calls

""")

# COMMAND ----------

# DBTITLE 1,Exibição dos dados
display(df_all_calls_trusted)

# COMMAND ----------

#Definindo o diretório para salvar o arquivo parquet
path_all_calls_trusted = '/FileStore/tables/swapi_dev/trusted/allcalls.parquet/'

# COMMAND ----------

#Escrevendo o arquivo parquet com compressão snappy e no modo overwrite
(df_all_calls_trusted.write
     .option("compression", "snappy")
     .mode("overwrite")
     .parquet(path_all_calls_trusted)
)