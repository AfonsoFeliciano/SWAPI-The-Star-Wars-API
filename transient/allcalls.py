# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
import requests
import json

# COMMAND ----------

# DBTITLE 1,Definindo url para requisição
url = 'https://swapi.dev/api/'

# COMMAND ----------

# DBTITLE 1,Realizando requisição de teste
response = None 

try: 
    response = requests.get(url)
except Exception as e: 
    print(e)
    
#exibe o contéudo da response caso ela não seja igual a None e o código seja igual a 200    
if response != None and response.status_code == 200:
    print(json.loads(response.text))


# COMMAND ----------

# DBTITLE 1,Cria variável com conteúdo em formato json
responseJson =  json.loads(response.text)
responseJson

# COMMAND ----------

path = "/FileStore/tables/swapi_dev/transient/allcalls/"

# COMMAND ----------

dbutils.fs.rm(path, True)

# COMMAND ----------

dbutils.fs.put(path + "allcalls.json", response.text)

# COMMAND ----------

