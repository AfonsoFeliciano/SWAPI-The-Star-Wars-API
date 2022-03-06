# Databricks notebook source
# DBTITLE 1,Bibliotecas necessárias
import requests
import json
import time

# COMMAND ----------

# DBTITLE 1,URL utilizada nada chamada da API
url = 'https://swapi.dev/api/films/'

# COMMAND ----------

# DBTITLE 1,Primeira chamada na API
response = None 

try: 
    response = requests.get(url)
except Exception as e: 
    print(e)
    
#exibe o contéudo da response caso ela não seja igual a None e o código seja igual a 200    
if response != None and response.status_code == 200:
    print(json.loads(response.text))


# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC O objetivo da chamada acima foi necessário para descobrir a quantidade de filmes existentes para realizar chamadas paginadas. Ao observar os valores presentes na `response`, pode-se observar que atualmente o retorno da quantidade é salvo no atributo `count`.

# COMMAND ----------

# DBTITLE 1,Salvando a quantidade de valores presentes para a paginação
responseDict =  json.loads(response.text)
qtd_calls = responseDict['count']

# COMMAND ----------

#Intera a quantidade de valores presentes na API
for i in range(0, qtd_calls):
    
    #Cria uma variavel string com o número n da iteração
    string_iterator = str(i + 1)
    
    #Concatena a url com o número do film
    url = "https://swapi.dev/api/films/"
    url = url + string_iterator
    
    #Realiza um get na API
    response = None 

    try: 
        response = requests.get(url)
    except Exception as e: 
        print(e)
    
    #exibe o contéudo da response caso ela não seja igual a None e o código seja igual a 200  
    #Removendo o comentário do print, além disso, salva os arquivos brutos na camada transient
    if response != None and response.status_code == 200:
        
        #print(json.loads(response.text))
        
        #Define o caminho que os arquivos brutos serão salvos
        path = "/FileStore/tables/swapi_dev/transient/films/"
        file = "call_films_" + string_iterator + ".json"
        
        #Por se tratar de um request full, remove os arquivos e os cria novamente
        dbutils.fs.rm(path + file, True)
        
        print("Gravando o arquivo " +  file)
        dbutils.fs.put(path + file, response.text)
        print("\n")
        
        #Aguarda 0.5 para a próxima requisição
        time.sleep(0.5)
