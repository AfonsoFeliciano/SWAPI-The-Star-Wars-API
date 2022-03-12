# Databricks notebook source
# DBTITLE 1,Bibliotecas necessárias
import requests
import json
import time

# COMMAND ----------

# DBTITLE 1,URL utilizada nada chamada da API
url = 'https://swapi.dev/api/vehicles/'

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

#Define o caminho que os arquivos brutos serão salvos
path = "/FileStore/tables/swapi_dev/transient/vehicles/"

# COMMAND ----------

# MAGIC %run "/Shared/Swapi_Dev/transient/functions/fn_qtd_files"

# COMMAND ----------

qtd_files = fn_qtd_files(path)
print(qtd_files)

# COMMAND ----------

#Itera a quantidade de valores presentes na API

i = 0

#Por se tratar de um request full, remove o diretório
dbutils.fs.rm(path, True)

#cria o diretório novamente
dbutils.fs.mkdirs(path)

while qtd_files <= qtd_calls:

    #Cria uma variavel string com o número n da iteração
    string_iterator = str(i + 1)
    
    #Concatena a url com o número do vehicles
    url = "https://swapi.dev/api/vehicles/"
    url = url + string_iterator
    
    #Realiza um get na API
    response = None 

    try: 
        response = requests.get(url)
    except Exception as e: 
        print(e)
    
    #print(json.loads(response.text))
    #print(response)
    

    
    #exibe o contéudo da response caso ela não seja igual a None e o código seja igual a 200  
    if response != None and response.status_code == 200:
        
        #Define o caminho que os arquivos brutos serão salvos
        path = "/FileStore/tables/swapi_dev/transient/vehicles/"
        
        #Verifica quantidade de arquivos json no path
        dir = dbutils.fs.ls(path)
        
    
        #print(json.loads(response.text))

        file = "call_vehicles_" + string_iterator + ".json"

        print("Gravando o arquivo " +  file)
        dbutils.fs.put(path + file, response.text)
        
        #Se o diretório possuir arquivos, salva a quantidade na variavel qtd_files
        qtd_files = fn_qtd_files(path)
        print("Qtd_Files " + str(qtd_files))
        print("\n")
        
        #Se a quantidade de arquivos for igual a quantidade de valores na chamada da api
        #Interrompe a busca
         
        if qtd_files == qtd_calls:
            print("Valores iguais")
            break

        else: 
            #Aguarda 0.25 para a próxima requisição
            time.sleep(0.25)
        
    i = i + 1

# COMMAND ----------

