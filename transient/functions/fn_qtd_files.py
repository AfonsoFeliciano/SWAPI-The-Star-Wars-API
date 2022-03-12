# Databricks notebook source
def fn_qtd_files(path):
#Se o diretório possuir arquivos, salva a quantidade na variavel qtd_files
        dir = dbutils.fs.ls(path)
        if dir: 
            df = spark.createDataFrame(dbutils.fs.ls(path)) 
            qtd_files = df.count()
            #print(qtd_files)
        #Se não possuir arquivos, atribui na variavel qtd_files
        else: 
            qtd_files = 0
            #print(qtd_files)
        return qtd_files

# COMMAND ----------

