#coding=UTF-8

import re
from os import listdir
from os.path import isfile,join
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as max_
from os.path import abspath
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
import subprocess
import csv
import pandas as pd
from datetime import datetime



############# Parametrizacao #########

path_parametros="./parameters/bronze.json"


spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


df_parar=spark.read.json(path_parametros)

df_parar.createOrReplaceTempView("df_parar")


id = df_parar.agg(max_("_id")).collect()[0][0]
description= df_parar.agg(max_("description")).collect()[0][0]
dir_destiny= df_parar.agg(max_("dir_destiny")).collect()[0][0]
extension_files= df_parar.agg(max_("extension_files")).collect()[0][0]
frequency= df_parar.agg(max_("frequency")).collect()[0][0]
method= df_parar.agg(max_("method")).collect()[0][0]
mirror= df_parar.agg(max_("mirror")).collect()[0][0]
pesquisa= df_parar.agg(max_("pesquisa")).collect()[0][0]
project= df_parar.agg(max_("project")).collect()[0][0]
source_type= df_parar.agg(max_("source_type")).collect()[0][0]
subprocess_dir= df_parar.agg(max_("subprocess_dir")).collect()[0][0]
subprocess_rm= df_parar.agg(max_("subprocess_rm")).collect()[0][0]
subprocess_run= df_parar.agg(max_("subprocess_run")).collect()[0][0]
timestamp= df_parar.agg(max_("timestamp")).collect()[0][0]
type_method= df_parar.agg(max_("type_method")).collect()[0][0]
url_path= df_parar.agg(max_("url_path")).collect()[0][0]
version= df_parar.agg(max_("version")).collect()[0][0]
name_file = df_parar.agg(max_("name_file")).collect()[0][0]
dir_download = df_parar.agg(max_("dir_download")).collect()[0][0]
complexo_exibicao = df_parar.agg(max_("complexo_exibicao")).collect()[0][0]
target_table_parquet_municipio = df_parar.agg(max_("target_table_parquet_municipio")).collect()[0][0]
target_table_parquet_filmes_exibidos = df_parar.agg(max_("target_table_parquet_filmes_exibidos")).collect()[0][0]
target_table_parquet_distribuidoras = df_parar.agg(max_("target_table_parquet_distribuidoras")).collect()[0][0]
target_table_parquet_coproducoes = df_parar.agg(max_("target_table_parquet_coproducoes")).collect()[0][0]

listagem_coproducao = df_parar.agg(max_("listagem_coproducao")).collect()[0][0]
listagem_distribuidora = df_parar.agg(max_("listagem_distribuidora")).collect()[0][0]
filmes_exibidos = df_parar.agg(max_("filmes_exibidos")).collect()[0][0]
current_year = datetime.today().strftime('%Y')
current_year = int(current_year)


def extracao():
    try:
        subprocess.run([subprocess_dir, dir_destiny], stdout = subprocess.PIPE)
        file = open(f"./{dir_destiny}/{name_file}", "a")
        script = f"{method} {type_method}{extension_files} {mirror} ./{dir_destiny}/{url_path[7:]}"
        file.write(f"{script}\n")
    except:
        print("Erro no processo de extração")
        


def processo():
    try:
        subprocess.run([subprocess_run, f"./{dir_destiny}/{name_file}"], stdout = subprocess.PIPE)
    except:
        print("Erro no processo de execução")

def delete():
    try:
        subprocess.run([subprocess_rm, f"./{dir_destiny}/{name_file}"], stdout = subprocess.PIPE)
    except:
        print("Erro no processo de deleção")

#./downloads/www.gov.br/ancine/pt-br/oca/cinema/arquivos.csv/evolucao-do-numero-de-salas-de-exibicao-1971-a-2020.csv
extracao()
processo()
delete()
 
path = dir_destiny+dir_download
find = [f for f in listdir(path) if isfile(join(path, f))]

#listagem de salas por municipio de 2011 em diante

for i in range(2011,current_year):
    try:
        if f"{complexo_exibicao}{i}{extension_files}" in find:
            data=pd.read_csv(f'{path}{complexo_exibicao}{i}{extension_files}',skiprows=2, encoding='latin1', sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""SELECT `UF` as state, `Grupo ANCINE` as group, `Número de Salas` as qt_room, `População*` as population, `Nome Complexo ` as name, "{i}" as source_year, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id FROM df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_municipio}")
            print("#"*100)
            
    except:
        if f"{complexo_exibicao}2018{extension_files}" in find:
            data=pd.read_csv(f'{path}{complexo_exibicao}2018{extension_files}', encoding='latin1', sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""SELECT `UF` as state, `Grupo ANCINE` as group, `Número de Salas` as qt_room, `População*` as population, `Nome Complexo ` as name, "2018" as source_year, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id FROM df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_municipio}")
            print("#"*100)
           

        if f"{complexo_exibicao}2019{extension_files}" in find:
            data=pd.read_csv(f'{path}{complexo_exibicao}2019{extension_files}',skiprows=2, encoding='utf-8', sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""SELECT `UF` as state, `Grupo ANCINE` as group, `Número de Salas` as qt_room, `População*` as population, `Nome Complexo` as name, "2019" as source_year, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id FROM df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_municipio}")
            print("#"*100)
            

        if f"{complexo_exibicao}2020{extension_files}" in find:
            data=pd.read_csv(f'{path}{complexo_exibicao}2020{extension_files}',skiprows=2, sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""SELECT `UF` as state, `Grupo ANCINE` as group, `Número de Salas` as qt_room, `População*` as population, `Nome Complexo` as name, "2020" as source_year, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id FROM df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_municipio}")
            print("#"*100)
print(f"insert into datalake bruto complexo_exibicao ")           

#listagem-de-coproducoes-internacionais

for i in range(2005,current_year):
    if f"{listagem_coproducao}2020{extension_files}" in find:
        data=pd.read_csv(f'{path}{listagem_coproducao}2020{extension_files}',skiprows=2, encoding='latin1', sep=';', low_memory=False)
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView('df')
        df = spark.sql(f"""select `Ano de Lançamento` as release_year, `Certificado de Produto Brasileiro (CPB)` as cpb, "{i}" as source_year, `Título` as title, `Direção` as filmmaker, `Gênero` as gender, `Empresa Produtora Majoritária Brasileira\n(Nome Fantasia)` national_producer, `UF` as state, `Empresa Coprodutora Estrangeira` as foreign_producer, `País da Empresa Coprodutora Estrangeira`as foreing_country, `Situação Patrimonial Brasileira` as br_equity_situation, `Data de Lançamento\n(em salas de exibição)`as release_date, `Distribuidora\n(em salas de exibição)` as distribuitor, `Máximo de Salas\n(em salas de exibição)`as max_room, `Público\n(em salas de exibição)` as target, `Renda (R$)\n(em salas de exibição)` as rent,date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id   FROM df""")
        df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_coproducoes}")
        print("#"*100)
       
    else:
        print("Error on insert listagem_coproducao")
print(f"insert into datalake bruto listagem_coproducao ")
#listagem-de-distribuidoras
for i in range(2009,current_year):
    if f"{listagem_distribuidora}2020{extension_files}" in find:
        data=pd.read_csv(f'{path}{listagem_distribuidora}2020{extension_files}',skiprows=2, encoding='latin1', sep=';', low_memory=False)
        df = spark.createDataFrame(data)
        df.createOrReplaceTempView('df')
        df = spark.sql(f"""select `Ano de exibição` as exposure_year, `Distribuidora` as distributor, "{i}" as source_year, `Número de Registro Ancine` as ancine_record, `Origem da empresa distribuidora` as distribuitor_country, `Público` as geral_public, `Público Filmes Brasileiros` as public_br, `Renda (R$)` as rent_geral, `Renda Filmes Brasileiros (R$)` as rent_br, `Títulos Exibidos` as title_geral, `Títulos Brasileiros Exibidos` as title_nacional, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id from df""")
        df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_distribuidoras}")
        
    else:
        print(f"Error on insert listagem_coproducao")
print(f"insert into datalake bruto listagem de distribuidora ")
#listagem-de-filmes-brasileiros-e-estrangeiros-exibidos
for i in range(2009,current_year):
    try:
        if f"{filmes_exibidos}{i}{extension_files}" in find:
            data=pd.read_csv(f'{path}{filmes_exibidos}{i}{extension_files}',skiprows=2, encoding='latin1', sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""select `Ano de exibição` as exposure_year, `Título da obra` as title, `CPB/ROE` as cpb, `Gênero` as gender, `País(es) produtor(es) da obra` as country_produce, "{i}" as source_year, `Nacionalidade da obra` as nationality_movie, `Data de lançamento` as release_date, `Empresa distribuidora` as distribution_company, `Origem da empresa distribuidora` as country_distribution, `Público no ano de exibição` as public, `Renda (R$) no ano de exibição` as rent, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id from df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_filmes_exibidos}")
            
    except:
        if f"{filmes_exibidos}{i}{extension_files}" in find:
            data=pd.read_csv(f'{path}{filmes_exibidos}{i}{extension_files}',skiprows=2, encoding='cp850', sep=';', low_memory=False)
            df = spark.createDataFrame(data)
            df.createOrReplaceTempView('df')
            df = spark.sql(f"""select `Ano de exibi├º├úo` as exposure_year, `T├¡tulo da obra` as title, `CPB/ROE` as cpb, `G├¬nero` as gender, `Pa├¡s(es) produtor(es) da obra` as country_produce, "{i}" as source_year, `Nacionalidade da obra` as nationality_movie, `Data de lan├ºamento` as release_date, `Empresa distribuidora` as distribution_company, `Origem da empresa distribuidora` as country_distribution, `P├║blico no ano de exibi├º├úo` as public, `Renda (R$) no ano de exibi├º├úo` as rent, date_format(current_date(), 'yyyyMMdd') as date_processed,REPLACE(uuid(), '-','') as id from df""")
            df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_filmes_exibidos}")
print(f"insert into datalake bruto filmes exibidos ")
