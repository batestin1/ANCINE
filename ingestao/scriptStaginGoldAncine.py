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
import mysql.connector
from sqlalchemy import create_engine
import findspark


#connection
con = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = "root"
)
#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
cursor = con.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS ancine')
my_conn = create_engine('mysql+mysqldb://root:root@localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')

##############################################Extract############################################################################


############# Parametrizacao #########

path_parametros="./parameters/gold.json"

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
source_table_parquet_municipio = df_parar.agg(max_("source_table_parquet_municipio")).collect()[0][0]
target_table_parquet_municipio = df_parar.agg(max_("target_table_parquet_municipio")).collect()[0][0]
source_table_parquet_filmes_exibidos = df_parar.agg(max_("source_table_parquet_filmes_exibidos")).collect()[0][0]
target_table_parquet_filmes_exibidos = df_parar.agg(max_("target_table_parquet_filmes_exibidos")).collect()[0][0]
source_table_parquet_distribuidoras = df_parar.agg(max_("source_table_parquet_distribuidoras")).collect()[0][0]
target_table_parquet_distribuidoras = df_parar.agg(max_("target_table_parquet_distribuidoras")).collect()[0][0]
source_table_parquet_coproducoes = df_parar.agg(max_("source_table_parquet_coproducoes")).collect()[0][0]
target_table_parquet_coproducoes = df_parar.agg(max_("target_table_parquet_coproducoes")).collect()[0][0]
listagem_coproducao = df_parar.agg(max_("listagem_coproducao")).collect()[0][0]
listagem_distribuidora = df_parar.agg(max_("listagem_distribuidora")).collect()[0][0]
filmes_exibidos = df_parar.agg(max_("filmes_exibidos")).collect()[0][0]
current_year = datetime.today().strftime('%Y')
current_year = int(current_year)


#salas_por_municipio


df = spark.read.parquet(f"{source_table_parquet_municipio}").createOrReplaceTempView("df")
df = spark.sql("""SELECT * FROM df""").show(truncate=False)