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
import pandas as pd
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')

#connection
con = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = "root",
    
)
#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
cursor = con.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS ancine')
my_conn = create_engine('mysql+mysqldb://root:root@localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')

##############################################Extract############################################################################


############# Parametrizacao #########

path_parametros="/home/bates/repositorio/big_data/ancine/parameters/gold.json"

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
mode = df_parar.agg(max_("mode")).collect()[0][0]
format_parquet = df_parar.agg(max_("format_parquet")).collect()[0][0]
format_sql = df_parar.agg(max_("format_sql")).collect()[0][0]


#salas_por_municipio

def salas_por_municipio():
    room = spark.read.parquet(f"{source_table_parquet_municipio}").createOrReplaceTempView("df")
    print("salas por municipio")
    room = spark.sql("""SELECT * FROM df""")

    room_by_state = spark.sql("""SELECT state, total FROM(
    SELECT 1 as index, 'SC' state, SUM(qt_room) as total FROM df where state = 'SC' UNION
    SELECT 2 as index, 'RO' state, SUM(qt_room) as total FROM df where state = 'RO' UNION
    SELECT 3 as index, 'PI' state, SUM(qt_room) as total FROM df where state = 'PI' UNION
    SELECT 4 as index, 'AM' state, SUM(qt_room) as total FROM df where state = 'AM' UNION
    SELECT 5 as index, 'RR' state, SUM(qt_room) as total FROM df where state = 'RR' UNION
    SELECT 6 as index, 'GO' state, SUM(qt_room) as total FROM df where state = 'GO' UNION
    SELECT 7 as index, 'TO' state, SUM(qt_room) as total FROM df where state = 'TO' UNION
    SELECT 8 as index, 'MT' state, SUM(qt_room) as total FROM df where state = 'MT' UNION
    SELECT 9 as index, 'SP' state, SUM(qt_room) as total FROM df where state = 'SP' UNION
    SELECT 10 as index, 'ES' state, SUM(qt_room) as total FROM df where state = 'ES' UNION
    SELECT 11 as index, 'PB' state, SUM(qt_room) as total FROM df where state = 'PB' UNION
    SELECT 12 as index, 'RS' state, SUM(qt_room) as total FROM df where state = 'RS' UNION
    SELECT 13 as index, 'MS' state, SUM(qt_room) as total FROM df where state = 'MS' UNION
    SELECT 14 as index, 'AL' state, SUM(qt_room) as total FROM df where state = 'AL' UNION
    SELECT 15 as index, 'MG' state, SUM(qt_room) as total FROM df where state = 'MG' UNION
    SELECT 16 as index, 'PA' state, SUM(qt_room) as total FROM df where state = 'PA' UNION
    SELECT 17 as index, 'BA' state, SUM(qt_room) as total FROM df where state = 'BA' UNION
    SELECT 18 as index, 'SE' state, SUM(qt_room) as total FROM df where state = 'SE' UNION
    SELECT 19 as index, 'PE' state, SUM(qt_room) as total FROM df where state = 'PE' UNION
    SELECT 20 as index, 'CE' state, SUM(qt_room) as total FROM df where state = 'CE' UNION
    SELECT 21 as index, 'RN' state, SUM(qt_room) as total FROM df where state = 'RN' UNION
    SELECT 22 as index, 'RJ' state, SUM(qt_room) as total FROM df where state = 'RJ' UNION
    SELECT 23 as index, 'MA' state, SUM(qt_room) as total FROM df where state = 'MA' UNION
    SELECT 24 as index, 'AC' state, SUM(qt_room) as total FROM df where state = 'AC' UNION
    SELECT 25 as index, 'DF' state, SUM(qt_room) as total FROM df where state = 'DF' UNION
    SELECT 26 as index, 'PR' state, SUM(qt_room) as total FROM df where state = 'PR' UNION
    SELECT 27 as index, 'AP' state, SUM(qt_room) as total FROM df where state = 'AP' 
    )

    """)
    room.write.mode(mode).format(format_parquet).partitionBy("source_year").save(target_table_parquet_municipio)
    room_by_state.write.format("jdbc").options(url='jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_metric_by_room_by_state',user="root",password="root").mode(mode).save()
    room.write.format("jdbc").options(url=f'jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_prod_room_by_state',user="root",password="root").mode(mode).save()
   


def movie_by_countrys():
    movie = spark.read.parquet(f"{source_table_parquet_filmes_exibidos}").createOrReplaceTempView("df")
    print("salas por filmes_exibidos")
    movie = spark.sql("""SELECT exposure_year,
    title,
    cpb,
    gender,
    country_produce,
    nationality_movie,
    release_date,
    distribution_company,
    country_distribution,
    public,
    rent,
    date_processed,
    id,
    source_year FROM df""")

    movie_by_distribution = spark.sql("""SELECT nationality_movie, total FROM(
        SELECT 1 as index, 'Foreign' nationality_movie, COUNT(nationality_movie) as total FROM df where nationality_movie = 'Estrangeira' UNION
        SELECT 2 as index, 'National' nationality_movie, COUNT(nationality_movie) as total FROM df where nationality_movie = 'Brasileira'
        )""")
    movie.write.mode(mode).format(format_parquet).partitionBy("source_year").save(target_table_parquet_municipio)
    movie_by_distribution.write.format("jdbc").options(url='jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_metric_by_movie_by_distribution_state',user="root",password="root").mode(mode).save()
    movie.write.format("jdbc").options(url=f'jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_prod_room_movie',user="root",password="root").mode(mode).save()
   

def coproducoes():
    movie_all = spark.read.parquet(f"{source_table_parquet_coproducoes}").createOrReplaceTempView("df")
    print("salas por co producoes")
    movie_all = spark.sql("""SELECT id, source_year, date_processed, rent, target, max_room, distribuitor, release_date, br_equity_situation, foreign_producer, cpb, foreing_country, title, filmmaker, gender,national_producer, state FROM df""")
    movie_by_year = spark.sql("""SELECT year, total FROM(
            SELECT 1 as index, '2007' year, COUNT(title) as total FROM df where source_year = '2007' UNION
            SELECT 2 as index, '2018' year, COUNT(title) as total FROM df where source_year = '2018' UNION
            SELECT 3 as index, '2015' year, COUNT(title) as total FROM df where source_year = '2015' UNION
            SELECT 4 as index, '2006' year, COUNT(title) as total FROM df where source_year = '2006' UNION
            SELECT 5 as index, '2013' year, COUNT(title) as total FROM df where source_year = '2013' UNION
            SELECT 6 as index, '2014' year, COUNT(title) as total FROM df where source_year = '2014' UNION
            SELECT 7 as index, '2019' year, COUNT(title) as total FROM df where source_year = '2019' UNION
            SELECT 8 as index, '2020' year, COUNT(title) as total FROM df where source_year = '2020' UNION
            SELECT 10 as index, '2012' year, COUNT(title) as total FROM df where source_year = '2012' UNION
            SELECT 12 as index, '2009' year, COUNT(title) as total FROM df where source_year = '2009' UNION
            SELECT 11 as index, '2016' year, COUNT(title) as total FROM df where source_year = '2016' UNION
            SELECT 13 as index, '2005' year, COUNT(title) as total FROM df where source_year = '2005' UNION
            SELECT 14 as index, '2010' year, COUNT(title) as total FROM df where source_year = '2010' UNION
            SELECT 15 as index, '2008' year, COUNT(title) as total FROM df where source_year = '2008' UNION
            SELECT 16 as index, '2017' year, COUNT(title) as total FROM df where source_year = '2017' UNION
            SELECT 17 as index, '2021' year, COUNT(title) as total FROM df where source_year = '2021' UNION
            SELECT 18 as index, '2011' year, COUNT(title) as total FROM df where source_year = '2011' 
            )""")
    movie_all.write.mode(mode).format(format_parquet).partitionBy("source_year").save(target_table_parquet_municipio)
    movie_by_year.write.format("jdbc").options(url='jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_metric_by_movie_by_year',user="root",password="root").mode(mode).save()
    movie_all.write.format("jdbc").options(url=f'jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_prod_comovies',user="root",password="root").mode(mode).save()



def distributor():

    distributor = spark.read.parquet(f"{source_table_parquet_distribuidoras}").createOrReplaceTempView("df")
    print("salas por distribuidoras")
    distributor = spark.sql("""SELECT * FROM df""")
    distributor_by_rent = spark.sql("""SELECT distributor, total FROM(
            SELECT 1 as index, "Aiuê Filmes                       " distributor, SUM(title_geral) as total FROM df where distributor = "Aiuê Filmes" UNION
            SELECT 2 as index, "Indiana Produções Cinematográficas" distributor, SUM(title_geral) as total FROM df where distributor = "Indiana Produções Cinematográficas" UNION
            SELECT 3 as index, "Flow Impact                       " distributor, SUM(title_geral) as total FROM df where distributor = "Flow Impact" UNION
            SELECT 4 as index, "TVA2 Produções                    " distributor, SUM(title_geral) as total FROM df where distributor = "TVA2 Produções" UNION
            SELECT 5 as index, "VPC Cinemavídeo                   " distributor, SUM(title_geral) as total FROM df where distributor = "VPC Cinemavídeo" UNION
            SELECT 6 as index, "LC Barreto Filmes                 " distributor, SUM(title_geral) as total FROM df where distributor = "LC Barreto Filmes" UNION
            SELECT 7 as index, "H2O Films/RioFilme                " distributor, SUM(title_geral) as total FROM df where distributor = "H2O Films/RioFilme" UNION
            SELECT 8 as index, "Walper Ruas                       " distributor, SUM(title_geral) as total FROM df where distributor = "Walper Ruas" UNION
            SELECT 9 as index, "Ivin Films                        " distributor, SUM(title_geral) as total FROM df where distributor = "Ivin Films" UNION
            SELECT 10 as index, "Lotado Filmes                     " distributor,SUM(title_geral) as total FROM df where distributor = "Lotado Filmes" UNION
            SELECT 12 as index, "Pansport                          " distributor,SUM(title_geral) as total FROM df where distributor = "Pansport" UNION
            SELECT 13 as index, "Bretz Filmes/RioFilme             " distributor,SUM(title_geral) as total FROM df where distributor = "Bretz Filmes/RioFilme" UNION
            SELECT 14 as index, "Sereia Filmes                     " distributor,SUM(title_geral) as total FROM df where distributor = "Sereia Filmes" UNION
            SELECT 15 as index, "Kinoscópio                        " distributor,SUM(title_geral) as total FROM df where distributor = "Kinoscópio" UNION
            SELECT 16 as index, "DM Filmes                         " distributor,SUM(title_geral) as total FROM df where distributor = "DM Filmes" UNION
            SELECT 17 as index, "Lança Filmes                      " distributor,SUM(title_geral) as total FROM df where distributor = "Lança Filmes" UNION
            SELECT 18 as index, "Bond's Filmes                     " distributor,SUM(title_geral) as total FROM df where distributor = "Bond's Filmes" UNION
            SELECT 19 as index, "Cultura Maior                     " distributor,SUM(title_geral) as total FROM df where distributor = "Cultura Maior" UNION
            SELECT 20 as index, "Maria Gorda Filmes                " distributor,SUM(title_geral) as total FROM df where distributor = "Maria Gorda Filmes" UNION
            SELECT 21 as index, "TGD Filmes     " 				    distributor, SUM(title_geral) as total FROM df where distributor = "TGD Filmes"
            )""")
    distributor.write.mode(mode).format(format_parquet).partitionBy("source_year").save(target_table_parquet_municipio)
    distributor.write.format("jdbc").options(url='jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_prod_by_distributor_by_rent',user="root",password="root").mode(mode).save()
    distributor_by_rent.write.format("jdbc").options(url=f'jdbc:mysql://localhost/ancine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver="com.mysql.cj.jdbc.Driver",dbtable='db_metric_distributor',user="root",password="root").mode(mode).save()

#init
salas_por_municipio()
movie_by_countrys()
coproducoes()
distributor()