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

path_parametros="./parameters/silver.json"


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

def df_sala_municipio():
    df = spark.read.parquet(f"{source_table_parquet_municipio}").createOrReplaceTempView("df")
    df = spark.sql("""SELECT * FROM(SELECT state, group, population, qt_room, name, id, source_year,
    row_number() OVER (PARTITION by state, SUM(qt_room), SUM(population), name, id, SUM(source_year) ORDER BY id) as row_id FROM df
    GROUP BY state, group, qt_room, population, name, id, source_year ORDER BY id)
    WHERE row_id = 1
    """)
    df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_municipio}")

#filmes_exibidos
def df_filmes_exibidos():
    df = spark.read.parquet(source_table_parquet_filmes_exibidos).createOrReplaceTempView("df")
    df = spark.sql("""SELECT * FROM(SELECT exposure_year, title, cpb, gender, country_produce, nationality_movie,release_date, distribution_company, country_distribution, public, rent, date_processed, id, source_year,
    row_number() OVER (PARTITION by exposure_year, title, cpb, gender, country_produce, nationality_movie,release_date, distribution_company, country_distribution, public, rent, date_processed, id, SUM(source_year) ORDER BY id) as row_id FROM df
    GROUP BY exposure_year, title, cpb, gender, country_produce, nationality_movie,release_date, distribution_company, country_distribution, public, rent, date_processed, id, source_year ORDER BY id)
    WHERE row_id = 1""")
    df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_filmes_exibidos}")

#coproducoes
def df_coproducoes():
    df = spark.read.parquet(source_table_parquet_coproducoes).createOrReplaceTempView("df")
    df = spark.sql("""SELECT * FROM(SELECT release_year, cpb, title, filmmaker, gender, national_producer, state, foreign_producer, foreing_country, br_equity_situation, release_date, distribuitor, max_room, target,rent, date_processed, id, source_year,
    row_number() OVER (PARTITION by release_year, cpb, title, filmmaker, gender, national_producer, state, foreign_producer, foreing_country, br_equity_situation, release_date, distribuitor, SUM(max_room), target,rent, date_processed, id, SUM(source_year) ORDER BY id) as row_id FROM df
    GROUP BY release_year, cpb, title, filmmaker, gender, national_producer, state, foreign_producer, foreing_country, br_equity_situation, release_date, distribuitor, max_room, target,rent, date_processed, id, source_year ORDER BY id)
    WHERE row_id = 1""")
    df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_coproducoes}")

#distribuidoras
def df_distribuidoras():
    df = spark.read.parquet(source_table_parquet_distribuidoras).createOrReplaceTempView("df")
    df = spark.sql("""SELECT * FROM(SELECT exposure_year, distributor, ancine_record, distribuitor_country, geral_public, public_br,rent_geral, rent_br, title_geral, title_nacional, date_processed, id, source_year,
        row_number() OVER (PARTITION by exposure_year, distributor, ancine_record, distribuitor_country, geral_public, public_br,rent_geral, rent_br, title_geral, title_nacional, date_processed, id, SUM(source_year) ORDER BY id) as row_id FROM df
        GROUP BY exposure_year, distributor, ancine_record, distribuitor_country, geral_public, public_br,rent_geral, rent_br, title_geral, title_nacional, date_processed, id, source_year ORDER BY id)
        WHERE row_id = 1""")
    df.write.mode("append").format("parquet").partitionBy("source_year").save(f"{target_table_parquet_distribuidoras}")

#df_sala_municipio()
df_filmes_exibidos()
df_distribuidoras()
df_coproducoes()