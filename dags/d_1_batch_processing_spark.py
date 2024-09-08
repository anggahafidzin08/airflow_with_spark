from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine
import time


from datetime import datetime

# def start_spark() -> SparkSession:
#     return SparkSession.builder \
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0")\
#         .master("local") \
#         .appName("PySpark_Postgres_test") \
#         .getOrCreate();


def top_country_get_data(**kwargs):
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0")\
        .master("local") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()
    
    df_country = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "country")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()
    
    df_city = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "city")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()
    
    df_customer = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "customer")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()
    
    df_address = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "address")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()
    
    df_country.createOrReplaceTempView("country")
    df_city.createOrReplaceTempView("city")
    df_customer.createOrReplaceTempView("customer")
    df_address.createOrReplaceTempView("address")
    
    # spark.sql("""
    # drop table if exists top_country_angga_hafidzin;
    # """)

    df_result = spark.sql("""
    select
        c3.country
        , count(distinct c.customer_id) total_customer_per_country
        , current_date() as date
    from customer c
    left join address a
        using(address_id)
    left join city c2
        using(city_id)
    left join country c3
        using(country_id)
    group by c3.country
    order by 2 desc
    """)

    df_result.write.mode('overwrite').partitionBy('date')\
        .option('compression', 'snappy')\
        .save('data_result_task_1')

def top_country_load_data(**kwargs):
    df_top_country = pd.read_parquet('data_result_task_1')

    engine = create_engine('mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/test',
                        echo=False)
    df_top_country.to_sql(name='top_country_angga_hafidzin', con=engine, if_exists = 'replace', index=False)


def total_film_get_data(**kwargs):
    # spark = ti.xcom_pull(taks_id=['start_task_batch'])
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0")\
        .master("local") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()
    
    df_film = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "film")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()

    df_film_category = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "film_category")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()

    df_category = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://34.42.78.12:5434/postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "category")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .load()
    
    df_film.createOrReplaceTempView("film")
    df_film_category.createOrReplaceTempView("film_category")
    df_category.createOrReplaceTempView("category")

    # spark.sql("""
    # drop table if exists total_film_angga_hafidzin;
    #           """)

    df_total_film = spark.sql("""
    select 
        c.name
        , count(distinct f.film_id) total_film
        , current_date() as date
    from film f 
    left join film_category fc 
        using(film_id)
    left join category c 
        using(category_id)
    group by 1
    order by 2 desc
    """)

    df_total_film.write.mode('overwrite').partitionBy('date')\
        .option('compression', 'snappy')\
        .save('data_result_task_2')

def total_film_load_data(**kwargs):
    df_total_film = pd.read_parquet('data_result_task_2')
    engine = create_engine('mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/test',
                        echo=False)
    df_total_film.to_sql(name='total_film_angga_hafidzin', con=engine, if_exists = 'replace', index=False)

with DAG(
    dag_id='d_1_batch_processing_spark',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task_batch = EmptyOperator(
        task_id='start_task_batch',
        # python_callable=start_spark,
        # do_xcom_push=True
    )

    op_top_country_get = PythonOperator(
        task_id='top_country_get_data',
        python_callable=top_country_get_data
    )

    op_top_country_load = PythonOperator(
        task_id='top_country_load_data',
        python_callable=top_country_load_data
    )

    op_total_film_get = PythonOperator(
        task_id='total_film_get_data',
        python_callable=total_film_get_data
    )

    op_total_film_load = PythonOperator(
        task_id='total_film_load_data',
        python_callable=total_film_load_data
    )

    end_task_batch = EmptyOperator(
        task_id = 'end_task_batch'
    )

    start_task_batch >> op_top_country_get >> op_top_country_load >> end_task_batch
    start_task_batch >> op_total_film_get >> op_total_film_load >> end_task_batch