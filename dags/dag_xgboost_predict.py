from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, date_format
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from xgboost import XGBRegressor
import mlflow.pyfunc
import mlflow.spark
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.models import Variable


with DAG(
    dag_id='recommender_xgboost_trigger',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'catchup': False,
    },
    catchup=False,
) as dag:

    def create_spark_session(app_name: str) -> SparkSession:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark

    def load_data(spark: SparkSession, csv_file_path: str, delimiter: str) -> DataFrame:
        data = spark.read.option("header", True).option("delimiter", delimiter).csv(csv_file_path)
        data = data.toDF(*(col_name.replace(' ', '_') for col_name in data.columns))

        numeric_cols = ['price', 'freight_value', 'payment_value', 'product_weight_g',
                        'product_height_cm', 'product_width_cm', 'payment_sequential']
        for col_name in numeric_cols:
            data = data.withColumn(col_name, regexp_replace(col(col_name), ',', '.').cast('float'))

        data = data.withColumn('product_name_lenght', col('product_name_lenght').cast('int'))

        return data

    def train_xgboost_model(spark: SparkSession, df: DataFrame) -> DataFrame:
      
        return df

    def insert_into_postgres(**kwargs):
      
        als_results = kwargs['ti'].xcom_pull(task_ids='main_task')

     
        xgboost_results = kwargs['ti'].xcom_pull(task_ids='train_xgboost_model')

        combined_results = pd.concat([als_results, xgboost_results], axis=1)

    def main():
        spark = create_spark_session("RecomendaçãoALS")

        csv_file_path = r"D:\teste2607\pedidos_olist.csv"
        data = load_data(spark, csv_file_path, ';')

        indexers = [
            StringIndexer(inputCol="order_id", outputCol="user_id", handleInvalid="keep"),
            StringIndexer(inputCol="product_id", outputCol="item_id", handleInvalid="keep")
        ]

        pipeline = Pipeline(stages=indexers)
        model = pipeline.fit(data)
        data = model.transform(data)

        als = ALS(userCol="user_id", itemCol="item_id", ratingCol="price", coldStartStrategy="drop")
        model = als.fit(data)

        userRecs = model.recommendForAllUsers(10)

        userRecs = userRecs.withColumnRenamed("recommendations", "recommendation")
        userRecs = userRecs.select("user_id", "recommendation.item_id", "recommendation.rating")

        data = data.withColumn("shipping_date", date_format(col("shipping_limit_date"), "yyyy-MM-dd"))
        data = data.select("order_id", "user_id", "customer_id", "item_id", "price",
                           "shipping_date", "product_category_name")

        userRecs = userRecs.join(data, on="user_id").drop("user_id")

        userRecs.show(truncate=False)

     
        return userRecs.toPandas()

    main_task = PythonOperator(
        task_id='main_task',
        python_callable=main,
    )

    train_xgboost_model_task = PythonOperator(
        task_id='train_xgboost_model',
        python_callable=train_xgboost_model,
        provide_context=True,
    )

    insert_postgres_task = PythonOperator(
        task_id='insert_into_postgres',
        python_callable=insert_into_postgres,
        provide_context=True,
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_train_tasks',
        trigger_dag_id='recommender_xgboost_trigger',
        execution_date="{{ next_execution_date }}",
    )

    main_task >> trigger_dag
    trigger_dag >> train_xgboost_model_task
    train_xgboost_model_task >> insert_postgres_task
