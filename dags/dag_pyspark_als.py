from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, date_format
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from pyspark.ml.recommendation import ALS, ALSModel


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'recommender_als',
    default_args=default_args,
    description='Recommendation ALS Pipeline',
    schedule_interval='@daily',
    catchup=False,
)

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
    
    data = data.withColumn('product_name_length', col('product_name_length').cast('int'))
    
    return data

def train_als_model() -> str:
  
    spark = create_spark_session('RecomendaçãoALS')
    csv_file_path = Variable.get("csv_file_path")
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
    

    model_path = "/path/to/als_model"
    return model_path

def generate_recommendations(**kwargs):
  
    spark = create_spark_session('RecomendaçãoALS')
    csv_file_path = Variable.get("csv_file_path")
    data = load_data(spark, csv_file_path, ';')
    
   
    model_path = kwargs['task_instance'].xcom_pull(task_ids='train_als_model')
    
   
    als = ALSModel.load(model_path)
    

    userRecs = als.recommendForAllUsers(10)
    userRecs = userRecs.withColumnRenamed("recommendations", "recommendation")
    userRecs = userRecs.select("user_id", "recommendation.item_id", "recommendation.rating")
    
 
    recommendations_df = userRecs.toPandas()
    recommendations_df = recommendations_df.explode('item_id')
    recommendations_df = recommendations_df.sort_values(by=['user_id', 'rating'], ascending=[True, False])
    recommendations_df = recommendations_df.drop_duplicates(subset=['user_id', 'item_id'])
    
 
    recommendations_df.to_csv('/path/to/recommendations.csv', index=False)

with dag:
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=[create_spark_session('RecomendaçãoALS'), "{{ var.value.csv_file_path }}", ';'],
    )

    train_als_model_task = PythonOperator(
        task_id='train_als_model',
        python_callable=train_als_model,
    )

    generate_recommendations_task = PythonOperator(
        task_id='generate_recommendations',
        python_callable=generate_recommendations,
        provide_context=True,
    )

    load_data_task >> train_als_model_task
    train_als_model_task >> generate_recommendations_task
