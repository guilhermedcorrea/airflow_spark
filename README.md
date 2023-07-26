#Iniciando o projeto
#iniciando o projeto

#python3 -m venv venv
#source venv/bin/activate

#export AIRFLOW_VERSION=2.6.3

#docker-compose up -d



```Python

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



```

<b>Collaborative Filtering ALS Dag - ainda esta sendo implementado</b>




```Python


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

```

<b>XGBOOST DAG ainda sendo implementado</b>






```Python


class PySparkOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            pyspark_script,
            pyspark_conn_id='pyspark_default',
            *args, **kwargs):
        super(PySparkOperator, self).__init__(*args, **kwargs)
        self.pyspark_script = pyspark_script
        self.pyspark_conn_id = pyspark_conn_id

    def execute(self, context):
        hook = PySparkHook(conn_id=self.pyspark_conn_id)
        spark = hook.get_spark()

        try:
            exec(self.pyspark_script)
        finally:
            spark.stop()




class LivyHook(BaseHook):
    def __init__(self, conn_id='livy_default'):
        self.conn_id = conn_id

    def get_livy_session(self):
        conn = self.get_connection(self.conn_id)
        return HttpClient(conn.host, auth=(conn.login, conn.password))

```


<b>PySpark e Livy Hook - ainda sendo implementado</b>




```Python

from airflow.hooks.base_hook import BaseHook
from selenium import webdriver

class SeleniumHook(BaseHook):
    def __init__(self, conn_id='selenium_default'):
        self.conn_id = conn_id

    def get_driver(self):
        conn = self.get_connection(self.conn_id)
        driver = webdriver.Chrome(executable_path=conn.host)
        return driver


```

<b>Selenium Hook - Ainda sendo implementado</b>


```Python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'trigger_google_shopping_prices',
    default_args=default_args,
    description='Trigger DAG for Google Shopping Prices',
    schedule_interval=None,
)


triggered_dag_id = 'get_urls_google'


trigger_dag_task = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id=triggered_dag_id,
    execution_date=datetime(2023, 7, 26), 
    dag=dag,
)


dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)

```

<b>Selenium Dag Scraping google URL produtos</b>



```Python

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'trigger_google_shopping_prices',
    default_args=default_args,
    description='Trigger DAG for Google Shopping Prices',
    schedule_interval=None,
)


triggered_dag_id = 'get_urls_google'


trigger_dag_task = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id=triggered_dag_id,
    execution_date=datetime(2023, 7, 26), 
    dag=dag,
)


dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)




```

<b>Selenium Dag Scraping google coletor de precos</b>