from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import insert
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'scrape_google_shopping_urls',
    default_args=default_args,
    description='DAG to scrape Google Shopping URLs',
    schedule_interval=None, 
)

def search_products(**kwargs):
    lista_dicts = []
    data = pd.read_csv('/path/to/your/csv/file.csv', sep=';', encoding='latin-1')
    data['EAN'] = data['EAN'].astype(str).apply(lambda x: x.split(".")[0])
    dicts = data.to_dict('records')

    options = Options() 
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    with webdriver.Chrome(options=options) as driver:
        for dic in dicts:
           
            dict_item = {
                'URLGOOGLE': 'url_here',
                'EAN': 'ean_here',
                'NOMEPRODUTO': 'product_name_here',
                'Marca': 'brand_here',
                'SKU': 'sku_here'
            }
            lista_dicts.append(dict_item)


    hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    connection = hook.get_conn()
    table = 'your_table_name'
    rows = [
        {'urlanuncio': dic['URLGOOGLE'],
         'referencia': dic['EAN'],
         'loja': 'Google Shopping'
         }
        for dic in lista_dicts
    ]
    stmt = insert(table).values(rows)
    connection.execute(stmt)

    return lista_dicts


chromedriver_autoinstaller.install()


scrape_google_urls_task = PythonOperator(
    task_id='scrape_google_urls',
    python_callable=search_products,
    provide_context=True,  
    dag=dag,
)


scrape_google_urls_task


trigger_other_dag_task = TriggerDagRunOperator(
    task_id='trigger_other_dag',
    trigger_dag_id='get_urls_google', 
    dag=dag,
)


scrape_google_urls_task >> trigger_other_dag_task
