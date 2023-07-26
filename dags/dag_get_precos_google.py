from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator


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


trigger_dag_task >> dummy_task
