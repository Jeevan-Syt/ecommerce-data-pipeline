from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts folder to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))

from extract_api import extract_all

default_args = {
    'owner': 'jeevan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ecommerce_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_all
    )

    extract_task
