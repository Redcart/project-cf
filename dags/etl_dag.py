from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils import extract_transform_load


URL = "https://api.publibike.ch/v1/public/partner/stations"
BUCKET_NAME = "ind-etl-publibike-dev"
PROJECT_ID = "inspired-victor-442419-j3"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('etl_dag', schedule_interval='*/5 * * * *', default_args=default_args, catchup=False) as dag:

    python_task = PythonOperator(
        task_id='etl_task',
        python_callable=extract_transform_load,
        op_kwargs={
            "url": URL, 
            "project_id": PROJECT_ID, 
            "bucket_name": BUCKET_NAME, 
            "mode": "capacity"
        },
    )
    