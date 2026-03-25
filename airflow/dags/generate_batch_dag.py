import sys
sys.path.append("/opt/airflow/scripts")  

from generate_batch_data import main

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_batch():
    today = datetime.today().strftime("%Y-%m-%d")
    main(today)

with DAG(
    dag_id="generate_fastfeast_batch",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="generate_batch_data",
        python_callable=run_batch
    )