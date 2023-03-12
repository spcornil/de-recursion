import airflow
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="OpenSky2",
    start_date=datetime.now(),
    schedule_interval='@once',
)

download_flights = BashOperator(
    task_id="download_flights",
    bash_command="curl -o /tmp/test_slc_2.json -L 'https://api.aviationapi.com/v1/airports?apt=SLC'",
    dag=dag,
)

download_flights