import airflow
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="ISS_Location",
    start_date=datetime(2022, 1, 1),
    schedule_interval='@once',
)

dl_ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")

def iss_combine():


download_location = BashOperator(
    task_id="download_location",
    bash_command=f"curl -o /tmp/iss_loc_{dl_ts}.json -L 'http://api.open-notify.org/iss-now.json'",
    dag=dag,
)