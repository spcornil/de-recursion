import airflow
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    dag_id="ISS_Location",
    start_date=datetime(2022, 1, 1),
    schedule_interval='@once',
)

dl_ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")

def _parse_data():
    import json
    import pandas as pd
    import glob
    import os
    files = glob.glob("/tmp/iss_*.json")
    df = []
    for f in files:
        j = pd.read_json(f, lines=True)
        df.append(j)
    df = pd.concat(df)
    df.reset_index(inplace=True, drop=True)
    df = (pd.DataFrame(df['iss_position'].values.tolist())
        .add_prefix('iss_')
        .join(df.drop('iss_position', axis=1)))
    df = df.rename(columns={'timestamp':'date_time'})
    df = df[['date_time','message','iss_longitude','iss_latitude']]
    df.to_csv('/tmp/iss_loc.csv', index=False, mode='w')

def _store_location():
    hook=PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY iss_location FROM stdin DELIMITER ',' CSV HEADER",
        filename='/tmp/iss_loc.csv'
    )

download_location = BashOperator(
    task_id="download_location",
    bash_command=f"curl -o /tmp/iss_loc_{dl_ts}.json -L 'http://api.open-notify.org/iss-now.json'",
    dag=dag,
)

parser_csv = PythonOperator(
    task_id ='parser_csv',
    python_callable = _parse_data,
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS iss_location (
            date_time TIMESTAMP NOT NULL,
            message TEXT NOT NULL,
            iss_longitude NUMERIC NOT NULL,
            iss_latitude NUMERIC NOT NULL
        );
    '''
)

store_data = PythonOperator(
    task_id='store_data',
    python_callable=_store_location,
    dag=dag,
)


create_table >> download_location >> parser_csv >> store_data