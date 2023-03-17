import airflow
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator,)

dag = DAG(
    dag_id="ISS_Location_3",
    start_date=datetime(2023, 3, 16),
    end_date=datetime(2023, 3, 19),
    schedule_interval='*/5 * * * *',
    catchup=False,
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

def _store_location_temp():
    hook=PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY temp_table FROM stdin DELIMITER ',' CSV HEADER",
        filename='/tmp/iss_loc.csv'
    )

schema = [
    {
        'name': 'date_time',
        'type': 'DATETIME',
        'mode': 'NULLABLE',
    },
    {
        'name': 'message',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'iss_longitude',
        'type': 'NUMERIC',
        'mode': 'NULLABLE',
    },
    {
        'name': 'iss_latitude',
        'type': 'NUMERIC',
        'mode': 'NULLABLE',
    },
]

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS iss_location (
            date_time TIMESTAMP UNIQUE NOT NULL,
            message TEXT NOT NULL,
            iss_longitude NUMERIC NOT NULL,
            iss_latitude NUMERIC NOT NULL
        );
        CREATE TABLE IF NOT EXISTS temp_table
        (LIKE iss_location INCLUDING DEFAULTS);
        TRUNCATE TABLE temp_table;
    ''',
    dag=dag,
)

download_location = BashOperator(
    task_id="download_location",
    bash_command=f"curl -o /tmp/iss_loc_{dl_ts}.json -L 'http://api.open-notify.org/iss-now.json'",
    dag=dag,
)

parser_csv = PythonOperator(
    task_id ='parser_csv',
    python_callable = _parse_data,
    dag=dag,
)

store_data_temp = PythonOperator(
    task_id='store_data_temp',
    python_callable=_store_location_temp,
    dag=dag,
)

temp_to_maintable = PostgresOperator(
    task_id='temp_to_maintable',
    postgres_conn_id='postgres',
    sql='''INSERT INTO iss_location
           SELECT *
           FROM temp_table
           ON CONFLICT (date_time) DO NOTHING;
           COMMIT;
           DROP TABLE temp_table;
    ''',
    dag=dag,
)

push_to_gcs = PostgresToGCSOperator(
    task_id='push_to_gcs',
    postgres_conn_id='postgres',
    gcp_conn_id='gcp_conn',
    sql='SELECT * FROM iss_location',
    bucket='de_proj_spc',
    filename='iss_loc/iss_location.csv',
    export_format='csv',
    gzip=False,
    use_server_side_cursor=False,
    dag=dag,
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    gcp_conn_id='gcp_conn',
    dataset_id='iss_loc',
    project_id='de-recursion'

)

gcs_to_bq = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
    gcp_conn_id='gcp_conn',
    bucket='de_proj_spc',
    source_objects='iss_loc/iss_location.csv',
    destination_project_dataset_table='de-recursion.iss_loc.iss_location',
    schema_fields=schema,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    dag=dag,
)

create_table >> download_location >> parser_csv >> store_data_temp >> temp_to_maintable >> push_to_gcs >> create_dataset >> gcs_to_bq