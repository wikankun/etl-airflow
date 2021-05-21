import time
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

TIMESTAMP = time.strftime("%Y%m%d")
OUTPUT_DB = './output/sqlite/output.db'

default_args = {
    'owner': 'wikan',
}

with DAG(
    'etl_disaster_dag',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space', 'csv'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_disaster.csv'

        df = pd.read_csv('./data/disaster_data.csv')
        df = df.dropna().drop_duplicates()
        df.to_csv(output_target, index=False, header=True, quoting=2)
        ti.xcom_push('filename', output_target)

    def load(**kwargs):
        ti = kwargs['ti']
        input_target = ti.xcom_pull(task_ids='extract_transform', key='filename')
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            df = pd.read_csv(input_target)
            df.to_sql('disaster', conn, if_exists='replace', index=False)

    start = DummyOperator(
        task_id='start',
    )
    
    extract_transform_task = PythonOperator(
        task_id='extract_transform',
        python_callable=extract_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> extract_transform_task >> load_task >> end
