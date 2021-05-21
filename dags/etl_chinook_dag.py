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
    'etl_chinook_dag',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space', 'sqlite'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_chinook_{}.csv'
        conn = sqlite3.connect("./data/chinook.db")

        with conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';", conn)
            for table in tables.name:
                df = pd.read_sql_query("SELECT * FROM {}".format(table), conn)
                df.columns = df.columns.str.lower()
                df.to_csv(output_target.format(table), index=False, header=True, quoting=2)

        ti.xcom_push('tables', tables.name.tolist())

    def load(**kwargs):
        ti = kwargs['ti']
        input_target = './output/' + TIMESTAMP + '_chinook_{}.csv'
        tables = ti.xcom_pull(task_ids='extract_transform', key='tables')
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            for table in tables:
                df = pd.read_csv(input_target.format(table))
                df.to_sql('chinook_{}'.format(table), conn, if_exists='replace', index=False)

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
