import time
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from helper import read_table_names

TIMESTAMP = time.strftime("%Y%m%d")
SOURCE_FILE = './data/chinook.db'
OUTPUT_DB = './output/sqlite/output.db'
TABLE_NAMES = read_table_names(SOURCE_FILE)

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
        table = kwargs['table']
        output_target = './output/{}_chinook_{}.csv'.format(TIMESTAMP, table)
        conn = sqlite3.connect(SOURCE_FILE)

        with conn:
            df = pd.read_sql_query("SELECT * FROM {}".format(table), conn)
            df.columns = df.columns.str.lower()
            df.to_csv(output_target, index=False, header=True, quoting=2)

    def load(**kwargs):
        input_target = './output/{}_chinook_{}.csv'
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            for table in TABLE_NAMES:
                df = pd.read_csv(input_target.format(TIMESTAMP, table))
                df.to_sql('chinook_{}'.format(table), conn, if_exists='replace', index=False)

    start = DummyOperator(
        task_id='start',
    )

    with TaskGroup("extract_transform_task") as extract_transform_task:
        for table in TABLE_NAMES:
           next_task = PythonOperator(
                task_id='extract_transform_{}'.format(table),
                python_callable=extract_transform,
                op_kwargs={'table': table},
            )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> extract_transform_task >> load_task >> end
