import os.path
import time
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from helper import find_file_names

TIMESTAMP = time.strftime("%Y%m%d")
OUTPUT_DB = './output/sqlite/output.db'
FILE_NAMES = find_file_names('./data/reviews_*')

default_args = {
    'owner': 'wikan',
}

with DAG(
    'etl_review_dag',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space', 'csv', 'xlsx'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        filename = kwargs['filename']
        output_target = './output/' + TIMESTAMP + '_review.csv'

        if os.path.isfile(output_target):
            df1 = pd.read_csv(output_target)
        else:
            df1 = pd.DataFrame()

        if filename.split('.')[-1] == 'csv':
            df2 = pd.read_csv(filename)
        elif filename.split('.')[-1] == 'xlsx':
            df2 = pd.read_excel(filename, engine='openpyxl')

        frames = [df1, df2]

        df = pd.concat(frames)
        df.drop_duplicates(subset=['id'], inplace=True)
        df.to_csv(output_target, index=False, header=True, quoting=2)

    def load(**kwargs):
        input_target = './output/' + TIMESTAMP + '_review.csv'
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            df = pd.read_csv(input_target)
            df.to_sql('review', conn, if_exists='replace', index=False)

    start = DummyOperator(
        task_id='start',
    )

    with TaskGroup("extract_transform_task") as extract_transform_task:
        for filename in FILE_NAMES:
            f = filename.split('/')[-1]
            next_task = PythonOperator(
                task_id='extract_transform_{}'.format(f),
                python_callable=extract_transform,
                op_kwargs={'filename': filename},
            )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> extract_transform_task >> load_task >> end
