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
    'etl_review_dag',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space', 'csv', 'xlsx'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_review.csv'

        review1 = pd.read_csv('./data/reviews_q1.csv')
        review2 = pd.read_csv('./data/reviews_q2.csv')
        review3 = pd.read_csv('./data/reviews_q3.csv')
        review4 = pd.read_csv('./data/reviews_q4.csv')
        review5 = pd.read_excel('./data/reviews_q1.xlsx', engine='openpyxl')

        frames = [review1, review2, review3, review4, review5]

        df = pd.concat(frames)
        df.drop_duplicates(subset=['id'], inplace=True)
        df.to_csv(output_target, index=False, header=True, quoting=2)
        ti.xcom_push('filename', output_target)

    def load(**kwargs):
        ti = kwargs['ti']
        input_target = ti.xcom_pull(task_ids='extract_transform', key='filename')
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            df = pd.read_csv(input_target)
            df.to_sql('review', conn, if_exists='replace', index=False)

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
