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
    'etl_database_dag',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space', 'sqlite'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_database.csv'
        conn = sqlite3.connect("./data/database.sqlite")

        with conn:
            df = {}
            for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", conn).name:
                df[table] = pd.read_sql_query("select * from {}".format(table), conn)

        df2 = df['reviews']
        for table in df:
            if table == 'reviews':
                continue
            df2 = df2.merge(df[table], how='left', on='reviewid')

        df2.to_csv(output_target, index=False, header=True, quoting=2)
        ti.xcom_push('filename', output_target)

    def load(**kwargs):
        ti = kwargs['ti']
        input_target = ti.xcom_pull(task_ids='extract_transform', key='filename')
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            df = pd.read_csv(input_target)
            df.to_sql('database_review', conn, if_exists='replace', index=False)

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
