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
SOURCE_FILE = './data/database.sqlite'
OUTPUT_DB = './output/sqlite/output.db'
TABLE_NAMES = read_table_names(SOURCE_FILE)

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

    def extract(**kwargs):
        table = kwargs['table']
        output_target = './output/{}_database_{}.csv'.format(TIMESTAMP, table)
        conn = sqlite3.connect(SOURCE_FILE)

        with conn:
            df = pd.read_sql_query("select * from {}".format(table), conn)
            df.to_csv(output_target, index=False, header=True, quoting=2)

    def transform(**kwargs):
        ti = kwargs['ti']
        input_target = './output/{}_database_{}.csv'
        output_target = './output/{}_database.csv'.format(TIMESTAMP)

        df = pd.read_csv(input_target.format(TIMESTAMP, 'reviews'))
        for table in TABLE_NAMES:
            if table == 'reviews':
                continue
            df2 = pd.read_csv(input_target.format(TIMESTAMP, table))
            df = df.merge(df2, how='left', on='reviewid')
        
        df.to_csv(output_target, index=False, header=True, quoting=2)
        ti.xcom_push('filename', output_target)

    def load(**kwargs):
        ti = kwargs['ti']
        input_target = ti.xcom_pull(task_ids='transform', key='filename')
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            df = pd.read_csv(input_target)
            df.to_sql('database_review', conn, if_exists='replace', index=False)

    start = DummyOperator(
        task_id='start',
    )

    with TaskGroup("extract_task") as extract_task:
        for table in TABLE_NAMES:
           next_task = PythonOperator(
                task_id='extract_{}'.format(table),
                python_callable=extract,
                op_kwargs={'table': table},
            )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> extract_task >> transform_task >> load_task >> end
