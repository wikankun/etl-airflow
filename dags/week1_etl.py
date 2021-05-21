import json
import time
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago


TIMESTAMP = time.strftime("%Y%m%d")
OUTPUT_DB = './output/sqlite/output.db'

default_args = {
    'owner': 'wikan',
}

with DAG(
    'etl_flatfile',
    default_args=default_args,
    description='ETL Blank Space Week 1',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['blank_space'],
) as dag:
    dag.doc_md = __doc__

    def extract_tweet_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_tweet.csv'

        data = []
        with open("./data/tweet_data.json", "r") as content:
            for line in content:
                json_data = json.loads(line)
                temp = {
                    'id': json_data['id'],
                    'text': json_data['text'],
                    'language': json_data['lang'],
                    'username': json_data['user']['screen_name'],
                    'location': json_data['user']['location'],
                    'created_at': json_data['user']['created_at'],
                }
                data.append(temp)

        extracted_tweet = pd.DataFrame(data)
        extracted_tweet.to_csv(output_target, index=False, header=True, quoting=2)
        
        ti.xcom_push('extracted_tweet', output_target)

    def extract_database_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_database.csv'
        conn = sqlite3.connect("./data/database.sqlite")

        with conn:
            data = {}
            for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", conn).name:
                data[table] = pd.read_sql_query("select * from {}".format(table), conn)

        extracted_database = data['reviews']
        for table in data:
            if table == 'reviews':
                continue
            extracted_database = extracted_database.merge(data[table], how='left', on='reviewid')

        extracted_database.to_csv(output_target, index=False, header=True, quoting=2)
        
        ti.xcom_push('extracted_database', output_target)

    def extract_chinook_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_chinook_{}.csv'
        conn = sqlite3.connect("./data/chinook.db")

        with conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';", conn)
            for table in tables.name:
                extracted_chinook = pd.read_sql_query("SELECT * FROM {}".format(table), conn)
                extracted_chinook.columns = extracted_chinook.columns.str.lower()
                extracted_chinook.to_csv(output_target.format(table), index=False, header=True, quoting=2)

        ti.xcom_push('extracted_chinook', tables.name.tolist())

    def extract_disaster_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_disaster.csv'

        extracted_disaster = pd.read_csv('./data/disaster_data.csv')
        extracted_disaster = extracted_disaster.dropna().drop_duplicates()
        extracted_disaster.to_csv(output_target, index=False, header=True, quoting=2)
        
        ti.xcom_push('extracted_disaster', output_target)

    def extract_review_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_review.csv'

        review1 = pd.read_csv('./data/reviews_q1.csv')
        review2 = pd.read_csv('./data/reviews_q2.csv')
        review3 = pd.read_csv('./data/reviews_q3.csv')
        review4 = pd.read_csv('./data/reviews_q4.csv')
        review5 = pd.read_excel('./data/reviews_q1.xlsx', engine='openpyxl')

        frames = [review1, review2, review3, review4, review5]

        extracted_review = pd.concat(frames)
        extracted_review.drop_duplicates(subset=['id'], inplace=True)
        extracted_review.to_csv(output_target, index=False, header=True, quoting=2)
        
        ti.xcom_push('extracted_review', output_target)

    def extract_user_data(**kwargs):
        ti = kwargs['ti']
        output_target = './output/' + TIMESTAMP + '_user.csv'

        extracted_user = pd.read_excel('./data/file_1000.xls')
        extracted_user.drop(['Unnamed: 0', 'First Name.1'], axis=1, inplace=True)
        extracted_user = extracted_user.reindex(columns=['Id', 'First Name', 'Last Name', 'Gender', 'Country', 'Age', 'Date'])
        extracted_user.columns = extracted_user.columns.str.replace(' ', '_').str.lower()
        extracted_user.to_csv(output_target, index=False, header=True, quoting=2)

        ti.xcom_push('extracted_user', output_target)

    def load(**kwargs):
        ti = kwargs['ti']
        files = {
            'extracted_tweet': ti.xcom_pull(task_ids='extract_tweet_data', key='extracted_tweet'),
            'extracted_database': ti.xcom_pull(task_ids='extract_database_data', key='extracted_database'),
            'extracted_chinook': ti.xcom_pull(task_ids='extract_chinook_data', key='extracted_chinook'),
            'extracted_disaster': ti.xcom_pull(task_ids='extract_disaster_data', key='extracted_disaster'),
            'extracted_review': ti.xcom_pull(task_ids='extract_review_data', key='extracted_review'),
            'extracted_user': ti.xcom_pull(task_ids='extract_user_data', key='extracted_user'),
        }

        chinook_target = './output/' + TIMESTAMP + '_chinook_{}.csv'
        conn = sqlite3.connect(OUTPUT_DB)

        with conn:
            for fname in files:
                if fname == 'extracted_chinook':
                    for table in files[fname]:
                        df = pd.read_csv(chinook_target.format(table))
                        df.to_sql('chinook_{}'.format(table), conn, if_exists='replace', index=False)
                else:
                    df = pd.read_csv(files[fname])
                    df.to_sql(fname.split('_')[-1], conn, if_exists='replace', index=False)


    extract_tweet_data = PythonOperator(
        task_id='extract_tweet_data',
        python_callable=extract_tweet_data,
    )
    extract_database_data = PythonOperator(
        task_id='extract_database_data',
        python_callable=extract_database_data,
    )
    extract_chinook_data = PythonOperator(
        task_id='extract_chinook_data',
        python_callable=extract_chinook_data,
    )
    extract_disaster_data = PythonOperator(
        task_id='extract_disaster_data',
        python_callable=extract_disaster_data,
    )
    extract_review_data = PythonOperator(
        task_id='extract_review_data',
        python_callable=extract_review_data,
    )
    extract_user_data = PythonOperator(
        task_id='extract_user_data',
        python_callable=extract_user_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_tweet_data >> load_task
    extract_database_data >> load_task
    extract_chinook_data >> load_task
    extract_disaster_data >> load_task
    extract_review_data >> load_task
    extract_user_data >> load_task
