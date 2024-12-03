from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging
import pandas as pd


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 9, 11),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'wf_002_STG_REGION_DAG',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def Extract_region_data():
    hook = PostgresHook(postgres_conn_id="htdvlpgs001-bi_user") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM REGION")
    with open("/home/bb-admin/airflow/data/wf_stg_region.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved region data in csv file wf_stg_region")

extract = PythonOperator(
    task_id="001_bi_user-region.csv",
    python_callable=Extract_region_data,
    dag=dag,
)

def Load_data_region():
    csv_file_path = '/home/bb-admin/airflow/data/wf_stg_region.csv'
    
    pg_hook = PostgresHook(postgres_conn_id='htdvlpgs002-stg_northwin')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(" TRUNCATE table region")
    copy_sql = """
        COPY region FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """ 
    with open(csv_file_path, 'r') as f:
        cursor.copy_expert(sql=copy_sql, file=f)
    
    cursor.close()
    conn.commit()
    conn.close()

copy = PythonOperator(
    task_id='region.csv-002_stg',
    python_callable=Load_data_region,
    dag=dag,
)

extract >> copy