from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging

default_args = {
    'owner': 'Mani',
    'start_date': datetime(2023, 10, 24),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'location_stg',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def Extract_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn_postgres") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT ship_city,ship_country FROM orders")
    with open("/home/manivel/stg_location.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved stg_location data in csv file stg_location")

extract = PythonOperator(
    task_id="sample_postgres-location.csv",
    python_callable=Extract_data,
    dag=dag,
)

def insert_customers():
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_sample')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE table location")

    insert_sql = """
                    INSERT INTO location(city, country)
                    select ship_city,ship_country 
                    from orders 
                    group by ship_country,ship_city;
                """
    cursor.execute(insert_sql)

    cursor.close()
    conn.commit()
    conn.close()

insert = PythonOperator(
    task_id='order_details.csv-sample_stg',
    python_callable=insert_customers,
    dag=dag,
)

extract >> insert
