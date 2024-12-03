from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging

default_args = {
    'owner': 'Mani',
    'start_date': datetime(2023, 10, 16),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'orders_stg',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def Extract_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn_postgres") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders")
    with open("/home/manivel/stg_orders.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved orders data in csv file stg_orders")

extract = PythonOperator(
    task_id="sample_postgres-orders.csv",
    python_callable=Extract_data,
    dag=dag,
)

def insert_products():
    csv_file_path = '/home/manivel/stg_orders.csv'
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_sample')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(" TRUNCATE table orders")
    # copy_sql = """
    #     COPY products FROM stdin WITH CSV HEADER
    #     DELIMITER as ','
    #     """ 
    with open(csv_file_path, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            insert_sql="""INSERT INTO orders(order_id,customer_id,employee_id,order_date,required_date,shipped_date,ship_via,freight,ship_name,ship_address,ship_city,ship_region,ship_postal_code,ship_country)
             values(%(order_id)s,%(customer_id)s,%(employee_id)s,%(order_date)s,%(required_date)s,%(shipped_date)s,%(ship_via)s,%(freight)s,%(ship_name)s,%(ship_address)s,%(ship_city)s,%(ship_region)s,%(ship_postal_code)s,%(ship_country)s) """
            cursor.execute(insert_sql, row)
    
    cursor.close()
    conn.commit()
    conn.close()

insert = PythonOperator(
    task_id='orders.csv-sample_stg',
    python_callable=insert_products,
    dag=dag,
)
extract >> insert