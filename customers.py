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
    'customers_stg',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def Extract_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn_postgres") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM customers")
    with open("/home/manivel/stg_customers.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved customers data in csv file stg_customers")

extract = PythonOperator(
    task_id="sample_postgres-customers.csv",
    python_callable=Extract_data,
    dag=dag,
)

def insert_customers():
    csv_file_path = '/home/manivel/stg_customers.csv'
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_sample')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(" TRUNCATE table customers")
    copy_sql = """
        COPY customers FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """ 
    with open(csv_file_path, 'r') as f:

        # for row in csv_reader:
        #     insert_sql="""INSERT INTO customers(product_id,product_name,supplier_id,category_id,quantity_per_unit,unit_price,unit_in_stock,unit_on_order,reorder_level,discontinued)
        #      values(%(product_id)s,%(product_name)s,%(supplier_id)s,%(category_id)s,%(quantity_per_unit)s,%(unit_price)s,%(unit_in_stock)s,%(unit_on_order)s,%(reorder_level)s,%(discontinued)s) """
        
        cursor.copy_expert(sql=copy_sql, file=f)
    
    cursor.close()
    conn.commit()
    conn.close()

insert = PythonOperator(
    task_id='customers.csv-sample_stg',
    python_callable=insert_customers,
    dag=dag,
)
extract >> insert