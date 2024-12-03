from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import csv
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 9, 11),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'postgres_to_mongodb',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def Extract_region_data():
    hook = PostgresHook(postgres_conn_id="postgres_connection") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    with open("/home/manivel/airflow/data/products.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved region data in csv file products.csv ")

def Insert_csv_data_to_mongodb():
   

        # Define the mongoimport command
        mongoimport_command = [
            'mongoimport',
            '--host', '73.183.198.103',  # MongoDB host
            '--port', '27017',  # MongoDB port
            '--db', 'productDB',  # MongoDB database name
            '--collection', 'productCollection',  # MongoDB collection name
            '--type', 'csv',  # Specify input file type
            '--file', '/home/manivel/airflow/data/products.csv',  # CSV file path
            '--headerline',  # Treat the first line as header
            '--username', 'bb-admin',  # MongoDB username
            '--password', 'AdminBB@2023',  # MongoDB password
        ]


        # Read and log the output and errors
        command_output = stdout.read().decode('utf-8')
        command_errors = stderr.read().decode('utf-8')

        logging.info("Mongoimport command output:")
        logging.info(command_output)

        if command_errors:
            logging.error("Mongoimport command errors:")
            logging.error(command_errors)


extract = PythonOperator(
    task_id="001_bi_user-products.csv",
    python_callable=Extract_region_data,
    dag=dag,
)

insert_to_mongodb = PythonOperator(
    task_id="insert_to_mongodb",
    python_callable=Insert_csv_data_to_mongodb,
    dag=dag,
)

# Set task dependencies
extract >> insert_to_mongodb


#  location_id========> 