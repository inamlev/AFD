# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# import csv
# import logging
# import subprocess
# from airflow.models import Variable
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# default_args = {
#     'owner': 'Airflow',
#     'start_date': datetime(2023, 9, 11),
#     'depends_on_past': False,
#     'retries': 1,
# }

# dag = DAG(
#     'wf_002_STG_REGION1_DAG',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False,
# )

# def Extract_region_data():
#     hook = PostgresHook(postgres_conn_id="htdvlpgs002-stg_northwind") 
#     conn = hook.get_conn()
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM REGION")
#     with open("/home/bb-admin/airflow/data/wf_stg_region1.csv", "w") as f:
#         csv_writer = csv.writer(f)
#         csv_writer.writerow([i[0] for i in cursor.description])
#         csv_writer.writerows(cursor)
#     cursor.close()
#     conn.close()
#     logging.info("Saved region data in csv file wf_stg_region1")

# def Insert_csv_data_to_mongodb():
#     # Retrieve the MongoDB password from Airflow's Variable
#     mongo_password = "AdminBB@2023"
#     # Use the subprocess module to execute the mongoimport command
#     mongoimport_command = [
#         'mongoimport',
#         '--host', '73.183.198.103',  # MongoDB host
#         '--port', '27017',               # MongoDB port
#         '--db', 'regionDB',         # MongoDB database name
#         '--collection', 'regionCollection',# MongoDB collection name
#         '--type', 'csv',                 # Specify input file type
#         '--file', '/home/bb-admin/airflow/data/wf_stg_region1.csv',  # CSV file path
#         '--headerline'                   # Treat the first line as header
#          '--username', 'bb-admin@73.183.198.103',   # MongoDB username
#         '--password', mongo_password,    # MongoDB password (retrieved from Airflow Variable)
#     ]

#     try:
#         logging.info("Inserting CSV data into MongoDB...")
#         subprocess.run(mongoimport_command, check=True)
#         logging.info("CSV data inserted successfully into MongoDB.")
#     except subprocess.CalledProcessError as e:
#         logging.error(f"Error while inserting CSV data into MongoDB: {e}")

# extract = PythonOperator(
#     task_id="001_bi_user-region.csv",
#     python_callable=Extract_region_data,
#     dag=dag,
# )

# insert_to_mongodb = PythonOperator(
#     task_id="insert_to_mongodb",
#     python_callable=Insert_csv_data_to_mongodb,
#     dag=dag,
# )

# # Set task dependencies
# extract >> insert_to_mongodb

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import csv
import logging
# import subprocess
import paramiko # Import the paramiko library
# from airflow.models import Variable
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

# Define SSH connection parameters
ssh_hostname = '73.183.198.103'
ssh_port = 22  # SSH port (default is 22)
ssh_username = 'bb-admin@73.183.198.103'
ssh_password = 'AdminBB@2023'  # Or use key-based authentication

def Extract_region_data():
    hook = PostgresHook(postgres_conn_id="htdvlpgs002-stg_northwind") 
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM REGION")
    with open("/home/bb-admin/airflow/data/wf_stg_region1.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved region data in csv file wf_stg_region1")

def Insert_csv_data_to_mongodb():
    try:
        # Create an SSH client instance
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote server
        ssh_client.connect(ssh_hostname, ssh_port, ssh_username, ssh_password)

        # Define the mongoimport command
        mongoimport_command = [
            'mongoimport',
            '--host', '73.183.198.103',  # MongoDB host
            '--port', '27017',  # MongoDB port
            '--db', 'regionDB',  # MongoDB database name
            '--collection', 'regionCollection',  # MongoDB collection name
            '--type', 'csv',  # Specify input file type
            '--file', '/home/bb-admin/airflow/data/wf_stg_region1.csv',  # CSV file path
            '--headerline',  # Treat the first line as header
            '--username', 'bb-admin@73.183.198.103',  # MongoDB username
            '--password', 'AdminBB@2023',  # MongoDB password
        ]

        # Execute the mongoimport command on the remote server
        ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(' '.join(mongoimport_command))

        # Read and log the output and errors
        command_output = ssh_stdout.read().decode('utf-8')
        command_errors = ssh_stderr.read().decode('utf-8')

        logging.info("Mongoimport command output:")
        logging.info(command_output)

        if command_errors:
            logging.error("Mongoimport command errors:")
            logging.error(command_errors)

    except Exception as e:
        logging.error(f"Error while inserting CSV data into MongoDB via SSH: {e}")

    finally:
        # Close the SSH connection
        ssh_client.close()

extract = PythonOperator(
    task_id="001_bi_user-region.csv",
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
