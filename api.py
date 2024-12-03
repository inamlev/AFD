from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests


# Define a function to get the access token from the Airflow connection

def get_access_token():
    conn = BaseHook.get_connection("my_api_connection")
    access_token = conn.password  # Retrieve the access token from the password field
    return access_token

# Define the default_args for the DAG

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 26),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_trigger_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False,
)

# Define tasks

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

# Define a function to perform an API request using the access token

def api_request():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
         
    # Make your API request here, for example:

    response = requests.post(
        url='http://192.168.1.7:8080/dagsrun',  # Replace with your API endpoint
        headers=headers,
        json={"key": "value"}  # Include your request data here
    )

    # Process the API response

    print(response.status_code)

    print(response.json())

# Define a PythonOperator to make the API request

api_request_task = PythonOperator(
    task_id='api_request_task',
    python_callable=api_request,
    dag=dag,
)

# Set task dependencies

start_task >> api_request_task

         