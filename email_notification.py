from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {  
     'owner': 'Mani',  
     'retries': 1,  
     'retry_delay': timedelta(seconds=5),
    'email': ['kbhvankumar11@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True}

with DAG (    
    dag_id="dag_email_notification_v02",
    start_date=datetime(2023, 11, 17),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False 
) as dag:
    task1 = BashOperator(
        task_id='simple_failed_bash_task',
        bash_command="cd non_exist_folder"
        )