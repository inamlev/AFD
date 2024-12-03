from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'Mani',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['manivel462001@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id="success_email",
    start_date=datetime(2023, 11, 17),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='simple_failed_bash_task',
        bash_command="cd non_exist_folder"
    )

    # Define the EmailOperator to send an email when the task fails
    email_notification = EmailOperator(
        task_id='email_notification',
        to=['manivel462001@gmail.com'],
        subject='Success Notification - Data Processed Successfully.',
        html_content='The data processing was successful.',
        dag=dag,
    )

    # Set up the task dependencies
    task1 >> email_notification
