from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from confluent_kafka import Producer, Consumer
from airflow.operators.email import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import json

default_args = {
    'owner': 'Mani',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['manivel462001@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

def consume_function():
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  
        'group.id': 'demo_group',  
        'auto.offset.reset': 'beginning',  
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['demo_topic'])  

    messages = []
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break
        if msg.error():
            continue
        messages.append(msg.value().decode('utf-8'))
    consumer.close()
    return messages
def produce_function(**kwargs):
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
    }
    producer = Producer(producer_config)
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_from_kafka')

    # Assuming each message is a JSON string
    projects = json.loads(messages)

    for project in projects:
        # Check if 'status' field is present in the project
        if 'status' in project:
            # Update the 'status' field to 'success'
            project['status'] = 'success'
        else:
            # If 'status' field is not present, add it with 'success'
            project['status'] = 'success'

        # Produce the updated project to the Kafka topic
        producer.produce('demo_topic', value=json.dumps(project))

    producer.flush()


with DAG(
    dag_id="Project_Failure_Alert",
    start_date=datetime(2023, 11, 17),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    # task1 = BashOperator(
    #     task_id='simple_failed_bash_task',
    #     bash_command="cd non_exist_folder"
    # )

    # Define the EmailOperator to send an email when the task fails
    email_notification = EmailOperator(
        task_id='email_notification',
        to=['manivel462001@gmail.com'],
        subject='Failure Notification - Project Status Failure',
        html_content='The Project status is failure.',
        dag=dag,
    )
    consume_task = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_function,
        dag=dag,
    )
    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_function,
        dag=dag,
    )
    trigger_project_status_dag = TriggerDagRunOperator(
        task_id='trigger_project_status_dag',
        trigger_dag_id='KAFKA_Project_Status_Dag',
        dag=dag
    )

    # Set up the task dependencies
    email_notification >> consume_task >> produce_task >> trigger_project_status_dag
