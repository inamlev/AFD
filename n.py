from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.email import EmailOperator
import json
import logging

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2023, 11, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "log_level": "DEBUG",
    "email": "manivel462001@gmail.com",  # Set the email address for notifications
}

employee = [{"name": "Manivel", "empID": "101"},
            {"name": "Saravana", "empID": "102"},
            {"name": "Sri", "empID": "103"}]

def producer_function():
    for i in employee:
        yield ("keys", json.dumps(i))

def consumer_function(message):
    # logging.info("Received message: %s", json.loads(message.value()))
    logging.info("Received message: %s", message.value().decode('utf-8'))

def send_success_email(**kwargs):
    # Implement logic to send success email
    subject = "Success Notification - Data Processed Successfully"
    content = "The data processing was successful."
    to = kwargs['dag_run'].conf.get('email', default_args['email'])
    send_email(subject, content, to)

def send_failure_email(**kwargs):
    # Implement logic to send failure email
    subject = "Failure Notification - Data Processing Failed"
    content = "The data processing has failed."
    to = kwargs['dag_run'].conf.get('email', default_args['email'])
    send_email(subject, content, to)

def send_email(subject, content, to):
    email_task = EmailOperator(
        task_id='send_email',
        to=to,
        subject=subject,
        html_content=content,
    )
    email_task.execute(context=None)

with DAG(
    "producer_to_consumer_email_notification1",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="demo_topic",
        producer_function=producer_function,
        kafka_config_id='kafka_default',
    )

    t2 = ConsumeFromTopicOperator(
        task_id="consume_to_topic",
        topics=['demo_topic'],
        apply_function=consumer_function,
        kafka_config_id='kafka_default'
    )

    send_success_email_task = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
        provide_context=True
    )

    send_failure_email_task = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_email,
        provide_context=True
    )

    # Set up task dependencies
    t1 >> t2 >> [send_success_email_task, send_failure_email_task]
