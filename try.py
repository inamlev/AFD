from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python_operator import  BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
import json
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'log_level':'DEBUG'
}

employee=["mani","saravana","sri","gowtham","bhuvan"]

def producer_function():
    for i in employee:
        yield (json.dumps("employee"),json.dumps(i))
        print(i)


def consumer_function(message):
    key=json.loads(message.key())
    value=json.loads(message.value())
    print(f"Received message:--key-->{key}----value-->{value}")
    logging.info("Received message:")

    return bool(json.loads(message.value()))

def decide_email(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='consumer_function')
    print()
    if result:
        return 'send_success_email'
    else:
        return 'send_failure_email'
    

with DAG(
    'kafka_mail_alert',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    produce_task = ProduceToTopicOperator(
            task_id="produce_to_topic",
            topic="demo_topic",
            producer_function=producer_function,
            kafka_config_id='kafka_default',
        )
    consume_task = ConsumeFromTopicOperator(
            task_id="consume_to_topic",
            topics=['demo_topic'],
            apply_function=consumer_function,
            kafka_config_id='kafka_default'
        )
    decision_task = BranchPythonOperator(
        task_id='make_decision',
        provide_context=True,
        python_callable=decide_email,
        dag=dag,
    )
    success_email_task = EmailOperator(
        task_id='send_success_email',
        to='manivel462001@gmail.com',
        subject='Kafka Consumption Success',
        html_content='Messages were successfully consumed from Kafka!',
        dag=dag,
    )
    failure_email_task = EmailOperator(
        task_id='send_failure_email',
        to='manivel462001@gmail.com',
        subject='Kafka Consumption Failure',
        html_content='Failed to consume messages from Kafka.',
        dag=dag,
    )

    produce_task >> consume_task >> decision_task
    decision_task >> [success_email_task, failure_email_task]