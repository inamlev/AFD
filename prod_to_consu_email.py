from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
}

employee = [{"name": "Manivel", "empID": "101"},
            {"name": "Saravana", "empID": "102"},
            {"name": "Sri", "empID": "103"}]

def producer_function():
    for i in employee:
        yield ("keys", json.dumps(i))

def consumer_function(message):
    logging.info("Received message: %s", json.loads(message.value()))

def decide_branch(**kwargs):
    # You can implement your logic here to decide whether to go to the success or failure path
    # For example, you can check the message received in the consumer function
    # and determine success or failure based on some condition.
    if kwargs['task_instance'].xcom_pull(task_ids='consume_to_topic'):
        return 'trigger_success_dag'
    else:
        return 'trigger_failure_dag'

with DAG(
    "producer_to_consumer_email_notification",
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
        kafka_config_id='kafka_consume'
    )

    decide_branch_task = BranchPythonOperator(
        task_id='decide_branch',
        python_callable=decide_branch,
        provide_context=True
    )

    trigger_success_dag = TriggerDagRunOperator(
        task_id='trigger_success_dag',
        trigger_dag_id='success_email',
        dag=dag
    )

    trigger_failure_dag = TriggerDagRunOperator(
        task_id='trigger_failure_dag',
        trigger_dag_id='failure_email',
        dag=dag
    )

    # Set up task dependencies
    t1 >> t2 >> decide_branch_task
    decide_branch_task >> trigger_success_dag
    decide_branch_task >> trigger_failure_dag
