from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from confluent_kafka import Producer, Consumer
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

def produce_function():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  
    }
    producer = Producer(producer_config)
    
    employee = [{"name": "Manivel", "empID": "101"},
            {"name": "Saravana", "empID": "102"},
            {"name": "Sri", "empID": "103"}]

    for emp in employee:
        producer.produce('demo_topic', value=emp)  
    producer.flush()

def consume_function():
    
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  
        'group.id': 'demo_group',  
        'auto.offset.reset': 'earliest',  
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
    print("Consumed messages:", messages)
    return bool(messages)

def decide_branch(**kwargs):

    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='consume_from_kafka')
    print(result)
    if result:
        return 'trigger_success_dag'
    else:
        return 'trigger_failure_dag'

with DAG(
    "KAFKA_Email_Dag_Trigger",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=None,
    catchup=False,
) as dag:

    produce_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_function,
    dag=dag,
    )

    consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_function,
    dag=dag,
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
    produce_task >> consume_task >> decide_branch_task
    decide_branch_task >> [trigger_success_dag, trigger_failure_dag]
