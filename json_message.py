from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from confluent_kafka import Producer, Consumer
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def produce_function():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  
    }
    producer = Producer(producer_config)
    
    employee = [{"name": "Manivel", "empID": 101},
            {"name": "Saravana", "empID": 102},
            {"name": "Sri", "empID": 103}]

    for emp in employee:
        emp_json = json.dumps(emp).encode('utf-8')
        producer.produce('demo_topic', value=emp_json)  
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

def decide_email(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='consume_from_kafka')
    if result:
        return 'send_success_email'
    else:
        return 'send_failure_email'

dag = DAG(
    'KAFKA_Failure_Mail_Dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

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

decision_task = BranchPythonOperator(
    task_id='make_decision',
    provide_context=True,
    python_callable=decide_email,
    dag=dag,
)

success_email_task = EmailOperator(
    task_id='send_success_email',
    to='manivel462001@gmail.com',
    subject='Success Notification - Data Processed Successfully.',
    html_content='The data processing was successful.',
    dag=dag,
)

failure_email_task = EmailOperator(
    task_id='send_failure_email',
    to='manivel462001@gmail.com',
    subject='Failure Notification - Data Processing Failed',
    html_content='The data processing has failed.',
    dag=dag,
)

produce_task >> consume_task >> decision_task
decision_task >> [success_email_task, failure_email_task]




