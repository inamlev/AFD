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

Project = [{"projectID": "101", "projectname": "project1","status":"failure"},
           {"projectID": "102", "projectname": "project2","status":"failure"},
           {"projectID": "103", "projectname": "project3","status":"failure"}]

def produce_function():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  
    }
    producer = Producer(producer_config)
    Project = [{"projectID": "101", "projectname": "project1","status":"failure"},
           {"projectID": "102", "projectname": "project2","status":"failure"},
           {"projectID": "103", "projectname": "project3","status":"failure"}]
    

    for p in Project:
        producer.produce('demo_topic', value=p)  
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
    # return bool(messages)
    return messages

def decide_branch(**kwargs):

    ti = kwargs['ti']
    # result = ti.xcom_pull(task_ids='consume_from_kafka')
    msg=ti.xcom_pull(task_ids='consume_from_kafka')
    print(msg)
    if msg:
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


from airflow import DAG
import os
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from confluent_kafka import Producer, Consumer
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import json

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2023, 11, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "log_level": "DEBUG",
}

def produce_function(**kwargs):
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  
    }
    producer = Producer(producer_config)
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_from_kafka', key='messages')
    
    for p in messages:
        producer.produce('demo_topic', value=p)  
    producer.flush()  

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
    print("Consumed messages:", messages)
    # return bool(messages)
    
    path="/home/manivel/airflow/data/" 
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(path, f"file_{timestamp}.txt")
    with open(file_path, 'w') as f:
        for msg_info in messages:
            f.write(json.dumps(msg_info) + '\n')
    return messages

def decide_branch(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_from_kafka')

    if messages:
        for msg in messages:
            project = json.loads(msg)

            # Check if 'status' field is present in the project
            if 'status' in project:
                project_status = project['status']

                if project_status == 'success':
                    continue  # Continue to the next project
                else:
                    # Trigger failure DAG for this project
                    return 'trigger_failure_dag'
            else:
                # If 'status' field is not present, consider it a failure
                return 'trigger_failure_dag'

        # If all projects have 'success' status, trigger success DAG
        return 'trigger_success_dag'
    else:
        return 'trigger_failure_dag'


with DAG(
    "KAFKA_Project_Status_Dag",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=None,
    catchup=False,
) as dag:

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
    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_function,
        dag=dag,
    )

    # Set up task dependencies
    consume_task >> decide_branch_task
    decide_branch_task >> [trigger_success_dag, trigger_failure_dag]
    trigger_failure_dag >> produce_task

