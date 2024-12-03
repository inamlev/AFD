from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from confluent_kafka import Producer, Consumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def produce_to_kafka():
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
        # Add more configuration as needed
    }
    producer = Producer(producer_config)

    # Simulating data production
    data = ['producer', 'consumer', 'email', 'branch']
    for item in data:
        producer.produce('demo_topic', value=str(item))  # Replace with your topic name
    producer.flush()

def consume_from_kafka():
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
        'group.id': 'demo_group',  # Replace with your consumer group ID
        'auto.offset.reset': 'earliest',  # Adjust as needed
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['demo_topic'])  # Replace with your topic name

    # Simulating data consumption
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
    return bool(messages)  # Return True if messages were consumed, False otherwise

def decide_email(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='consume_from_kafka')
    if result:
        return 'send_success_email'
    else:
        return 'send_failure_email'

dag = DAG(
    'sri_tag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

produce_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
    dag=dag,
)

consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_from_kafka,
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




# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.email import EmailOperator
# from confluent_kafka import Producer, Consumer
# import json
# import random
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 11, 27),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'email': 'manivel462001@gmail.com',  # Set the email address for notifications
# }


# def produce_to_kafka():
#     # Kafka producer configuration
#     producer_config = {
#         'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
#         # Add more configuration as needed
#     }
#     producer = Producer(producer_config)

#     # Simulating data production
#     employee = ["mani","vel","moorthi"]
#     for i in employee:
#         producer.produce('demo_topic',key="employee",value=i)
#         print(i)  # Replace with your topic name
#     producer.flush()

# def consume_from_kafka():
#     # Kafka consumer configuration
#     consumer_config = {
#         'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
#         'group.id': 'demo_group',  # Replace with your consumer group ID
#         'auto.offset.reset': 'earliest',  # Adjust as needed
#     }
#     consumer = Consumer(consumer_config)
#     consumer.subscribe(['demo_topic'])  # Replace with your topic name

#     # Simulating data consumption
#     messages = []
#     while True:
#         msg = consumer.poll(timeout=1.0)
#         if msg is None:
#             break
#         if msg.error():
#             continue
#         messages.append(msg.value().decode('utf-8'))

#     consumer.close()
#     print("Consumed messages:", messages)
#     return messages  # Return True if messages were consumed, False otherwise

# def check_data_processing_status():
#     # Simulate data processing with a success or failure flag
#     data_processing_status = True if random.random() > 0.5 else False  # Adjust the threshold as needed
#     print(data_processing_status)
#     if data_processing_status:
#         return 'send_success_email'
#     else:
#         return 'send_failure_email'

# def send_success_email():
#     # Implement logic to send success email
#     subject = "Success Notification - Data Processed Successfully"
#     content = "The data processing was successful."
#     to = default_args['email']
#     send_email(subject, content, to)

# def send_failure_email():
#     # Implement logic to send failure email
#     subject = "Failure Notification - Data Processing Failed"
#     content = "The data processing has failed."
#     to = default_args['email']
#     send_email(subject, content, to)

# def send_email(subject, content, to):
#     email_task = EmailOperator(
#         task_id='send_email',
#         to=to,
#         subject=subject,
#         html_content=content
#     )
#     email_task.execute(context=None)

# def decide_email(**kwargs):
#     ti = kwargs['ti']
#     result = ti.xcom_pull(task_ids='consume_from_kafka')
#     print(result)
#     if result:
#         return 'send_success_email'
#     else:
#         return 'send_failure_email'

# with DAG('producer_to_consumer_email_alert',
#           default_args=default_args,
#           description="Kafka Interaction DAG",
#           schedule_interval=timedelta(days=1),
#           catchup=False) as dag:

#     produce_task = PythonOperator(
#         task_id='produce_to_kafka',
#         python_callable=produce_to_kafka,
#     )

#     consume_task = PythonOperator(
#         task_id='consume_from_kafka',
#         python_callable=consume_from_kafka,
#     )

#     check_processing_status = BranchPythonOperator(
#         task_id='check_data_processing_status',
#         python_callable=check_data_processing_status
#     )

#     success_branch = PythonOperator(
#         task_id='send_success_email',
#         python_callable=send_success_email
#     )

#     failure_branch = PythonOperator(
#         task_id='send_failure_email',
#         python_callable=send_failure_email
#     )

#     decision_task = BranchPythonOperator(
#         task_id='make_decision',
#         provide_context=True,
#         python_callable=decide_email,
#     )

#     produce_task >> consume_task >> check_processing_status >> [success_branch, failure_branch]
#     check_processing_status >> success_branch
#     check_processing_status >> failure_branch
#     consume_task >> decision_task
#     decision_task >> success_branch
#     decision_task >> failure_branch
