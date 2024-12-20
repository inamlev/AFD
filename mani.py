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
        print(msg)
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
    'producer_to_consumer_email_alert_1',
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