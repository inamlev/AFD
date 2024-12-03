# # from airflow.decorators import dag, task
# # from pendulum import datetime
# # from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# # from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# # from airflow.operators.python import BranchPythonOperator
# # from airflow.operators.email import EmailOperator
# # from airflow.models import TaskInstance
# # import json
# # import random


# # employee=["mani","saravana","sri","gowtham","bhuvan"]
# # # Define producer and consumer functions
# # def prod_function(num_treats, pet_name):
# #     for i in employee:
# #         yield (json.dumps("employee"), json.dumps(i))
        

# # def consume_function(message, name):
# #     if message.key() is not None:
# #         key = json.loads(message.key())
# #         value = json.loads(message.value())
        
# #         print(
# #             f"{key} ------- {value}!"
# #         )
# #     else:
# #         print("Message key is None. Skipping processing.")

# # # Define the DAG
# # @dag(
# #     'email_kafka_alert',
# #     start_date=datetime(2023, 4, 1),
# #     schedule=None,
# #     catchup=False,
# #     render_template_as_native_obj=True,
# # )
# # def produce_consume_treats():
# #     @task
# #     def get_your_pet_name(pet_name=None):
# #         return pet_name

# #     @task
# #     def get_number_of_treats(num_treats=None):
# #         return num_treats

# #     @task
# #     def get_pet_owner_name(your_name=None):
# #         return your_name

# #     produce_treats = ProduceToTopicOperator(
# #         task_id="produce_treats",
# #         kafka_config_id="kafka_default",
# #         topic=["demo_topic"],
# #         producer_function=prod_function,
# #         producer_function_args=["{{ ti.xcom_pull(task_ids='get_number_of_treats')}}"],
# #         producer_function_kwargs={
# #             "pet_name": "{{ ti.xcom_pull(task_ids='get_your_pet_name')}}"
# #         },
# #         poll_timeout=10,
# #     )

# #     consume_treats = ConsumeFromTopicOperator(
# #         task_id="consume_treats",
# #         kafka_config_id="kafka_default",
# #         topics=["demo_topic"],
# #         apply_function=consume_function,
# #         apply_function_kwargs={
# #             "name": "{{ ti.xcom_pull(task_ids='get_pet_owner_name')}}"
# #         },
# #         poll_timeout=20,
# #         max_messages=20,
# #         max_batch_size=20,
# #     )

# #     def check_consume_result(**kwargs):
# #         task_instance = kwargs['ti']
# #         consume_treats_state = task_instance.get_dagrun().get_task_instance('consume_treats').current_state()
# #         print(f"consume_treats_state: {consume_treats_state}")
# #         if consume_treats_state == 'success':
# #             return 'send_success_email'
# #         else:
# #             return 'send_failure_email'

# #     check_consume = BranchPythonOperator(
# #         task_id='check_consume',
# #         python_callable=check_consume_result,
# #     )

# #     send_success_email = EmailOperator(
# #         task_id='send_success_email',
# #         to='manivel462001@gmail.com',
# #         subject='Successful Data Consumption Alert',
# #         html_content='The data from Kafka was successfully consumed and processed. Details are in the logs.',
# #     )

# #     send_failure_email = EmailOperator(
# #         task_id='send_failure_email',
# #         to='manivel462001@gmail.com',
# #         subject='Failure Alert: Data Consumption from Kafka',
# #         html_content='There was a failure in consuming data from Kafka. Please check the logs for more details.',
# #     )

# #     [get_your_pet_name(YOUR_PET_NAME), get_number_of_treats(NUMBER_OF_TREATS)] >> produce_treats
# #     get_pet_owner_name(YOUR_NAME) >> consume_treats
# #     consume_treats >> check_consume
# #     check_consume >> [send_success_email, send_failure_email]

# # # Instantiate the DAG
# # produce_consume_treats = produce_consume_treats()



# from airflow.decorators import dag, task
# from pendulum import datetime
# from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# from airflow.operators.python import BranchPythonOperator
# from airflow.operators.email import EmailOperator
# from airflow.models import TaskInstance
# import json
# import random

# employee = ["mani", "saravana", "sri", "gowtham", "bhuvan"]
# msg1=[]

# # Define producer and consumer functions
# def prod_function(num_treats, pet_name):
#     for i in range(num_treats):
#         for emp in employee:
#             yield (json.dumps("employee"), json.dumps(emp))

# def consume_function(message, name):
#     if message.key() is not None:
#         key = json.loads(message.key())
#         value = json.loads(message.value())
#         msg1=value
#         print(f"{key} ------- {value}!")
#     else:
#         print("Message key is None. Skipping processing.")

# @dag(
#     'email_kafka_alert',
#     start_date=datetime(2023, 4, 1),
#     schedule=None,
#     catchup=False,
#     render_template_as_native_obj=True,
# )
# def produce_consume_treats():

#     @task
#     def get_number_of_treats(num_treats=None):
#         return num_treats

#     @task
#     def get_pet_owner_name(your_name=None):
#         return your_name

#     produce_treats = ProduceToTopicOperator(
#         task_id="produce_treats",
#         kafka_config_id="kafka_default",
#         topic="demo_topic",  # Change to string
#         producer_function=prod_function,
#         producer_function_args=["{{ ti.xcom_pull(task_ids='get_number_of_treats')}}"],
#         producer_function_kwargs={
#             "pet_name": "{{ ti.xcom_pull(task_ids='get_your_pet_name')}}"
#         },
#         poll_timeout=10,
#     )

#     consume_treats = ConsumeFromTopicOperator(
#         task_id="consume_treats",
#         kafka_config_id="kafka_default",
#         topics="demo_topic",  # Change to string
#         apply_function=consume_function,
#         apply_function_kwargs={
#             "name": "{{ ti.xcom_pull(task_ids='get_pet_owner_name')}}"
#         },
#         poll_timeout=20,
#         max_messages=20,
#         max_batch_size=20,
#     )

#     def check_consume_result(**kwargs):
#         task_instance = kwargs['ti']
#         consume_treats_state = task_instance.get_dagrun().get_task_instance('consume_treats').current_state()
#         print(f"consume_treats_state: {consume_treats_state}")
#         if consume_treats_state == 'success':
#             return 'send_success_email'
#         else:
#             return 'send_failure_email'

#     check_consume = BranchPythonOperator(
#         task_id='check_consume',
#         python_callable=check_consume_result,
#     )

#     send_success_email = EmailOperator(
#         task_id='send_success_email',
#         to='manivel462001@gmail.com',
#         subject='Successful Data Consumption Alert',
#         html_content='The data from Kafka was successfully consumed and processed. Details are in the logs.',
#     )

#     send_failure_email = EmailOperator(
#         task_id='send_failure_email',
#         to='manivel462001@gmail.com',
#         subject='Failure Alert: Data Consumption from Kafka',
#         html_content='There was a failure in consuming data from Kafka. Please check the logs for more details.',
#     )

#     [get_your_pet_name(), get_number_of_treats()] >> produce_treats
#     get_pet_owner_name() >> consume_treats
#     consume_treats >> check_consume
#     check_consume >> [send_success_email, send_failure_email]

# # Instantiate the DAG
# produce_consume_treats = produce_consume_treats()


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from confluent_kafka import Producer, Consumer
import json
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': 'manivel462001@gmail.com',  # Set the email address for notifications
}

# Sample employee data
employee = ["mani", "saravana", "sri", "gowtham", "bhuvan"]

def producer_function():
    for i in employee:
        yield (json.dumps("employee"), json.dumps(i))
        print(i)

def consumer_function(message):
    message_value = json.loads(message.value())
    message_key = json.loads(message.key())
    print(message_key, message_value)
    # Returning True to indicate that the message was received
    return True

def produce_to_kafka():
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
        # Add more configuration as needed
    }
    producer = Producer(producer_config)

    # Simulating data production
    messages = []
    for key, value in producer_function():
        producer.produce('demo_topic', key=key, value=value)  # Replace with your topic name
        messages.append((key, value))

    producer.flush()
    return messages

def consume_from_kafka(message):
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
        'group.id': 'demo_group',  # Replace with your consumer group ID
        'auto.offset.reset': 'beginning',  # Adjust as needed
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
        messages.append(json.loads(message.value()))

    consumer.close()
    print("Consumed messages:", messages)
    return bool(messages)  # Return True if messages were consumed, False otherwise

def check_data_processing_status():
    # Simulate data processing with a success or failure flag
    data_processing_status = True if random.random() > 1.0 else False

    if data_processing_status:
        return 'send_success_email'
    else:
        return 'send_failure_email'

def send_success_email():
    # Implement logic to send success email
    subject = "Success Notification - Data Processed Successfully"
    content = "The data processing was successful."
    to = default_args['email']
    send_email(subject, content, to)

def send_failure_email():
    # Implement logic to send failure email
    subject = "Failure Notification - Data Processing Failed"
    content = "The data processing has failed."
    to = default_args['email']
    send_email(subject, content, to)

def send_email(subject, content, to):
    email_task = EmailOperator(
        task_id='send_email',
        to=to,
        subject=subject,
        html_content=content
    )
    email_task.execute(context=None)

def decide_email(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='consume_from_kafka')
    if result:
        return 'send_success_email'
    else:
        return 'send_failure_email'

with DAG('producer_to_consumer_email_alert',
          default_args=default_args,
          description="producer_to_consumer_email_alert",
          schedule_interval=timedelta(days=1),
          catchup=False) as dag:

    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka,
    )

    consume_task = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_from_kafka,
    )

    check_processing_status = BranchPythonOperator(
        task_id='check_data_processing_status',
        python_callable=check_data_processing_status
    )

    success_branch = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email
    )

    failure_branch = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_email
    )

    decision_task = BranchPythonOperator(
        task_id='make_decision',
        provide_context=True,
        python_callable=decide_email,
    )

    produce_task >> consume_task >> check_processing_status >> [success_branch, failure_branch]
    check_processing_status >> success_branch
    check_processing_status >> failure_branch
    consume_task >> decision_task
    decision_task >> [success_branch, failure_branch]