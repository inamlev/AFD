# from airflow import DAG
# from datetime import datetime,timedelta
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# import json
# import logging
# default_args = {
#   "owner": "airflow",
#   "depend_on_past": False,
#   "start_date": datetime(2023, 11, 19),
#   "email_on_failure": False,
#   "email_on_retry": False,
#   "retries": 1,
#   "log_level": "DEBUG",

# }

# fruits_test = ["Apple", "Pear", "Peach", "Banana"]
# def producer_function():
#   for i in fruits_test:
#     yield (json.dumps(i), json.dumps(i + i))


# def consumer_function(message):
#   logging.info("Received message: %s",json.loads(message.value()))

# with DAG(
#   "kafka_DAG",
#   default_args=default_args,
#   description="KafkaOperators",
#   schedule_interval=None,
#   catchup=False,
# ) as dag:

#   t1 = ProduceToTopicOperator(
#     task_id="produce_to_topic",
#     topic="demo_airflow",
#     producer_function=producer_function,        
# 	kafka_config_id='kafka_default',
# ) 

#   t2 = ConsumeFromTopicOperator(
#         task_id="consume_to_topic",
#         topics=['demo_airflow'],
#         apply_function=consumer_function,
#         kafka_config_id='consume_conn'
#     ) 

#     t1 >> t2


from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
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

# employee = [{"name":"Manivel","empID":"101"},{"name":"Saravana","empID":"102"},
#             {"name":"Sri","empID":"103"}]
employee=["mani","saravana","sri","gowtham","bhuvan"]

def producer_function():
    for i in employee:
        yield (json.dumps(i))

def consumer_function(message):
    message_value = json.loads(message.value())
    logging.info("Received message: %s",message_value )
    print(message_value)

with DAG(
    "consumer_DAG",
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

    t1 >> t2
