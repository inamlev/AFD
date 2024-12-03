from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
import json
 
default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2023, 11, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}
 
employee = [{"name":"Manivel","empID":"101"},{"name":"Saravana","empID":"102"},
            {"name":"Sri","empID":"103"}]
 
def producer_function():
    for i in employee:
        yield ("keys",json.dumps(i))
 
with DAG(
    "Producer_DAG",
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