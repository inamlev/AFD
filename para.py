from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    'wf_PASS_PARAMETER_THROUGH_API',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 2),
        'retries': 1
    },
    # Define your desired schedule interval or set it to None
    schedule_interval=None,
    catchup=False,  # Set to False if you don't want to backfill historical runs
)

# Define the 'push' function
def push(**context):
    Name = context['dag_run'].conf['name']
    Emp_id = context['dag_run'].conf['emp_id']
    run_id = context['dag_run'].run_id
    print(Name, Emp_id, run_id)
    return Name

# Create a PythonOperator to execute the 'push' function
push_task = PythonOperator(
    task_id='push_task',
    python_callable=push,
    provide_context=True,
    dag=dag
)

# Define the 'pull' function
def pull(ti):
    Name1 = ti.xcom_pull(task_ids='push_task', key='return_value')
    print(Name1)

# Create a PythonOperator to execute the 'pull' function
pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull,
    provide_context=True,
    dag=dag
)

# Set the task dependencies
push_task >> pull_task