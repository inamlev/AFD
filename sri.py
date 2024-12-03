from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_and_insert():
    # Create a PostgresHook instance to connect to your PostgreSQL database
    postgres_hook = PostgresHook(postgres_conn_id='emp_to_emp1')
    
    # Extract data from the 'emp' table
    sql = "SELECT * FROM emp"
    results = postgres_hook.get_records(sql)
    
    # Insert data into the 'emp1' table
    if results:
        insert_sql = "INSERT INTO emp1 (emp_id, emp_name, date_of_birth, salary) VALUES %s"
        postgres_hook.insert_rows(table="emp1", rows=results)
    
default_args = {
    'owner': 'sri',
    'start_date': datetime(2023, 9, 22),  # Specify the date in the correct format
    'retries': 1,
    'depends_on_past': False
}

dag = DAG(
    dag_id='sample_dag',
    default_args=default_args,
    schedule_interval=None,  # Changed to None for no automatic scheduling
    catchup=False
)

# Define a PythonOperator to run the extract_and_insert function
task1 = PythonOperator(
    task_id='task_id',
    python_callable=extract_and_insert,
    dag=dag
)

# Set the task dependencies if needed
# task1 >> task2

if __name__ == "__main__":
    dag.cli()
