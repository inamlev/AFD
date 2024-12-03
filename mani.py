# from airflow import DAG
# from datetime import datetime
# from airflow.operators.python_operator import PythonOperator
# from airflow.decorators import XComArg
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# postgres_hook = PostgresHook(postgres_conn_id='emp_to_emp1')

# def extract():
#    # postgres_hook = PostgresHook(postgres_con_id='your_postgres_connection_id')
#     sql="SELECT * FROM emp"
#     results=postgres_hook.get_records(sql)
#     return results

# def insert(ti, **kwargs):
#     # postgres_hook = PostgresHook(postgres_con_id='your_postgres_connection_id')
#     results = ti.xcom_pull(task_ids='extract')
#     if results:
#         insert_sql="INSERT INTO emp1 (emp_id, emp_name, salary) VALUES %s"
#         postgres_hook.insert_rows(table="emp1", rows=results)


# default_args={
#     'owner':'mani',
#     'start_date':datetime(2023,9,25),
#     'retries':1,
#     'depends_on_past':False
# }    
# dag=DAG(
#     dag_id='sample1_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False
# )
# task1=PythonOperator(
#     task_id='task_id',
#     python_callable=extract,
#     dag=dag
# )
# task2=PythonOperator(
#     task_id='task_id',
#     python_callable=insert,
#     provide_context=True,
#     dag=dag
# )
# task1 >> task2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_data():
    source_hook = PostgresHook(postgres_conn_id="emp_to_emp1")
    conn = source_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM emp")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

def insert_data(**kwargs):
    target_hook = PostgresHook(postgres_conn_id="emp_to_emp1")
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    conn = target_hook.get_conn()
    cursor = conn.cursor()
    for row in data:
        cursor.execute("INSERT INTO emp1 VALUES (%s, %s, %s)", row)
    cursor.close()
    conn.commit()
    conn.close()

dag = DAG(
    'sample1_dag',
    default_args={
        'owner': 'Mani',
        'start_date': datetime(2023, 9, 27),
        'retries': 1,
    },
    schedule_interval=None,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    provide_context=True,
    dag=dag,
)
extract_task >> insert_task