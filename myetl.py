from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'Mani',
   'start_date': datetime(2023, 9, 26),
   'retries': 1,
}

dag = DAG(
   'oltp_to_olap_etl',
   default_args=default_args,
   description='ETL process to insert new/updated records from OLTP to OLAP',
   schedule_interval='@daily',
)

customers_conn_id = 'postgres-server1'
customers_dim_conn_id = 'postgres-server2'

insert_new_rows = PostgresOperator(
   task_id='extract_from_oltp',
   sql=''' INSERT INTO customers_dim (date_from, customer_id, company_name, contact_name, contact_title, address)
           SELECT 
           C.created_date, C.customer_id, C.company_name, C.contact_name, C.contact_title, C.address
           FROM customers C
           LEFT JOIN customers_dim D ON C.customer_id = D.customer_id
           WHERE D.customer_id IS NULL;''',
   postgres_conn_id=customers_dim_conn_id, 
   database='my_user',  
   dag=dag,
)

# insert_new_rows = PostgresOperator(
#    task_id='extract_from_oltp',
#    sql=''' INSERT INTO customers_dim (date_from, customer_id, company_name, contact_name, contact_title, address)
# SELECT 
#    C.created_date, C.customer_id, C.company_name, C.contact_name, C.contact_title, C.address
# FROM customers C
# WHERE NOT EXISTS (
#    SELECT 1
#    FROM customers_dim D
#    WHERE C.customer_id = D.customer_id
# );''',
#    postgres_conn_id=customers_dim_conn_id, 
#    database='my_user',  
#    dag=dag,
# )