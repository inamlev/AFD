from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'Mani',
   'start_date': datetime(2023, 10, 26),
   'retries': 1,
}

dag = DAG(
   'location_dim',
   default_args=default_args,
   description='ETL process to insert new/updated records from OLTP to OLAP',
   schedule_interval=None,
)

dw_northwind_conn_id = 'postgres_conn_sample1'

insert_new_rows = PostgresOperator(
   task_id='insert_into_location_dim',
   sql='''
      INSERT INTO location_dim (date_from,country,city)
      SELECT
      C.created_date,C.country, C.city
      FROM STG_NORTHWIND_SCHEMA.location C
      LEFT JOIN location_dim D ON C.country = D.country and C.city = D.city
      WHERE (D.country IS NULL and D.city IS NULL)
      ''',
    postgres_conn_id=dw_northwind_conn_id,
    dag=dag,
)

insert_new_rows