from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'Mani',
   'start_date': datetime(2023, 10, 26),
   'retries': 1,
}

dag = DAG(
   'date_dim',
   default_args=default_args,
   description='ETL process to insert new/updated records from OLTP to OLAP',

   schedule_interval=None,
)

dw_northwind_conn_id = 'postgres_conn_sample1'

insert_new_rows = PostgresOperator(
   task_id='insert_into_date_dim',
   sql='''
   INSERT INTO DATE_DIM (DATE,YEAR,TRIMESTRE,MONTH,DAY)
Select d,extract('year' from d),extract('quarter' from d),extract('month' from d),extract('day' from d) from generate_series((select min(order_date) from stg_northwind_schema.orders),(select CASE
WHEN MAX (ORDER_DATE) > MAX(REQUIRED_DATE)
   THEN
     CASE
       WHEN MAX(ORDER_DATE) > MAX(SHIPPED_DATE)
         THEN
           MAX(ORDER_DATE)
         ELSE
           MAX(SHIPPED_DATE)
         END
WHEN MAX(REQUIRED_DATE) > MAX(SHIPPED_DATE)
  THEN MAX(REQUIRED_DATE)
ELSE MAX(SHIPPED_DATE)
END AS MAX_DATE
from stg_northwind_schema.orders),interval '1 days') as d
WHERE (SELECT COUNT(*) FROM DATE_DIM) = 1 and (SELECT DATE FROM DATE_DIM) IS NULL;
      ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)


insert_updated_to_olap = PostgresOperator(
   task_id='insert_updated_rows_date_dim',
   sql='''
        INSERT INTO DATE_DIM (DATE,YEAR,TRIMESTRE,MONTH,DAY)
Select d,extract('year' from d),extract('quarter' from d),extract('month' from d),extract('day' from d) from generate_series((select max(date) from DATE_DIM)+INTERVAL '1 days',(select CASE
WHEN MAX (ORDER_DATE) > MAX(REQUIRED_DATE)
   THEN
     CASE
       WHEN MAX(ORDER_DATE) > MAX(SHIPPED_DATE)
         THEN
           MAX(ORDER_DATE)
         ELSE
           MAX(SHIPPED_DATE)
         END
WHEN MAX(REQUIRED_DATE) > MAX(SHIPPED_DATE)
  THEN MAX(REQUIRED_DATE)
ELSE MAX(SHIPPED_DATE)
END AS MAX_DATE
from stg_northwind_schema.orders),interval '1 days') as d
WHERE (select max(date) from DATE_DIM) < (SELECT MAX(ORDER_DATE)    FROM STG_NORTHWIND_SCHEMA.ORDERS) OR
      (select max(date) from DATE_DIM) < (SELECT MAX(SHIPPED_DATE)  FROM STG_NORTHWIND_SCHEMA.ORDERS) OR
      (select max(date) from DATE_DIM) < (SELECT MAX(REQUIRED_DATE) FROM STG_NORTHWIND_SCHEMA.ORDERS);                  

   ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)
insert_new_rows >> insert_updated_to_olap