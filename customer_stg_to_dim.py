from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

 

default_args = {
   'owner': 'Mani',
   'start_date': datetime(2023, 10, 22),
   'retries': 1,
}

 

dag = DAG(
   'customer_stg_to_dim',
   default_args=default_args,
   description='ETL process to insert new/updated records from OLTP to OLAP',
   schedule_interval= None,
)

 

dw_northwind_conn_id = 'postgres_conn_sample1'

 

insert_new_rows = PostgresOperator(
   task_id='insert_into_customers_dim',
   sql='''INSERT INTO customers_dim (date_from, customerid, company_name, contact_name, contact_title, address)
      SELECT
      C.created_date, C.customer_id, C.company_name, C.contact_name, C.contact_title, C.address
      FROM STG_NORTHWIND_SCHEMA.customers C
      LEFT JOIN customers_dim D ON C.customer_id = D.customerid
      WHERE D.customerid IS NULL;''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)

 

update_olap = PostgresOperator(
   task_id='update_customers_dim_history',
   sql='''                                                                                                                                                        UPDATE CUSTOMERS_DIM d
      SET
      date_to = s.created_date,
      current_flag=FALSE
      FROM STG_NORTHWIND_SCHEMA.CUSTOMERS s
      where d.customerid = s.customer_id
      AND (d.company_name <> s.company_name or d.contact_name <> s.contact_name or d.contact_title <> s.contact_title or d.address <> s.address )
      AND (d.current_flag = TRUE);
    ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)

 

insert_updated_to_olap = PostgresOperator(
   task_id='insert_updated_rows_customer_id',
   sql=''' INSERT INTO CUSTOMERS_DIM (date_from,customerid,company_name,contact_name,contact_title,address)
         SELECT c.created_date,c.customer_id,c.company_name,c.contact_name,c.contact_title,c.address
         FROM STG_NORTHWIND_SCHEMA.customers c join customers_dim d  on c.customer_id = d.customerid
         where(d.company_name <> c.company_name or d.contact_name <> c.contact_name or d.contact_title <> c.contact_title or d.address <> c.address)
         AND c.created_date = d.date_to
         AND NOT EXISTS (
      SELECT 1
      FROM CUSTOMERS_DIM x
      WHERE x.customerid = c.customer_id
         AND x.date_from = c.created_date
         AND x.company_name = c.company_name
         AND x.contact_name = c.contact_name
         AND x.contact_title = c.contact_title
         AND x.address = c.address
   );

 

    ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)

 

insert_new_rows >> update_olap >> insert_updated_to_olap