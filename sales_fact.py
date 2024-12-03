from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
        'owner':'Mani',
        'start_date':datetime(2023, 10, 26),
        'retries':1,
}
dag = DAG(
        'sales_fact',
        default_args=default_args,
        description='ETL process to insert/updated records TO FACT TABLE',
        schedule_interval= None,
)

stg_northwind_conn_id = 'postgres_conn_sample1'

insert_new_rows = PostgresOperator(
      task_id='insert_into_sales_fact',
      sql='''
      INSERT INTO SALES_FACT(LOCATION_ID,DATE_ID,CUSTOMER_ID,PRODUCT_ID,REVENUE,QUANTITY)
      select ld.location_sk,dd.date_sk,cd.customer_sk,pd.product_sk,round(((od.unit_price * od.quantity)*(1 - od.discount)):: integer , 2 ) as revenues,od.quantity
from stg_northwind_schema.order_details od
inner join stg_northwind_schema.orders o on od.order_id = o.order_id
inner join stg_northwind_schema.products p on od.product_id = p.product_id
inner join stg_northwind_schema.customers c on o.customer_id = c.customer_id
inner join products_dim pd on p.product_id = pd.productid
inner join customers_dim cd on c.customer_id = cd.customerid
inner join location_dim ld on o.ship_country = ld.country and o.ship_city = ld.city
inner join date_dim dd on o.order_date :: timestamptz = dd.date
WHERE (SELECT max_created_date FROM max_order_details_created_date) < od.CREATED_DATE :: timestamp;
''',
      postgres_conn_id=stg_northwind_conn_id,
      dag=dag
)

insert_new_rows