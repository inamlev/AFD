from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
   'owner': 'Bhuvan',
   'start_date': datetime(2023, 9, 28),
   'retries': 1,
}

dag = DAG(
   'ETL_STG_NORTHWIND-DW_NORTHWIND_PRODUCTS_DIM',
   default_args=default_args,
   description='ETL process to insert new/updated records from OLTP to OLAP',
   schedule_interval=None,
)

dw_northwind_conn_id = 'htdvlpgs002-dw_northwind'

insert_new_rows = PostgresOperator(
   task_id='insert_into_products_dim',
   sql='''INSERT INTO products_dim (date_from, productid, productname, category_id, unitprice, unitsinstock, unitsonorder,discontinued)
      SELECT
      C.created_date, C.product_id, c.product_name , c.category_id , C.unit_price , C.units_in_stock , c. units_on_order,c.discontinued
      FROM STG_NORTHWIND_SCHEMA.products C
      LEFT JOIN products_dim D ON C.product_id = D.productid
      WHERE D.productid IS NULL;''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)


update_olap = PostgresOperator(
   task_id='update_products_dim_history',
   sql='''
      UPDATE products_DIM d
      SET
      date_to = s.created_date,
      current_flag=FALSE
      FROM STG_NORTHWIND_SCHEMA.products s
       where d.productid = s.product_id
      AND (d.productname <> s.product_name or d.category_id <> s.category_id or d.unitprice <> s.unit_price  or d.unitsinstock <> s.units_in_stock or d.unitsonorder <> s.units_on_order or d.discontinued <> s.discontinued)
      AND (d.current_flag = TRUE);
    ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)

insert_updated_to_olap = PostgresOperator(
   task_id='insert_updated_rows_product_id',
   sql=''' INSERT INTO products_DIM (date_from, productid, productname, category_id, unitprice, unitsinstock, unitsonorder,discontinued)
         SELECT  C.created_date, C.product_id, c.product_name , c.category_id , C.unit_price , C.units_in_stock , c. units_on_order,c.discontinued
         FROM STG_NORTHWIND_SCHEMA.products c join products_dim d  on c.product_id = d.productid
         where (d.productname <> c.product_name or d.category_id <> c.category_id or d.unitprice <> c.unit_price  or d.unitsinstock <> c.units_in_stock or d.unitsonorder <> c.units_on_order or d.discontinued <> c.discontinued)
         AND c.created_date = d.date_to
         AND NOT EXISTS (
      SELECT 1
      FROM products_DIM x
      WHERE x.productid = c.product_id
         AND x.date_from = c.created_date
         AND x.productname = c.product_name
         AND x.unitprice = c.unit_price
         AND x.category_id = c.category_id
         AND x.unitsinstock = c.units_in_stock
         AND X.unitsonorder = C.units_on_order
         AND X.discontinued = C.discontinued
          );

    ''',
   postgres_conn_id=dw_northwind_conn_id,
   dag=dag,
)

insert_new_rows >> update_olap >> insert_updated_to_olap