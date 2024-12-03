# from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from datetime import datetime, timedelta

 

# default_args = {

#    'owner': 'Bhuvan',

#    'start_date': datetime(2023, 9, 24),

#    'retries': 1,

# }

 

# dag = DAG(

#    'oltp_to_olap_etl',
#    default_args=default_args,
#    description='ETL process to insert new/updated records from OLTP to OLAP',
#    schedule_interval='@daily',
# )

 


# insert_new_rows = PostgresOperator(

#    task_id='extract_from_oltp',
#    sql=''' 
#            INSERT INTO customers_dim (date_from, customer_id, company_name, contact_name, contact_title, address)
#            SELECT 
#            C.created_date, C.customer_id, C.company_name, C.contact_name, C.contact_title, C.address
#            FROM customers C
#            LEFT JOIN customers_dim D ON C.customer_id = D.customer_id
#            WHERE D.customer_id IS NULL;
#        ''',
#    postgres_conn_id='postgres-server-1',
#    database='my_user',
#    dag=dag,

# )


# update_olap = PostgresOperator(
#         task_id='update_olap',
#         sql='''
#        UPDATE CUSTOMERS_DIM d
#           SET
#           date_to = s.created_date,
#           current_flag=FALSE
#           FROM CUSTOMERS s
#           where d.customer_id = s.customer_id
#           AND (d.company_name <> s.company_name or d.contact_name <> s.contact_name or d.contact_title <> s.contact_title or d.address <> s.address )
# 		  AND (d.current_flag = TRUE);
#         ''',
#         postgres_conn_id='postgres-server1',
#         database='my_user',
#         dag=dag,
#     )

# insert_updatd_to_olap = PostgresOperator(
#    task_id='insert_updated',
#    sql='''INSERT INTO CUSTOMERS_DIM (date_from,customer_id,company_name,contact_name,contact_title,address)
#          SELECT c.created_date,c.customer_id,c.company_name,c.contact_name,c.contact_title,c.address
#          FROM customers c join customers_dim d  on c.customer_id = d.customer_id
# 		   where d.customer_id = s.customer_id
#          AND (d.company_name <> c.company_name or d.contact_name <> c.contact_name or d.contact_title <> c.contact_title or d.address <> c.address )
#          AND c.created_date = d.date_to;
# ''',
#    postgres_conn_id='Staging_northwindsql',  
#    database='postgres-server1',
#    dag=dag,
# )


# insert_new_rows >> update_olap >> insert_updatd_to_olap


# ALTER SEQUENCE customers_dim_customer_id_seq RESTART WITH 0;
# ALTER SEQUENCE customers_dim_customer_id_seq RESTART WITH 0;
# INSERT INTO customers VALUES ('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'Obere Str. 57', 'Berlin', NULL, '12209', 'Germany', '030-0074321', '030-0076545');
# INSERT INTO customers VALUES ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745');
# INSERT INTO customers VALUES ('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Owner', 'Mataderos  2312', 'México D.F.', NULL, '05023', 'Mexico', '(5) 555-3932', NULL);
# INSERT INTO customers VALUES ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', NULL, 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750');
# INSERT INTO customers VALUES ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Order Administrator', 'Berguvsvägen  8', 'Luleå', NULL, 'S-958 22', 'Sweden', '0921-12 34 65', '0921-12 34 67');
# INSERT INTO customers VALUES ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924');
# INSERT INTO customers VALUES ('BLONP', 'Blondesddsl père et fils', 'Frédérique Citeaux', 'Marketing Manager', '24, place Kléber', 'Strasbourg', NULL, '67000', 'France', '88.60.15.31', '88.60.15.32');
# INSERT INTO customers VALUES ('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Owner', 'C/ Araquil, 67', 'Madrid', NULL, '28023', 'Spain', '(91) 555 22 82', '(91) 555 91 99');

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

update_olap = PostgresOperator(
   task_id='update_olap',
   sql='''
       UPDATE CUSTOMERS_DIM d
          SET
          date_to = s.created_date,
          current_flag=FALSE
          FROM CUSTOMERS s
          where d.customer_id = s.customer_id
          AND (d.company_name <> s.company_name or d.contact_name <> s.contact_name or d.contact_title <> s.contact_title or d.address <> s.address )
          AND (d.current_flag = TRUE);
    ''',
   postgres_conn_id=customers_dim_conn_id,  
   database='my_user',  
   dag=dag,
)

insert_updatd_to_olap = PostgresOperator(
   task_id='insert_updated',
   sql='''INSERT INTO CUSTOMERS_DIM (date_from,customer_id,company_name,contact_name,contact_title,address)
         SELECT c.created_date,c.customer_id,c.company_name,c.contact_name,c.contact_title,c.address
         FROM customers c join customers_dim d  on c.customer_id = d.customer_id
         where  (d.company_name <> c.company_name or d.contact_name <> c.contact_name or d.contact_title <> c.contact_title or d.address <> c.address );
    ''',
   postgres_conn_id=customers_dim_conn_id,  
   database='my_user',  
   dag=dag,
)

insert_new_rows >> update_olap >> insert_updatd_to_olap
