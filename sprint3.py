import psycopg2

import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

psql_conn = BaseHook.get_connection('pg_connection')

nickname = 'daria_kolbasina'
cohort = '3'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    df.loc[df['status'] == 'refunded', 'quantity'] *= -1
    df.loc[df['status'] == 'refunded', 'payment_amount'] *= -1

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


def update_mart_f_tables(ti):
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname={psql_conn.schema} port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    cur.execute(
        """delete from mart.f_customer_retention;

    WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT date_id) AS order_count,
        SUM(payment_amount) AS total_revenue,
        MIN(date_id) AS first_order_date,
        DATE_PART('week', date_id) AS period_id
    FROM mart.f_sales
    GROUP BY customer_id, DATEPART(WEEK, date_id)
),
new_customers AS (
    SELECT 
        period_id,
        COUNT(customer_id) AS new_customers_count,
        SUM(total_revenue) AS new_customers_revenue
    FROM staging.customer_orders
    WHERE order_count = 1
    GROUP BY period_id
),
returning_customers AS (
    SELECT 
        period_id,
        COUNT(customer_id) AS returning_customers_count,
        SUM(total_revenue) AS returning_customers_revenue
    FROM staging.customer_orders
    WHERE order_count > 1
    GROUP BY period_id
),
refunded_customers AS (
    SELECT 
        DATE_PART('week', date_id) AS period_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count,
        COUNT(*) AS customers_refunded
    FROM mart.f_sales
    WHERE payment_amount < 0  
    GROUP BY DATE_PART('week', date_id)
)
INSERT INTO mart.f_customer_retention (
    period_name, period_id, item_id,
    new_customers_count, returning_customers_count, refunded_customer_count,
    new_customers_revenue, returning_customers_revenue, customers_refunded
)
SELECT 
    'weekly' AS period_name,
    nc.period_id,
    fds.item_id,
    nc.new_customers_count,
    rc.returning_customers_count,
    rfc.refunded_customer_count,
    nc.new_customers_revenue,
    rc.returning_customers_revenue,
    rfc.customers_refunded
FROM new_customers nc
LEFT JOIN returning_customers rc ON nc.period_id = rc.period_id
LEFT JOIN refunded_customers rfc ON nc.period_id = rfc.period_id
LEFT JOIN mart.f_sales fds ON nc.period_id = DATE_PART('week', fds.date_id)
GROUP BY nc.period_id, fds.item_id, 
         nc.new_customers_count, rc.returning_customers_count, rfc.refunded_customer_count, 
         nc.new_customers_revenue, rc.returning_customers_revenue, rfc.customers_refunded;
"""
        )
    conn.commit()

    cur.close()
    conn.close()

    return 200


args = {
    "owner": "student",
    'email': ['kolbasina9a@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})
    
    upload_user_activity_inc = PythonOperator(
        task_id='upload_user_activity_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                    'filename': 'user_activity_log_inc.csv',
                    'pg_table': 'user_activity_log',
                    'pg_schema': 'staging'})
    
    upload_customer_research_inc = PythonOperator(
        task_id='upload_customer_research_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research_inc.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    # update_f_customer_retention = PostgresOperator(
    #     task_id='update_f_customer_retention',
    #     postgres_conn_id=postgres_conn_id,
    #     sql="sql/mart.f_customer_retention.sql",
    #     parameters={"date": {business_dt}}
    # )
    update_f_customer_retention = PythonOperator(task_id='update_f_customer_retention',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)

    (
            generate_report
            >> get_report
            >> get_increment
            >> [upload_user_order_inc, upload_user_activity_inc, upload_customer_research_inc]
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
            >> update_f_customer_retention
    )
