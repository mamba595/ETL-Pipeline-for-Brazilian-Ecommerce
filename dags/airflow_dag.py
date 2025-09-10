from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

with DAG('glue_etl_dag', start_date=datetime(2025, 10, 1), schedule_interval='@daily', catchup=False) as dag:
    clean_orders_task = GlueJobOperator(
        task_id='cleaning_orders',
        job_name='cleaning_orders',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '100'
        },
        aws_conn_id='aws_default' 
    )

    clean_customers_task = GlueJobOperator(
        task_id='cleaning_customers',
        job_name='cleaning_customers',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '100'
        },
        aws_conn_id='aws_default' 
    )

    clean_order_items_task = GlueJobOperator(
        task_id='cleaning_order_items',
        job_name='cleaning_order_items',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '100'
        },
        aws_conn_id='aws_default' 
    )

    clean_payments_task = GlueJobOperator(
        task_id='cleaning_payments',
        job_name='cleaning_payments',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '100'
        },
        aws_conn_id='aws_default' 
    )

    clean_products_task = GlueJobOperator(
        task_id='cleaning_products',
        job_name='cleaning_products',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '10000'
        },
        aws_conn_id='aws_default' 
    )

    clean_sellers_task = GlueJobOperator(
        task_id='cleaning_sellers',
        job_name='cleaning_sellers',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--RAW_DATABASE': 'brazilian_ecommerce_raw',
            '--PROCESSED_DATABASE': 'brazilian_ecommerce_clean',
            '--COUNT': '10000'
        },
        aws_conn_id='aws_default' 
    )

    business_transformation_task = GlueJobOperator(
        task_id='business_transformation',
        job_name='business_transformation',
        script_args={
            '--BUCKET': 's3://your-bucket',
            '--CLEAN_DATABASE': 'brazilian_ecommerce_clean',
            '--INCONSISTENCIES_DATABASE': 'brazilian_ecommerce_inconsistencies',
            '--COUNT': '10000'
        },
        aws_conn_id='aws_default' 
    )

    [clean_orders_task, clean_customers_task, clean_order_items_task, 
     clean_payments_task, clean_products_task, clean_sellers_task] >> business_transformation_task
