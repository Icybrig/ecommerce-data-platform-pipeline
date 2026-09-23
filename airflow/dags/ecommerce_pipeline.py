from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /opt/project/src/bronze/ingest.py",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/project/src/silver/transform.py",
    )

    cdc_merge = BashOperator(
        task_id="cdc_merge",
        bash_command="python /opt/project/src/silver/merge_orders.py",
    )

    daily_sales = BashOperator(
        task_id="daily_sales",
        bash_command="python /opt/project/src/gold/daily_sales.py",
    )

    customer_sales = BashOperator(
        task_id="customer_sales",
        bash_command="python /opt/project/src/gold/customer_sales.py",
    )

    product_sales = BashOperator(
        task_id="product_sales",
        bash_command="python /opt/project/src/gold/product_sales.py",
    )

    bronze_ingestion >> silver_transform >> cdc_merge

    cdc_merge >> daily_sales
    cdc_merge >> customer_sales
    cdc_merge >> product_sales