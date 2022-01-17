
from datetime import datetime, timedelta

from airflow.models.dag import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    'task1',
    default_args=default_args,
    description='My first job',
    start_date=days_ago(0),
    schedule_interval=None,
    catchup=False,
    tags=['scraping'],
) as dag:
    t1 = PostgresOperator(
        task_id="select_table",
        postgres_conn_id="postgres_db",
        sql='SELECT * FROM "public"."product" LIMIT %(limit)d;',
        parameters={"limit": 1},
    )