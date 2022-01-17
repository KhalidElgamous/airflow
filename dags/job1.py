
from datetime import datetime, timedelta

from airflow.models.dag import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
}

with DAG(
    'task1',
    default_args=default_args,
    description='My first job',
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