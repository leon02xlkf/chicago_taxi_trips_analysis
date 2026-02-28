from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="redshift_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_conn = PostgresOperator(
        task_id="test_redshift_connection",
        postgres_conn_id="redshift_default",
        sql="SELECT current_user, current_database, current_timestamp;",
    )