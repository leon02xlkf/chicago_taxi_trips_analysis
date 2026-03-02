from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

def check_silver_parquet_not_empty(bucket_name: str, prefix: str, aws_conn_id: str = "aws_default") -> str:
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []
    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise AirflowException(f"No parquet found under s3://{bucket_name}/{prefix}")
    parquet_key = sorted(parquet_keys)[0]
    obj = hook.get_conn().head_object(Bucket=bucket_name, Key=parquet_key)
    size = obj.get("ContentLength", 0)
    if size <= 0:
        raise AirflowException(f"Parquet is empty (size={size}) at s3://{bucket_name}/{parquet_key}")
    return parquet_key

with DAG(
    dag_id="taxi_wait_for_raw_csv",
    start_date=datetime(2025, 6, 1),
    schedule=None,
    catchup=False,
    params={
        "year": "2025",
        "month": "06",
    },
) as dag:

    wait_for_raw_csv = S3KeySensor(
        task_id="wait_for_raw_csv",
        bucket_name="chicago-taxi-trips-guanyiw",
        bucket_key=(
            "raw/year={{ params.year }}/"
            "month={{ params.month }}/"
            "taxi_{{ params.year }}_{{ params.month }}.csv"
        ),
        aws_conn_id="aws_default",
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 6,
    )

    run_glue_job = GlueJobOperator(
        task_id="run_glue_raw_to_silver",
        job_name="chicago-taxi-trips-etl",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--year": "{{ params.year }}",
            "--month": "{{ params.month }}",
        },
    )

    check_silver_parquet = PythonOperator(
        task_id="check_silver_parquet_not_empty",
        python_callable=check_silver_parquet_not_empty,
        op_kwargs={
            "bucket_name": "chicago-taxi-trips-guanyiw",
            "prefix": "silver/year={{ params.year }}/month={{ params.month }}/",
            "aws_conn_id": "aws_default",
        },
    )

    create_fact_table = PostgresOperator(
        task_id="create_fact_taxi_trips_table",
        postgres_conn_id="redshift_default",
        sql="""
        CREATE TABLE IF NOT EXISTS fact_taxi_trips (
        trip_id VARCHAR(200),
        taxi_id VARCHAR(200),
        trip_start_timestamp TIMESTAMP,
        trip_end_timestamp TIMESTAMP,
        trip_seconds BIGINT,
        trip_miles DOUBLE PRECISION,
        pickup_community_area INTEGER,
        dropoff_community_area INTEGER,
        fare DOUBLE PRECISION,
        tips DOUBLE PRECISION,
        tolls DOUBLE PRECISION,
        extras DOUBLE PRECISION,
        trip_total DOUBLE PRECISION,
        payment_type VARCHAR(50),
        company VARCHAR(200),
        pickup_centroid_latitude DOUBLE PRECISION,
        pickup_centroid_longitude DOUBLE PRECISION,
        dropoff_centroid_latitude DOUBLE PRECISION,
        dropoff_centroid_longitude DOUBLE PRECISION,
        pickup_hour INTEGER,
        pickup_dow INTEGER,
        );
        """,
    )

    copy_fact_table = PostgresOperator(
        task_id="copy_fact_taxi_trips",
        postgres_conn_id="redshift_default",
        sql="""
        COPY fact_taxi_trips
        FROM 's3://chicago-taxi-trips-guanyiw/silver/year={{ params.year }}/month={{ params.month }}/'
        IAM_ROLE 'arn:aws:iam::735702561273:role/service-role/AmazonRedshift-CommandsAccessRole-20260224T151011'
        FORMAT AS PARQUET;
        """,
    )
    
    wait_for_raw_csv >> run_glue_job >> check_silver_parquet >> create_fact_table >> copy_fact_table