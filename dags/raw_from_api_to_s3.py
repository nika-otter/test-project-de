import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "nika"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 5, tz="Europe/Kiev"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[pendulum.DateTime, pendulum.DateTime]:
    start_date = context["data_interval_start"]
    end_date = context["data_interval_end"]
    return start_date, end_date


def make_kyiv_day_url(lat: float, lon: float, date: pendulum.DateTime) -> str:
    tz_offset = pendulum.duration(hours=3)
    start_ts = (date.start_of("day") - tz_offset).int_timestamp
    end_ts = (date.end_of("day") - tz_offset).int_timestamp
    return (
        f'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}'
        f'&start={start_ts}&end={end_ts}&appid=a50d8617c4433fc2fa49638d75994537'
    )


def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    url = make_kyiv_day_url(50.45, 30.52, start_date, )
    safe_date = start_date.format("YYYY-MM-DD")
    con = duckdb.connect()
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_region = 'us-east-1';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY (
            SELECT *
            FROM read_json_auto('{url}')
        )
        TO 's3://prod/{LAYER}/{safe_date}/response.json';
    """)
    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 5 * * *",
        default_args=args,
        tags=["s3", "raw"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    get_and_transfer_api_data_to_s3_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(task_id="end")

    start >> get_and_transfer_api_data_to_s3_task >> end

