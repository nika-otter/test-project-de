
import logging
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor   # ← ЗАМІНА СЕНСОРА
import duckdb

OWNER = "nika"
DAG_ID = "process_raw_to_s3_parquet"

RAW_LAYER = "raw"

PROCESSED_BUCKET = Variable.get("processed_bucket", default_var="prod")
PROCESSED_PREFIX = Variable.get("processed_prefix", default_var="processed")

ACCESS_KEY  = Variable.get("access_key")
SECRET_KEY  = Variable.get("secret_key")
S3_ENDPOINT = Variable.get("s3_endpoint", default_var="minio:9000")
S3_REGION   = Variable.get("s3_region", default_var="us-east-1")
_s3_use_ssl_var = Variable.get("s3_use_ssl", default_var="FALSE").strip().upper()
S3_USE_SSL = "TRUE" if _s3_use_ssl_var in ("TRUE", "1", "YES", "ON") else "FALSE"

LONG_DESCRIPTION = """
Process only: RAW JSON -> cleaned Parquet in S3.
- Waits for RAW JSON presence in S3
- Reads s3://{bucket}/raw/{YYYY-MM-DD}/response.json
- Keeps necessary fields, renames, cleans negatives, adds aqi_category
- Converts timestamps to Kyiv time (pretty string + TIMESTAMP + day)
- Writes Parquet to s3://{bucket}/processed/{YYYY-MM-DD}/data_{run_tag}.parquet
"""

SHORT_DESCRIPTION = "Clean raw JSON → Parquet to S3 (no DB)"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 5, tz="Europe/Kiev"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    return (
        context["data_interval_start"].format("YYYY-MM-DD"),
        context["data_interval_end"].format("YYYY-MM-DD"),
    )

def _configure_duckdb_s3(con: duckdb.DuckDBPyConnection):
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = '{S3_ENDPOINT}';
        SET s3_region = '{S3_REGION}';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = {S3_USE_SSL};
    """)


def wait_raw_json_callable(**context) -> bool:
    day, _ = get_dates(**context)
    src_json = f"s3://{PROCESSED_BUCKET}/{RAW_LAYER}/{day}/response.json"
    try:
        con = duckdb.connect()
        _configure_duckdb_s3(con)
        con.execute(f"SELECT 1 FROM read_json_auto('{src_json}') LIMIT 1;")
        con.close()
        return True
    except Exception as e:
        logging.info(f"RAW json not ready yet at {src_json}: {e}")
        try:
            con.close()
        except Exception:
            pass
        return False


def process_to_s3_parquet(**context):
    start_date, _ = get_dates(**context)
    logging.info(f"🧪 Processing date: {start_date}")

    run_tag = context["ts_nodash"]

    src_json    = f"s3://{PROCESSED_BUCKET}/{RAW_LAYER}/{start_date}/response.json"
    out_parquet = f"s3://{PROCESSED_BUCKET}/{PROCESSED_PREFIX}/{start_date}/data_{run_tag}.parquet"

    con = duckdb.connect()
    _configure_duckdb_s3(con)

    con.sql(f"""
        COPY (
            WITH src AS (
                SELECT * FROM read_json_auto('{src_json}')
            ),
            exploded AS (
                SELECT
                    -- Київський локальний час у гарному форматі (для звітів)
                    strftime((TO_TIMESTAMP(elt.dt) AT TIME ZONE 'Europe/Kiev'),
                             '%Y-%m-%d %H:%M:%S') AS ts_local_str,

                    -- Київський локальний час як TIMESTAMP (для подальших завантажень)
                    CAST(strptime(
                        strftime((TO_TIMESTAMP(elt.dt) AT TIME ZONE 'Europe/Kiev'),
                                 '%Y-%m-%d %H:%M:%S'),
                        '%Y-%m-%d %H:%M:%S'
                    ) AS TIMESTAMP) AS ts_local,

                    -- День (для ідемпотентності наступних пайплайнів)
                    strftime(DATE_TRUNC('day', (TO_TIMESTAMP(elt.dt) AT TIME ZONE 'Europe/Kiev')),
                             '%Y-%m-%d') AS day_local,

                    -- Координати
                    src.coord.lat AS lat,
                    src.coord.lon AS lon,

                    -- AQI + категорія (обмежуємо 1..5)
                    CASE WHEN elt.main.aqi BETWEEN 1 AND 5 THEN elt.main.aqi ELSE NULL END AS aqi,
                    CASE
                        WHEN elt.main.aqi = 1 THEN 'Good'
                        WHEN elt.main.aqi = 2 THEN 'Fair'
                        WHEN elt.main.aqi = 3 THEN 'Moderate'
                        WHEN elt.main.aqi = 4 THEN 'Poor'
                        WHEN elt.main.aqi = 5 THEN 'Very Poor'
                        ELSE NULL
                    END AS aqi_category,

                    -- Компоненти (негативні значення -> NULL) + читаємі назви з _ugm3
                    CASE WHEN elt.components.co    < 0 THEN NULL ELSE elt.components.co    END AS co_ugm3,
                    CASE WHEN elt.components.no    < 0 THEN NULL ELSE elt.components.no    END AS no_ugm3,
                    CASE WHEN elt.components.no2   < 0 THEN NULL ELSE elt.components.no2   END AS no2_ugm3,
                    CASE WHEN elt.components.o3    < 0 THEN NULL ELSE elt.components.o3    END AS o3_ugm3,
                    CASE WHEN elt.components.so2   < 0 THEN NULL ELSE elt.components.so2   END AS so2_ugm3,
                    CASE WHEN elt.components.pm2_5 < 0 THEN NULL ELSE elt.components.pm2_5 END AS pm25_ugm3,
                    CASE WHEN elt.components.pm10  < 0 THEN NULL ELSE elt.components.pm10  END AS pm10_ugm3,
                    CASE WHEN elt.components.nh3   < 0 THEN NULL ELSE elt.components.nh3   END AS nh3_ugm3
                FROM src,
                UNNEST(src.list) AS l(elt)
            )
            SELECT DISTINCT
                day_local,
                ts_local_str,
                ts_local,
                lat, lon,
                aqi, aqi_category,
                co_ugm3, no_ugm3, no2_ugm3, o3_ugm3, so2_ugm3, pm25_ugm3, pm10_ugm3, nh3_ugm3
            FROM exploded
        )
        TO '{out_parquet}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    con.close()
    logging.info(f"✅ Written cleaned parquet to S3: {out_parquet}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "processing", "parquet"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    wait_for_raw_json = PythonSensor(
        task_id="wait_for_raw_json",
        python_callable=wait_raw_json_callable,
        poke_interval=60,
        timeout=60*60*6,
        mode="reschedule",
    )

    process_task = PythonOperator(
        task_id="process_to_s3_parquet",
        python_callable=process_to_s3_parquet,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_raw_json >> process_task >> end
