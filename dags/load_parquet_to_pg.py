
import logging
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import duckdb

OWNER = "nika"
DAG_ID = "load_parquet_to_pg"

S3_BUCKET = Variable.get("processed_bucket", default_var="prod")
S3_PREFIX = Variable.get("processed_prefix", default_var="processed")

ACCESS_KEY  = Variable.get("access_key")
SECRET_KEY  = Variable.get("secret_key")
S3_ENDPOINT = Variable.get("s3_endpoint", default_var="minio:9000")
S3_REGION   = Variable.get("s3_region", default_var="us-east-1")
_s3_use_ssl_var = Variable.get("s3_use_ssl", default_var="FALSE").strip().upper()
S3_USE_SSL = "TRUE" if _s3_use_ssl_var in ("TRUE", "1", "YES", "ON") else "FALSE"

PG_SCHEMA = "ods"
PG_TABLE  = "air_quality"
PG_HOST = Variable.get("pg_host", default_var="postgres_dwh")
PG_PORT = int(Variable.get("pg_port", default_var="5432"))
PG_DB   = Variable.get("pg_db",   default_var="postgres")
PG_USER = Variable.get("pg_user", default_var="postgres")
PG_PASS = Variable.get("pg_password")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 5, tz="Europe/Kiev"),
    "catchup": True,
    "retries": 0,
}

def _cfg_s3(con: duckdb.DuckDBPyConnection):
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs; LOAD httpfs;
        SET s3_url_style='path';
        SET s3_endpoint='{S3_ENDPOINT}';
        SET s3_region='{S3_REGION}';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl={S3_USE_SSL};
    """)

def _cfg_pg(con: duckdb.DuckDBPyConnection):
    con.sql(f"""
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST '{PG_HOST}',
            PORT {PG_PORT},
            DATABASE '{PG_DB}',
            USER '{PG_USER}',
            PASSWORD '{PG_PASS}'
        );
        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
    """)

# ------------ Sensor: чекаємо parquet у S3 ------------
def wait_parquet_callable(**context) -> bool:
    day = context["data_interval_start"].format("YYYY-MM-DD")
    src_glob = f"s3://{S3_BUCKET}/{S3_PREFIX}/{day}/data_*.parquet"
    try:
        con = duckdb.connect()
        _cfg_s3(con)
        con.execute(f"SELECT 1 FROM read_parquet('{src_glob}', union_by_name=true) LIMIT 1;")
        con.close()
        logging.info(f"✅ Parquet is available: {src_glob}")
        return True
    except Exception as e:
        logging.info(f"⏳ Parquet not ready yet at {src_glob}: {e}")
        try:
            con.close()
        except Exception:
            pass
        return False

def load_simple(**context):
    day = context["data_interval_start"].format("YYYY-MM-DD")
    src = f"s3://{S3_BUCKET}/{S3_PREFIX}/{day}/data_*.parquet"
    logging.info(f"Loading day={day} from {src}")

    con = duckdb.connect()
    _cfg_s3(con)
    _cfg_pg(con)

    con.sql(f"CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{PG_SCHEMA};")
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS dwh_postgres_db.{PG_SCHEMA}.{PG_TABLE} (
            day_local    VARCHAR(10),
            ts_local_str VARCHAR(19),
            ts_local     TIMESTAMP,
            lat          DOUBLE,
            lon          DOUBLE,
            aqi          INTEGER,
            aqi_category VARCHAR(16),
            co_ugm3      DOUBLE,
            no_ugm3      DOUBLE,
            no2_ugm3     DOUBLE,
            o3_ugm3      DOUBLE,
            so2_ugm3     DOUBLE,
            pm25_ugm3    DOUBLE,
            pm10_ugm3    DOUBLE,
            nh3_ugm3     DOUBLE,
            CONSTRAINT pk_air_quality PRIMARY KEY (ts_local, lat, lon)
        );
    """)

    con.sql(f"DELETE FROM dwh_postgres_db.{PG_SCHEMA}.{PG_TABLE} WHERE day_local = '{day}';")

    con.sql(f"""
        WITH stg AS (
            SELECT *
            FROM read_parquet('{src}', union_by_name=true) 
        ),
        norm AS (
            SELECT
                COALESCE(
                    TRY_CAST(ts_local AS TIMESTAMP),
                    TRY_CAST(strptime(ts_local_str, '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
                ) AS ts_local,

                COALESCE(
                    ts_local_str,
                    CASE
                        WHEN TRY_CAST(ts_local AS TIMESTAMP) IS NOT NULL
                        THEN strftime(TRY_CAST(ts_local AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
                        ELSE NULL
                    END
                ) AS ts_local_str,
                COALESCE(
                    day_local,
                    CASE
                        WHEN COALESCE(
                            TRY_CAST(ts_local AS TIMESTAMP),
                            TRY_CAST(strptime(ts_local_str, '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
                        ) IS NOT NULL
                        THEN strftime(
                            DATE_TRUNC('day', COALESCE(
                                TRY_CAST(ts_local AS TIMESTAMP),
                                TRY_CAST(strptime(ts_local_str, '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
                            )),
                            '%Y-%m-%d'
                        )
                        ELSE NULL
                    END
                ) AS day_local,

                TRY_CAST(lat AS DOUBLE) AS lat,
                TRY_CAST(lon AS DOUBLE) AS lon,
                
                TRY_CAST(aqi AS INTEGER) AS aqi,
                aqi_category,

                TRY_CAST(co_ugm3  AS DOUBLE) AS co_ugm3,
                TRY_CAST(no_ugm3  AS DOUBLE) AS no_ugm3,
                TRY_CAST(no2_ugm3 AS DOUBLE) AS no2_ugm3,
                TRY_CAST(o3_ugm3  AS DOUBLE) AS o3_ugm3,
                TRY_CAST(so2_ugm3 AS DOUBLE) AS so2_ugm3,
                TRY_CAST(pm25_ugm3 AS DOUBLE) AS pm25_ugm3,
                TRY_CAST(pm10_ugm3 AS DOUBLE) AS pm10_ugm3,
                TRY_CAST(nh3_ugm3  AS DOUBLE) AS nh3_ugm3
            FROM stg
        ),
        dedup AS (
            SELECT
                day_local, ts_local_str, ts_local,
                lat, lon, aqi, aqi_category,
                co_ugm3, no_ugm3, no2_ugm3, o3_ugm3, so2_ugm3, pm25_ugm3, pm10_ugm3, nh3_ugm3,
                ROW_NUMBER() OVER (
                    PARTITION BY ts_local, lat, lon
                    ORDER BY ts_local NULLS LAST
                ) AS rn
            FROM norm
        )
        INSERT INTO dwh_postgres_db.{PG_SCHEMA}.{PG_TABLE}
        SELECT
            day_local,
            ts_local_str,
            ts_local,
            lat, lon,
            aqi, aqi_category,
            co_ugm3, no_ugm3, no2_ugm3, o3_ugm3, so2_ugm3, pm25_ugm3, pm10_ugm3, nh3_ugm3
        FROM dedup
        WHERE rn = 1;
    """)

    con.close()
    logging.info(f"✅ Done: {PG_SCHEMA}.{PG_TABLE} <- {src}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3","pg","parquet","simple"],
    description="S3 Parquet → Postgres (schema-tolerant, with file sensor)",
    max_active_runs=1,
) as dag:
    wait_for_parquet = PythonSensor(
        task_id="wait_for_parquet",
        python_callable=wait_parquet_callable,
        poke_interval=60,   # чек раз/хв
        timeout=60*60*6,    # до 6 год
        mode="reschedule",
    )

    load = PythonOperator(
        task_id="load_simple",
        python_callable=load_simple,
    )

    wait_for_parquet >> load
