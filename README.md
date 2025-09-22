# Data Engineering Project 

This project uses a pre-built **docker-compose.yaml** for Apache Airflow with a few additional services required for the pipeline.

## Core services
- **Airflow (webserver, scheduler, worker, triggerer)** – DAG orchestration  
- **Postgres (Airflow metadata)** – stores Airflow state  
- **Redis** – message broker for CeleryExecutor  

## Added services
- **Postgres DWH** – separate warehouse database for analytics  
- **pgAdmin** – web interface for managing the DWH  
- **MinIO** – local S3-compatible object storage  

## Why added
- **Postgres DWH + pgAdmin** – to have a dedicated data warehouse and easy administration  
- **MinIO** – to simulate cloud-like S3 storage for data  

---

## DAG: `raw_from_api_to_s3`

- Fetches raw air quality data from **OpenWeatherMap API**  
- Saves JSON files into MinIO under:  
  `s3://prod/raw/{date}/response.json`  
- Adjusts timestamps to **Europe/Kiev** timezone (API requires Unix time)  

---

## DAG: `process_raw_to_s3_parquet`

- Waits for raw JSON in S3 using a **PythonSensor**  
- Cleans and transforms: removes negatives, adds AQI categories, formats timestamps in **Europe/Kiev**  
- Stores processed data as **Parquet with ZSTD compression** under:  
  `s3://prod/processed/{date}/data_{run_tag}.parquet`  

---

## DAG: `load_parquet_to_pg`

- Waits for Parquet in S3:  
  `s3://prod/processed/{date}/data_*.parquet`  
- Loads data into **Postgres (ODS schema)** via DuckDB  
- Creates schema/table if not present; primary key: `(ts_local, lat, lon)`  
- Deletes existing rows for the day, inserts new data with **deduplication** (`ROW_NUMBER`)  
- Schema-tolerant (uses `union_by_name=true`, `TRY_CAST`)  

**Schedule:** `0 5 * * *`  
**Retries:** `0` (disabled)  


## Requirements & Setup

To run this project you need:

- Install Python dependencies:  
  ```bash
  pip install apache-airflow==2.10.5 duckdb==1.2.2
  ```
- **An API key** for OpenWeatherMap  
- **Credentials from MinIO**: access key, secret key  
- **Credentials from Postgres DWH**: host, port, user, password  
- Upload all these secrets and the API key into **Airflow Variables** (Web UI → Admin → Variables)  

