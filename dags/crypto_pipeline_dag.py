from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import pandas as pd

# En el servidor central, dags/ es la raíz del PYTHONPATH de Airflow.
# La estructura es: dags/shared_etl/ y dags/crypto-etl-pipeline/
# Así que tanto shared_etl como etl son importables directamente.
sys.path.insert(0, "/opt/airflow/dags/crypto-etl-pipeline")

from etl.extract import fetch_crypto_data
from etl.transform import transform_data
from etl.load import load_data


def extract(**context):
    """
    Fetches raw crypto data from the CoinGecko API and pushes it to XCom.
    """
    raw_data = fetch_crypto_data()
    if not raw_data:
        raise ValueError("No data returned from CoinGecko API.")
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    print(f"Extracted {len(raw_data)} records.")


def transform(**context):
    """
    Pulls raw data from XCom, cleans it with pandas, pushes result back.
    DataFrame is serialized to dict because XCom doesn't support DataFrames natively.
    """
    ti = context["ti"]
    raw = ti.xcom_pull(key="raw_data", task_ids="extract_task")

    if not raw:
        raise ValueError("No data received from extract_task.")

    df = transform_data(raw)
    ti.xcom_push(key="clean_data", value=df.to_dict(orient="records"))
    print(f"Transformed {len(df)} records.")


def load(**context):
    """
    Pulls cleaned data from XCom and persists it into postgres-central
    under the 'crypto' schema.
    """
    ti = context["ti"]
    clean_data_dict = ti.xcom_pull(key="clean_data", task_ids="transform_task")

    if not clean_data_dict:
        raise ValueError("No data received from transform_task.")

    df = pd.DataFrame(clean_data_dict)
    load_data(df)
    print(f"Loaded {len(df)} records into crypto.crypto_prices.")


default_args = {
    "owner": "sabnei",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_etl_pipeline",
    description="ETL pipeline: CoinGecko API → crypto.crypto_prices en postgres-central",
    schedule="@hourly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
