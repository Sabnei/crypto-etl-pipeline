from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Adding project path to sys.path
sys.path.insert(0, "/opt/airflow")

from etl.extract import fetch_crypto_data
from etl.transform import transform_data
from etl.load import load_data

# --- Task Functions ---
def extract(**context):
    """
    Fetches raw crypto data from the API and pushes it to XCom
    """
    raw_data = fetch_crypto_data()

    # Push raw data to XCom for the next task
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    print("Successfully pushed raw data to XCom.")

def transform(**context):
    """
    Retrives raw data from XCom, transgorms it using pandas,
    and pushes the cleaned dictionary bact to XCom.
    """
    ti = context["ti"]
    # Pull raw data from the "extract_task"
    raw = ti.xcom_pull(key="raw_data", task_ids="extract_task")

    if not raw:
        raise ValueError("No data received from extract_task")
    
    # Transform the data
    df = transform_data(raw)

    # XCom does not support native DataFrame easily, so we convert to dict
    ti.xcom_push(key="clean_data", value=df.to_dict(orient="records"))
    print(f"Successfully transformed {len(df)} records and pushed to XCom.")

def load(**context):
    """
    Retrieves cleaned data from XCom and persists it into the database.
    """
    ti = context["ti"]
    # Pull cleaned data from the "transform_task"
    clean_data_dict = ti.xcom_pull(key="clean_data", task_ids="transform_task")

    if not clean_data_dict:
        raise ValueError("No data received from transform_task")
    
    # Reconstruct DataFrame
    df = pd.DataFrame(clean_data_dict)

    # Load data into PostegreSQL
    load_data(df)
    print("Successfully loaded data into the database.")

# ── Definición del DAG ────────────────────────────

default_args = {
    "owner": "sabnei",
    "retries": 2,                           
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_etl_pipeline",            
    description="ETL de precios crypto cada hora",
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

    # Define el orden de ejecución
    extract_task >> transform_task >> load_task