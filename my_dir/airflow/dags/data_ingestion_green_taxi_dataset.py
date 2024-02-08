import os
import logging

from urllib import urlretrieve

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = "nyc_taxi_dataset"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#list from 01 to 12
months = ["%.2d" % i for i in range(1,13)]

def download_files_from_url():
    for i in months:
        filename = f"green_tripdata_2022-{i}.parquet"
        dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

        fullfilename = os.path.join(path_to_local_home, filename)
        urllib.urlretrieve(dataset_url, fullfilename)

def upload_to_gcs(bucket, local_file_path):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    for i in months:
        filename = f"green_tripdata_2022-{i}.parquet"
        objectname = f"raw/{filename}"
        blob = bucket.blob(objectname)
        local_file = os.path.join(path_to_local_home, filename)
        blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_green_taxi_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_files_from_url
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "local_file_path": path_to_local_home,
        },
    )


    download_dataset_task >> local_to_gcs_task 
