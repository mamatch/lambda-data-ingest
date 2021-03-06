import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from dag_utils import download_file
from dag_utils import transform_csv_parquet
from dag_utils import upload_to_gcs

url = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-01.csv"
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
BUCKET = os.environ.get("BUCKET_ID")
csv_filename = ((url.split("/"))[-1]).replace('-', '_')
parquet_filename = csv_filename.replace(".csv", ".parquet")

path_to_local_home = os.environ.get('AIRFLOW_HOME', "/opt/airflow/")

default_args = {
    "owners": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="FHV_ingesion",
    description="A dag to ingest FHV data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,
    tags=['test']
) as dag:

    download_file_task = PythonOperator(
        task_id="doawnload_file_task",
        python_callable=download_file,
        op_kwargs={
            "url": url,
            "local_path_to_home": path_to_local_home,
            "destination_file_path": csv_filename
        }
    )

    transform_to_parquet_task = PythonOperator(
        task_id="transform_to_parquet_task",
        python_callable=transform_csv_parquet,
        op_kwargs={
            "input_file": "{}/{}".format(path_to_local_home, csv_filename)
        }
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    upload_to_gsc_task = PythonOperator(
        task_id="upload_to_gsc_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "object_name": parquet_filename,
            "filename": "{}/{}".format(
                path_to_local_home,
                parquet_filename
            )
        }
    )
    
    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": "FHV_trips",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "compression": "NONE",
                "sourceUris": [
                    "gs://{}/raw/{}".format(
                        BUCKET,
                        parquet_filename
                        )
                    ],
            },
        },
    )

    download_file_task >> transform_to_parquet_task >> upload_to_gsc_task >> create_external_table_task
