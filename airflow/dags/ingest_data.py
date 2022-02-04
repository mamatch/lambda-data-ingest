import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from dag_utils import download_file
from dag_utils import transform_csv_parquet
from dag_utils import upload_to_gcs


url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
project_id = os.environ.get("PROJECT_ID")
dataset_id = os.environ.get("DATASET_ID")
bucket = os.environ.get("BUCKET_ID")
file_downloaded = "raw_data.csv"
path_to_local_home = os.environ.get('AIRFLOW_HOME', "/opt/airflow/")


default_args = {
    "owners": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}


with DAG(
    dag_id="ingest",
    description="A dag to ingest data",
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
            "destination_file_path": file_downloaded
        }
    )

    transform_to_parquet_task = PythonOperator(
        task_id="transform_to_parquet_task",
        python_callable=transform_csv_parquet,
        op_kwargs={
            "input_file": "{}/{}".format(path_to_local_home, file_downloaded)
        }
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    # TODO: transfer to gcp
    upload_to_gsc_task = PythonOperator(
        task_id="upload_to_gsc_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": bucket,
            "object_name": file_downloaded.replace('.csv', '.parquet'),
            "filename": "{}/{}".format(
                path_to_local_home,
                file_downloaded.replace('.csv', '.parquet')
            )
        }
    )

    # TODO: create an external table in the bigquery dataset
    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": "my_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "compression": "NONE",
                "sourceUris": [
                    "gs://{}/raw/{}".format(
                        bucket,
                        file_downloaded.replace('.csv', '.parquet')
                        )
                    ],
            },
        },
    )

    download_file_task >> transform_to_parquet_task >> upload_to_gsc_task >> create_external_table_task
