import pytest
from airflow_home.dags import dag_utils
import os


class Test_dag_utils:
    def test_file_was_downloaded():
        url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
        local_path_to_home = "test_folder"
        destination_file_path = "result.csv"
        final_destination = os.path.join(local_path_to_home, destination_file_path)
        dag_utils.download_file(
            url=url,
            local_path_to_home=local_path_to_home,
            destination_file_path=destination_file_path,
        )
        assert (
            os.path.exists(final_destination) and os.path.getsize(final_destination) > 0
        )
