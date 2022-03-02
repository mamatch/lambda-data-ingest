import csv
import os

from airflow_home.dags import dag_utils


def get_len_of_file(filename: str):
    """
    Get the length of the file
    Args:
        filename: The name of the file

    Returns:
        result: The length of the file

    """
    input_file = open(filename, "r+")
    reader_file = csv.reader(input_file)
    result = len(list(reader_file))
    return result


def test_download_file():
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


def test_download_files():
    urls = [
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-{:0>2}.csv".format(
            month
        )
        for month in range(1, 3)
    ]
    """dag_utils.download_files(
        urls=urls,
        local_path_to_home="test_folder",
        destination_file_path=
    )"""


def test_size_result_merge_all_files():
    csv1 = "test_folder/csv1.csv"
    csv2 = "test_folder/csv2.csv"
    csv3 = "test_folder/csv3.csv"
    csv3_len = get_len_of_file(csv3)
    files = [csv1, csv2]

    final_destination = "test_folder/result_len_csv.csv"
    dag_utils.merge_all_files(files_path=files, local_path_to_home="test_folder",
                              destination_filename="result_len_csv.csv")
    assert csv3_len == get_len_of_file(final_destination)
