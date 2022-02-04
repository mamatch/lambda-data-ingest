import logging
import os
from pyarrow import csv as pv
from pyarrow import parquet as pq
from google.cloud import storage


def download_file(url, local_path_to_home, destination_file_path):
    """A function that download from an url

    Args:
        url (string): Th url
        local_path_to_home (string): The path to the local home
        destination_file_path (string): path to the destination
    """
    final_dest_path = os.path.join(local_path_to_home, destination_file_path)
    # Check if the file exists
    if not os.path.exists(final_dest_path) or \
        (os.path.exists(final_dest_path) and
            os.path.getsize(final_dest_path) == 0):
        os.system("curl -sSf {} > {}".format(url, final_dest_path))
    else:
        logging.info("File already exists.")


def transform_csv_parquet(input_file: str):
    """A function to transform a csv file ot parquet

    Args:
        input_file (str): Th einput file
    """
    if not input_file.endswith(".csv"):
        logging.error("Can't handle none csv file.")
        return
    data = pv.read_csv(input_file)
    pq.write_table(data, input_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket_name, object_name, filename):
    """A function to upload a file to a google cloud storage

    Args:
        bucket_name (str): The name of the bucket
        object_name (str): the name the object that will be written
        filename (str): The name of the file to be uploaded
    """
    try:
        bucket_client = storage.Client()
        bucket = bucket_client.bucket(bucket_name)
        blob = bucket.blob("raw/{}".format(object_name))
        blob.upload_from_filename(filename)
    except Exception as e:
        print(e)
