"""
Iterates over files downloaded from a google cloud bucket based on a
given config.
"""
import os
import re
from pathlib import Path
import glob
from loguru import logger
import shutil
from google.cloud import storage


import ijson
import yaml

from source_connector import SourceConnector

from data_process_ingest import GetLoadData
log_file_path = os.getenv("LOG_FILE_PATH", "./logs/pipeline_error.log")
processing_log_file_path = os.getenv(
    "PROCESSING_LOG_FILE_PATH", "./logs/processing.log")


ERROR_FORMAT = "{time} | {name} | {level} | {message}"
INFO_FORMAT = "{time} | {name} | {level} | {message}"

# Add a file handler for logging errors
logger.add(log_file_path, level="ERROR", format=ERROR_FORMAT)

# Add another file handler for logging processing information
logger.add(processing_log_file_path, level="INFO", format=INFO_FORMAT)


class GCPConnector(SourceConnector):
    """
    An iterator designed to find and download files from a gcp bucket
    """

    def __init__(self, config_dict: dict):
        super().__init__(config_dict)
        self.config_dict = config_dict

        self.persistent_data: dict = {}

        self.download_file_paths: list = []
        self.total_files_downloaded = 0
        self.total_files_processed = 0
        self.current_file = None
        self.current_index = 0
        self.total_object_count = 0

        PATH = os.path.join(os.getcwd(), self.prop("key_path"))
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

        self.storage_client = storage.Client(PATH)

        bucket_name = self.prop("bucket_name")

        self.create_persistent_data()
        self.find_gcp_files()

    def create_blob_name_storage(self) -> None:
        gcp_file_location = self.prop("gcp_file_location")

        file_path = Path(gcp_file_location)
        os.makedirs(file_path.parent, exist_ok=True)
        file_path.touch(exist_ok=True)

        with open(gcp_file_location, "r", encoding="utf8") as file:
            self.persistent_data = yaml.safe_load(file)
            if self.persistent_data is None:
                self.persistent_data = {}

    def create_persistent_data(self) -> None:
        """
        Create the persistent data structure for the connector to use
        """

        persistence_file = self.prop("persistence_file_path")

        file_path = Path(persistence_file)
        os.makedirs(file_path.parent, exist_ok=True)
        file_path.touch(exist_ok=True)

        with open(persistence_file, "r", encoding="utf8") as file:
            self.persistent_data = yaml.safe_load(file)
            if self.persistent_data is None:
                self.persistent_data = {}

    def update_persistent_data(self):
        """
        Saves the current contents of the persistent_data
        dictionary to the specified file in the config
        """
        file_path = self.prop("persistence_file_path")
        with open(file_path, "w", encoding="utf8") as file:
            yaml.safe_dump(self.persistent_data, file)

    def find_gcp_files(self) -> None:
        """
        Searches the GCP bucket/ Local file for any files that match the search parameters
        """
        search_folder = self.prop("search_folder")
        blob_list = glob.glob(os.path.join(search_folder, '*.gz'))
        if search_folder.lower() == "gcp":
            blob_list = self.get_file_from_bucket()

        file_extension = self.prop("file_extension")
        download_file_list = self.prop("download_file_list", optional=True)

        logger.info(
            "Files are extracted from Bucket: {info}", info="Files are extracted from Bucket")

        if download_file_list is not None:
            for file_name in download_file_list:
                self.download_file_paths.append(file_name)

        self.find_all_files(blob_list)

        self.total_files_downloaded = len(self.download_file_paths)

    def find_all_files(self, blob_list):
        """
        Adds all files from a list of blobs.
        :param blob_list: the list of blobs to be searched
        :param file_extension: the file extension to be collected
        """

        for blob in blob_list:

            gcp_path = blob
            self.download_file_paths.append(gcp_path)

    def get_file_from_bucket(self) -> str:
        """
        Download a bucket path from the gcp
        :param file_name: The name of the file to be downloaded.
        """

        pass

        output_dir = self.prop("output_dir")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        bucket_name = self.prop("bucket_name")

        client = storage.Client()

        # Get the bucket and list all blobs
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        gcs_url = f"gs://{bucket_name}"
        dfs = []
        for blob in blobs:
            # Extract file name from blob name
            file_name = blob.name.split('/')[-1]

            # Skip non-TSV files
            if not file_name.endswith('.gz'):
                continue

            file_url = f"{gcs_url}/{file_name}"
            dfs.append(file_url)

        return dfs

    def is_file_duplicate(self, file_name: str) -> bool:
        """
        Detect wheather a file has been downloaded and ingested before
        :param file_name: the name of the file to be checked
        """
        result = False

        if "download_file_list" in self.persistent_data and self.persistent_data.get(
                "download_file_list", None):
            print("TYPE of PERSISTENT", type(self.persistent_data))
            result = file_name in self.persistent_data.get(
                "download_file_list")
            print("TYPE of PERSISTENTsdfsdfsf", result)
        else:
            self.persistent_data["download_file_list"] = []

        return result

    def delete_file(self, file_path: str) -> None:
        """
        Delete a file from the system
        :param file_name: The file to be deleted.
        """

        try:
            shutil.rmtree(file_path)
            print(
                f'The folder {file_path} and its contents have been successfully removed.')
        except OSError as e:
            print(f'Error: {e}')

    def update_persistent_data(self):
        """
        Saves the current contents of the persistent_data
        dictionary to the specified file in the config
        """
        file_path = self.prop("persistence_file_path")
        with open(file_path, "w", encoding="utf8") as file:
            yaml.safe_dump(self.persistent_data, file)

    def item_generator(self):
        """
        A generator function for returning the items found by the connector one at a time
        """
        ignore_duplicates = self.prop("ignore_duplicates")

        for gcp_path in self.download_file_paths:

            if self.is_file_duplicate(gcp_path) and ignore_duplicates:

                continue

            # local_file_name = self.download_file(gcp_path)
            local_file_name = gcp_path

            try:
                file_path = GetLoadData(
                    self.config_dict)
                folder_name = file_path.download_file(local_file_name)

            except ijson.IncompleteJSONError:
                print("Malformed File Found: Skipping")
                self.update_and_clean(local_file_name, folder_name)
                continue

            self.update_and_clean(local_file_name, folder_name)

    def update_and_clean(self, file_path_consumed: str, local_file_path: str):
        """
        Updates persistent data and deletes the consumed file if required
        :param file_path_consumed: the file consumed from the aws bucket
        :param local_file_path: the local file path where that file can be found
        """
        delete_consumed_files = self.prop(
            "delete_consumed_files", optional=True)
        self.persistent_data["download_file_list"].append(file_path_consumed)
        self.update_persistent_data()

        if delete_consumed_files:
            self.delete_file(local_file_path)


def load_yaml(yaml_file_path: str) -> dict:
    """
    Loads the contents of the yaml file into a dictionary for use by a job
    :param yaml_file_path: a path to a yaml file with configuration information
    """
    data = None

    try:
        with open(yaml_file_path, "r", encoding="utf8") as file:
            data = yaml.safe_load(file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"No config file found at {yaml_file_path}") from exc

    if data is None:
        raise ValueError("Config File Contains Nothing or Does Not Exist")
    return data


if __name__ == "__main__":

    CONFIG_FILE_PATH = "./configs/gcp_config.yaml"
    config_dict = load_yaml(CONFIG_FILE_PATH)
    connector = GCPConnector(config_dict)
    generator = connector.item_generator()
