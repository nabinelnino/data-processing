
from source_connector import SourceConnector
from pyspark.sql.utils import AnalysisException
from loguru import logger
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType
from pyspark.sql.functions import col, when
from pyspark.sql.functions import lit

from google.cloud import storage

import os

from google.cloud import bigquery

from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

PATH = os.path.join(os.getcwd(), 'my-sile.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH


log_file_path = os.getenv("LOG_FILE_PATH", "./logs/pipeline_error.log")
processing_log_file_path = os.getenv(
    "PROCESSING_LOG_FILE_PATH", "./logs/processing.log")


ERROR_FORMAT = "{time} | {name} | {level} | {message}"
INFO_FORMAT = "{time} | {name} | {level} | {message}"

# Add a file handler for logging errors
logger.add(log_file_path, level="ERROR", format=ERROR_FORMAT)

# Add another file handler for logging processing information
logger.add(processing_log_file_path, level="INFO", format=INFO_FORMAT)


class GetLoadData(SourceConnector):

    def __init__(self, config_dict) -> None:
        super().__init__(config_dict)
        self.spark = self.initialize_spark()
        # self.download_file(file_name)

    def initialize_spark(self):
        # Create a Spark session
        spark = SparkSession.builder \
            .appName("example") \
            .config("spark.executor.memory", self.prop("executor_memory", 4)) \
            .config("spark.executor.memoryOverhead", self.prop("executor_memoryOverhea", 2)) \
            .config("spark.driver.memory", self.prop("driver_memory", 4)) \
            .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
            .getOrCreate()
        logger.info(
            "Spark Session created: {info}", info="Spark Session created")

        # Get the Spark configuration
        conf = spark.sparkContext.getConf()

        # Retrieve the default executor memory
        default_executor_memory = conf.get(
            "spark.executor.memory", "Not specified")
        print(f"Default Executor Memory: {default_executor_memory}")

        # Retrieve the default driver memory
        default_driver_memory = conf.get(
            "spark.driver.memory", "Not specified")
        print(f"Default Driver Memory: {default_driver_memory}")
        return spark

        # Stop the Spark session

    def download_file(self, filename) -> None:
        schema = StructType([
            StructField("ID", StringType(), False),
            StructField("Library_ID", StringType()),
            StructField("Sub_ID_1", StringType()),
            StructField("Sub_ID_2", StringType()),
            StructField("Sub_ID_3", StringType()),
            StructField("MW", FloatType()),
            StructField("LogP", FloatType()),
            StructField("FP1", StringType()),
            StructField("FP2", StringType()),
            StructField("FP3", StringType()),
            StructField("FP4", StringType()),
            StructField("FP5", StringType()),


        ])

        try:

            output_dir = self.prop("output_dir")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            df_zipped = self.spark.read.format("csv").option(
                "delimiter", "\t").option("header", True).load(filename)

            # Check for missing columns and add them with None values
            columns_to_add = (
                set(schema.fieldNames()) - set(df_zipped.columns))

            if columns_to_add:
                for column in columns_to_add:
                    df_zipped = df_zipped.withColumn(
                        column, lit(None).cast(schema[column].dataType))

            df_repartitioned = df_zipped.repartition(10)

            for partition_id, partition_df in enumerate(df_repartitioned.rdd.glom().toLocalIterator(), 1):

                # Convert the list of Rows to a DataFrame
                partition_df = self.spark.createDataFrame(
                    partition_df, schema=schema)

                new_folder = f"test_{partition_id}"

                # Write the partition DataFrame to a Parquet file
                parquet_file_path = os.path.join(
                    output_dir, new_folder, f"partition_{partition_id}.parquet")
                partition_df.write.parquet(parquet_file_path)
                # self.another_function(parquet_file_path)
                self.insert_data_intobq(parquet_file_path)

        except AnalysisException as e:
            logger.error(f"An error occurred: {e}")

        finally:
            self.spark.stop()

        return output_dir

    # function to show the sample records

    # def another_function(self, parquet_file_path):
    #     """
    #     Another function to process the Parquet file.

    #     Parameters:
    #         - parquet_file_path: Path to the Parquet file
    #     """
    #     # Add your logic to process the Parquet file in another function
    #     print(f"Processing Parquet file: {parquet_file_path}")
    #     df_parquet = self.spark.read.parquet(parquet_file_path)
    #     print(df_parquet.select("ID", "Library_ID", "Sub_ID_3").show(4))

    def insert_data_intobq(self, parquet_file_path):
        storage_client = storage.Client(PATH)

        key_path = self.prop("key_path")
        project_id = self.prop("project_id")
        dataset_id = self.prop("dataset_id")
        table = self.prop("bq_table_name")
        # table = "wikidata"
        table_id = "{}.{}.{}".format(project_id, dataset_id, table)
        print("********* NAME OF TABLE IS", table_id)

        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials,
                                 project=project_id)
        table_schema = [
            bigquery.SchemaField("ID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Library_ID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Sub_ID_1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Sub_ID_2", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Sub_ID_3", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("MW", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("LogP", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("FP1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("FP2", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("FP3", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("FP4", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("FP5", "STRING", mode="NULLABLE"),

        ]
        clustering_fields = 'ID'
        partitioning_field = 'Library_ID'

        table = bigquery.Table(table_id, schema=table_schema)
        try:
            existing_table = client.get_table(table)
            print(f'Table {table_id, existing_table.schema} already exists.')

        except NotFound:
            table = bigquery.Table(table_id, schema=table_schema)
            table.library_partitioning = partitioning_field
            table.clustering_fields = clustering_fields
            table = client.create_table(table)
            print(
                "Created clustered table {}.{}.{}".format(
                    table.project, table.dataset_id, table.table_id
                )
            )

        for root, dirs, files in os.walk(parquet_file_path):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_file_path = os.path.join(root, file)

                    # Load the Parquet partition directly into the table
                    job_config = bigquery.LoadJobConfig()
                    job_config.source_format = bigquery.SourceFormat.PARQUET
                    try:

                        with open(parquet_file_path, 'rb') as source_file:
                            job = client.load_table_from_file(
                                source_file, table, job_config=job_config)

                        # Wait for the job to complete
                        job.result()

                        # Print the number of rows inserted
                        print(
                            f'Loaded {job.output_rows} rows from {parquet_file_path} into {table_id}.')
                    except Exception as e:
                        logger.error(f"An error occurred: {e}")

    def query_data_by_id(self):
        project_id = self.prop("project_id")
        dataset_id = self.prop("dataset_id")
        table = self.prop("bq_table_name")
        target_id = self.prop("target_id")
        table_id = "{}.{}.{}".format(project_id, dataset_id, table)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        client = bigquery.Client()

        query = f"""
            SELECT *
            FROM `{table_ref}`
            WHERE ID = '{target_id}'
        """

        try:
            query_job = client.query(query)

            # Wait for the query to complete
            results = query_job.result()

            # Iterate over the results and print or process them as needed
            for row in results:
                print(row)

            # Return the results (you can modify this based on your use case)
            return results

        except Exception as e:
            logger.error(f"An error occurred: {e}")


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
    connector = GetLoadData(config_dict)
    connector.query_data_by_id()
