## This project has mainly 2 components

- Create Sample data
- Process and load data into Bigquery table

# Prerequisites

- Python 3.9+
- Google Cloud Platform (GCP) account and bucket
- since the implementation is done using pyspark framework, you need to have the Java Runtime Environment (JRE) installed on your machine. PySpark relies on Java for its underlying processing engine, and the presence of JRE ensures the necessary Java dependencies are available for PySpark to function properly

#### Installation

- Clone the repository: - git clone git@github.com:nabinelnino/data-processing.git
- cd data-processing
- create venv
- Install dependencies: pip install -r requirements.txt

1. Create_data.py

- The `Create_data.py` file has class `CreateSampleData` script generates sample data and stores it in compressed tab-separated values (tsv) files. It utilizes the pandas library to create DataFrames and writes the data to gzip-compressed tsv files.
- You can set the name of compressed tsv file, rows size and store location in info_config.yaml file
- to run: cd to src and run python create_data.py

2. Process Module (final.py)
- python final.py

## Overview

- The GCPConnector class is part of a Python codebase designed to iterate over files downloaded from a Google Cloud Platform (GCP) bucket or local storage based on a given configuration. - The codebase includes functionality to interact with GCP storage, download files, manage persistent data, and handle data processing.

## Configuration

- The script reads configuration from a YAML file (gcp_config.yaml).
- Edit the configuration file to specify GCP-related, (local file), system configuration to run pyspark, persistent storage file location parameters,the GCP bucket name, search folder, and other options.

## Functionality

GCPConnector Class

### Initialization

The class initializes by reading configuration parameters from gcp_config.yaml and setting up the GCP client (search_folder: "GCP").

### File Retrieval

- The script searches the GCP bucket for files based on the specified search folder and file extension.
- Also, script searches files in local system based on paramter specified in gcp_config.yaml (search_folder: "local folder path")
- Files can be filtered based on different criteria, such as downloading specific files or excluding duplicates.

### Data Processing

- The class iterates over the retrieved files, downloads them, and processes the data using the DownloadFrmGCPBucket class.
  Processed files are marked in the persistent data structure, and consumed files can be optionally deleted.

- In case of a code interruption after processing some files from either the local file system or a Google Cloud Platform (GCP) bucket, the script persists the last processed file's name in a designated file. This ensures that, upon the next execution, the script resumes processing from the remaining files rather than starting over.

- Additionally, the script processes one file at a time, converting it into a Parquet file. The Parquet file is then loaded into GCP, and partitioning is applied based on the specified partition size in the configuration file. This partitioning strategy, implemented using the PySpark library, enhances efficiency and accelerates the overall processing speed.

- To monitor the status and configuration details of PySpark, you can access the following link: http://localhost:4040/

### Load data into Bigquery

GetLoadData class has function called insert_data_intobq

- This function is responsible for setting up and configuring the connection to Google Cloud Storage (GCS) and BigQuery. It then processes a Parquet file by loading its contents into a specified BigQuery table.

#### Here's a breakdown of the key steps in the function:

- Configure Google Cloud Storage and BigQuery Connection:
- Create a GCS client using the provided JSON key file path (PATH)

- Obtain configuration parameters such as key_path, project_id, dataset_id, and bq_table_name from the properties (self.prop).
- Create a BigQuery client using the obtained credentials and project ID.

#### Define BigQuery Table Schema:

- Define the schema for the BigQuery table, specifying field names, types, and modes. This schema corresponds to the structure of the data to be loaded.

#### Configure Clustering and Partitioning:

- Specify clustering and partitioning fields for optimizing query performance. I have used, clustering is based on the "ID" field, and partitioning is based on the "Library_ID" field.

#### Check and Create BigQuery Table:

- Check if the specified BigQuery table already exists. If it does not exist, create a new table with the defined schema, clustering, and partitioning settings.

#### Load Parquet File into BigQuery Table:

- Traverse the directory containing Parquet files. For each Parquet file, configure a BigQuery load job and execute it to load the file's contents into the specified table.

### Example Query for ID Mapping:

In order to get data for particular ID, you can simply run the `data_process_ingest.py`

#### `query_data_by_id` Function

This function queries data from a specified BigQuery table based on a given ID.

### Parameters:

- `self`: Assumes it's part of a class, but the usage context is not provided.
- `project_id`: Project ID where the BigQuery table is located, can be retrieved from config file.
- `dataset_id`: Dataset ID where the BigQuery table is stored,can be retrieved from config file.
- `bq_table_name`: Name of the BigQuery table,an be retrieved from config file.
- `target_id`: ID to be used in the WHERE clause for filtering, an be set to config file.

### Usage Example:

`python data_process_ingest.py `

### Supporting Functions

- load_yaml Function:
  Loads the contents of a YAML file into a dictionary for configuring the job.
