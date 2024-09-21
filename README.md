# cloud_data_engineering
## Google Cloud data engineering 

This repository contains various data engineering workflows code designed to automate tasks such as extracting data from Google Drive, uploading data to Google Cloud Storage (GCS), running Dataproc workflows, and creating tables in BigQuery using Dataform. It also includes Airflow DAGs to orchestrate these processes.

### Main Services Used:
- **BigQuery**: Google’s fully managed data warehouse.
- **Airflow**: A platform for orchestrating complex workflows.
- **Dataform**: A tool to build scalable data workflows in BigQuery.
- **Dataproc**: A fully managed Spark and Hadoop service that allows you to run workflows efficiently.
- **Google Cloud Storage (GCS)**: A scalable and secure object storage service.
- **Google Drive**: A cloud storage service for extracting and storing files.



### Folder Structure:
- ``bigquery``: Contains workflows related to managing data within BigQuery,How to move the data from gcs to bigquery through code .
- ``dag``: Contains DAGs for Airflow, automating ETL and other workflows.
- ``dataform``: Contains SQLX scripts for analyzing  tables in BigQuery using Dataform.
- ``dataproc``: Python-based workflows to run jobs on Google Dataproc.
- ``extract_from_gdrive``: Scripts to extract files from Google Drive and upload them to BigQuery.
- ``upload_to_GCS``: Contains scripts to upload files to Google Cloud Storage.