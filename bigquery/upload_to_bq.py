import os
from google.oauth2 import service_account
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Replace these variables with your own values
SERVICE_ACCOUNT_FILE = 'credentials.json'
DRIVE_FILE_ID = '1zhtB841WDsoLzsHypIRhLf77qdJgcxME'
BIGQUERY_TABLE_ID = 'datapipeline-434821.rahul.sales_dataset'

# Authenticate to Google Drive and BigQuery
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery'
    ]
)

drive_service = build('drive', 'v3', credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials)

# Step 1: Download the file from Google Drive
def download_file_from_drive(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%.")

        file_io.seek(0)  # Reset the pointer to the beginning of the file
        return file_io
    except Exception as e:
        print(f"Failed to download file from Google Drive: {e}")
        raise

try:
    file_io = download_file_from_drive(DRIVE_FILE_ID)
except Exception as e:
    print(f"Error downloading file: {e}")
    raise

# Step 2: Load the file into BigQuery
def load_file_into_bigquery(file_io, table_id):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Assuming the file is a CSV
        autodetect=True,  # Let BigQuery infer the schema
        skip_leading_rows=1  # Skip the header row if present
    )

    try:
        # Load the data from the file into BigQuery
        load_job = bigquery_client.load_table_from_file(
            file_io,
            table_id,
            job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load data into BigQuery: {e}")
        raise

try:
    load_file_into_bigquery(file_io, BIGQUERY_TABLE_ID)
except Exception as e:
    print(f"Error during BigQuery load: {e}")
