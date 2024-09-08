from google.cloud import storage
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
        Upload to google cloud storage from local after extracting the file from google drive
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Set your GCS bucket name and destination file name
bucket_name = 'rahul-sales-data'
source_file_name = 'sales_dataset.csv'
destination_blob_name = 'sales_dataset.csv'