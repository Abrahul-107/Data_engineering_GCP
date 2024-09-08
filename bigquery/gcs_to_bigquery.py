from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define dataset and table names
dataset_id = 'rahul'
full_table_id = 'datapipeline-434821.rahul.nyc_dataset'

# Define the URI of the file in Google Cloud Storage
source_uri = 'gs://nyc_taxi_dataset/nyc_tlc_yellow_trips_2018_subset_1.csv'

# Define the job configuration
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
)

# Create the load job
load_job = client.load_table_from_uri(
    source_uri,
    full_table_id,
    job_config=job_config
)

# Wait for the load job to complete
load_job.result()

print(f"Loaded data into {full_table_id} with schema auto-detection.")
