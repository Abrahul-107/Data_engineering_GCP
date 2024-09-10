#This job is to add all python executable files to Dataproc Workflow
import google.cloud.dataproc_v1 as dataproc
from google.cloud import storage

def fetch_py_files_from_gcs(bucket_path):
    client = storage.Client()
    bucket_name, folder_path = bucket_path.split("/", 3)[2:]
    bucket = client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=folder_path, delimiter="/")
    py_files = []

    for blob in blobs:
        if blob.name.endswith(".py"):
            py_files.append(f"gs://{bucket_name}/{blob.name}")

    return py_files

# Function to delete an existing Dataproc workflow
def delete_existing_workflow(project_id, region, template_id):
    project_id = "rakeshmohandas_XXXX"  # Replace with your actual project ID
    region = "us-central1"
    template_id = "specify a template ID HERE"
    client = dataproc.WorkflowTemplateServiceClient(client_options={
        "api_endpoint": f"{region}-dataproc.googleapis.com:443"
    })

    template_name = f"projects/{project_id}/regions/{region}/workflowTemplates/{template_id}"
    client.delete_workflow_template(name=template_name)
    print(f"Workflow Template {template_id} deleted.")

def create_dataproc_workflow(py_files):
    project_id = "rakeshmohandas_XXXX"  # Replace with your actual project ID
    region = "us-central1"
    template_id = "specify a template ID HERE"

    # Create a client with explicit endpoint
    client = dataproc.WorkflowTemplateServiceClient(client_options={
        "api_endpoint": "us-central1-dataproc.googleapis.com:443"
    })

    # Define the workflow template request
    template = {
        "id": template_id,
        "name": "",
        "labels": {},
        "placement": {
            "managed_cluster": {
                "cluster_name": "dataproc",
                "config": {
                    "config_bucket": "417864390887_us_import_content",  # Replace with your actual config bucket
                    "gce_cluster_config": {
                        "network_uri": "",  # Replace with your actual network URI
                        "subnetwork_uri": "rakeshmohandas-vpc",  # Replace with your actual subnetwork URI
                        "internal_ip_only": False,
                        "zone_uri": "us-central1-b",
                        "metadata": {},
                        "tags": [],
                        "shielded_instance_config": {
                            "enable_secure_boot": False,
                            "enable_vtpm": False,
                            "enable_integrity_monitoring": False
                        }
                    },
                    "master_config": {
                        "num_instances": 1,
                        "machine_type_uri": "n2-standard-4",
                        "disk_config": {
                            "boot_disk_type": "pd-standard",
                            "boot_disk_size_gb": 500,
                            "num_local_ssds": 0
                        },
                        "min_cpu_platform": "",
                        "image_uri": ""
                    },
                    "worker_config": {
                        "num_instances": 2,
                        "machine_type_uri": "n2-standard-4",
                        "disk_config": {
                            "boot_disk_type": "pd-standard",
                            "boot_disk_size_gb": 500,
                            "num_local_ssds": 0
                        },
                        "min_cpu_platform": "",
                        "image_uri": ""
                    },
                    "secondary_worker_config": {
                        "num_instances": 0
                    },
                    "software_config": {
                        "image_version": "2.1-debian11",
                        "properties": {},
                        "optional_components": ["JUPYTER"]
                    },
                    "initialization_actions": [
                        {
                            "executable_file": "gs://goog-dataproc-initialization-actions-asia-south1/connectors/connectors.sh"
                        },
                        {
                            "executable_file": "gs://goog-dataproc-initialization-actions-asia-south1/python/pip-install.sh"
                        }
                    ]
                },
                "labels": {
                    "cluster": "XXXXXXXXX"
                }
            }
        },
        "jobs": [],
        "parameters": []
    }


    jobs = []
    for i, py_file in enumerate(py_files, start=1):
        job = {
            "pyspark_job": {
                "main_python_file_uri": py_file,
                "python_file_uris": [],
                "jar_file_uris": [],
                "file_uris": [],
                "archive_uris": [],
                "properties": {},
                "args": []
            },
            "step_id": f"xxx-job{i}",
            "labels": {},
            "prerequisite_step_ids": []
        }
        jobs.append(job)

    template["jobs"] = jobs

    # Create the workflow template
    parent = f"projects/{project_id}/regions/{region}"
    response = client.create_workflow_template(parent=parent, template=template)

    # Get the template ID for submission
    template_name = response.name
    print(f"Workflow Template created with ID: {template_name}")

if __name__ == "__main__":
    gcs_folder_path = "gs://XXXXXXX/notebooks/jupyter/XXX-jobs-new/"
    py_files = fetch_py_files_from_gcs(gcs_folder_path)
    if not py_files:
        print("No .py files found in the specified GCS folder.")
    else:
        create_dataproc_workflow(py_files)