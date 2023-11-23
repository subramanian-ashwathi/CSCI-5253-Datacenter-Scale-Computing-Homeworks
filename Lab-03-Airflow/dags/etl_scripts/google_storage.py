from google.cloud import storage

BUCKET_NAME = "outcomes_data"
CREDENTIALS_PATH = "/opt/airflow/creds.json"

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name, credentials_file):
    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client.from_service_account_json(credentials_file)

    # Get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # Upload the file to the bucket
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

    print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")

def download_from_gcs(bucket_name, local_file_path, storage_blob_name, credentials_file):
    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client.from_service_account_json(credentials_file)

    # Get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # Upload the file to the bucket
    blob = bucket.blob(storage_blob_name)
    blob.download_to_filename(local_file_path)

    print(f"File {local_file_path} downloaded from gs://{bucket_name}/{storage_blob_name}")

