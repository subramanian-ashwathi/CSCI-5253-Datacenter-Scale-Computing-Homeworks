import requests
import pandas as pd
import datetime
from pathlib import Path
from dags.etl_scripts.google_storage import upload_to_gcs, BUCKET_NAME, CREDENTIALS_PATH

DATASET_URL = "https://data.austintexas.gov/resource/9t4d-g238.json"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

def extract_data(output_dir, output_file, **context):
    running_date = datetime.datetime.strptime(context["ds"], "%Y-%m-%d")
    
    start_date_format = (running_date - datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    end_date_format = running_date.strftime(DATE_FORMAT)
    dataset_query = f"{DATASET_URL}?$where=datetime between '{start_date_format}' and '{end_date_format}'"
    print(dataset_query)
    res = requests.get(dataset_query)

    if res.status_code != 200:
        raise Exception(res.content)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(res.json()).to_csv(output_file, index=False)

    print("Uploading file to Google Storage...")
    upload_to_gcs(BUCKET_NAME, output_file, output_file, CREDENTIALS_PATH)

