import os
import io
import zipfile
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

PROJECT_ID = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("AIRFLOW_VAR_GCS_BUCKET")

# CMS SynPUF Sample 1 (2008-2010)

base_url_1 = "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_"
base_url_2 = "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/synpufs/downloads/de1_0_"
base_url_3 = "http://downloads.cms.gov/files/DE1_0_"

DATA_SOURCES = [
    {"type": "beneficiary", "year": "2008", "url": f"{base_url_1}2008_Beneficiary_Summary_File_Sample_1.zip"},
    {"type": "beneficiary", "year": "2009", "url": f"{base_url_1}2009_Beneficiary_Summary_File_Sample_1.zip"},
    {"type": "beneficiary", "year": "2010", "url": f"{base_url_2}2010_Beneficiary_Summary_File_Sample_20.zip"},
    {"type": "inpatient", "year": "2008_2010", "url": f"{base_url_1}2008_to_2010_Inpatient_Claims_Sample_1.zip"},
    {"type": "carrier", "year": "2008_2010_part_a", "url": f"{base_url_3}2008_to_2010_Carrier_Claims_Sample_1A.zip"},
    {"type": "carrier", "year": "2008_2010_part_b", "url": f"{base_url_3}2008_to_2010_Carrier_Claims_Sample_1B.zip"},
]

def download_and_extract(file_type, year, url):
    """Downloads zip, extracts the CSV, and streams it to GCS."""
    print(f"Downloading {file_type} ({year}) from {url}...")
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Use stream=True to avoid loading the whole zip into memory if possible
    with requests.get(url, headers=headers, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to download. Status: {response.status_code}")
        
        # ZipFile still needs a seekable object, so we use BytesIO for the zip itself
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith('.csv'):
                    with z.open(filename) as f:
                        # SET UP GCS CLIENT
                        client = storage.Client()
                        bucket = client.bucket(BUCKET_NAME)
                        dest_path = f"bronze/{file_type}/{file_type}_{year}.csv"
                        blob = bucket.blob(dest_path)
                        
                        # upload_from_file is much more memory efficient than upload_from_string
                        blob.upload_from_file(f, content_type='text/csv')
                        print(f"✅ Success: Saved as {dest_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="medicare_bronze_ingestion_v2",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['medicare', 'bronze']
) as dag:

    for entry in DATA_SOURCES:
        PythonOperator(
            task_id=f"ingest_{entry['type']}_{entry['year']}",
            python_callable=download_and_extract,
            op_kwargs={
                'file_type': entry['type'], 
                'year': entry['year'], 
                'url': entry['url']
            }
        )