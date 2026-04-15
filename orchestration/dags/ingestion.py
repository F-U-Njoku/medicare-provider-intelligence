import os
import io
import zipfile
import requests
from airflow import DAG
from pathlib import Path
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("AIRFLOW_VAR_GCS_BUCKET")
BRONZE_DATASET_NAME = os.getenv("AIRFLOW_VAR_BRONZE_DATASET_NAME")
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/medicare")

profile_config = ProfileConfig(
    profile_name="medicare",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="db_bq_conn",
        profile_args={"dataset": "medicare_silver"},
    ),
)

# CMS SynPUF Sample 1 (2008-2010)
# Note: Keeping the corrected casing for path segments
BASE_URL_WWW = "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_"
BASE_URL_STATS = "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/SynPUFs/Downloads/DE1_0_"
BASE_URL_FILES = "http://downloads.cms.gov/files/DE1_0_"

DATA_SOURCES = [
    {"type": "beneficiary", "year": "2008", "url": f"{BASE_URL_WWW}2008_Beneficiary_Summary_File_Sample_1.zip"},
    {"type": "beneficiary", "year": "2009", "url": f"{BASE_URL_WWW}2009_Beneficiary_Summary_File_Sample_1.zip"},
    {"type": "beneficiary", "year": "2010", "url": f"{BASE_URL_STATS}2010_Beneficiary_Summary_File_Sample_20.zip"},
    {"type": "inpatient", "year": "2008_2010", "url": f"{BASE_URL_WWW}2008_to_2010_Inpatient_Claims_Sample_1.zip"},
    {"type": "carrier", "year": "2008_2010_part_a", "url": f"{BASE_URL_FILES}2008_to_2010_Carrier_Claims_Sample_1A.zip"},
    {"type": "carrier", "year": "2008_2010_part_b", "url": f"{BASE_URL_FILES}2008_to_2010_Carrier_Claims_Sample_1B.zip"},
]

# --- HELPER FUNCTIONS ---
def download_and_extract(file_type, year, url):
    """Downloads zip, extracts the CSV, and streams it to GCS."""
    from google.cloud import storage  
    print(f"Downloading {file_type} ({year}) from {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    with requests.get(url, headers=headers, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to download. Status: {response.status_code}")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith('.csv'):
                    with z.open(filename) as f:
                        client = storage.Client()
                        bucket = client.bucket(BUCKET_NAME)
                        dest_path = f"bronze/{file_type}/{file_type}_{year}.csv"
                        blob = bucket.blob(dest_path)
                        if blob.exists():
                             print(f"⏭️ Skipping: gs://{BUCKET_NAME}/{dest_path} already exists.")
                             return
                        # Memory efficient streaming upload
                        blob.upload_from_file(f, content_type='text/csv')
                        print(f"✅ Success: Saved to gs://{BUCKET_NAME}/{dest_path}")

def create_external_table(project_id, dataset_id, table_id, bucket_name, gcs_path):
    """Creates a BigQuery external table over a GCS CSV file with autodetect."""
    from google.cloud import bigquery 
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    ext_config = bigquery.ExternalConfig("CSV")
    ext_config.source_uris = [f"gs://{bucket_name}/{gcs_path}"]
    ext_config.autodetect = True
    ext_config.csv_options.skip_leading_rows = 1

    table = bigquery.Table(table_ref)
    table.external_data_configuration = ext_config
    client.create_table(table, exists_ok=True)
    print(f"✅ External table {table_ref} created/updated.")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="medicare_pipeline",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['medicare', 'ingestion', 'transformation'],
) as dag:

    transform_data = DbtTaskGroup(
        group_id="transform_medicare_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models/staging", "path:models/marts"]
        )
    )

    # Synthesize tasks in a single loop for cleaner logic
    for entry in DATA_SOURCES:
        f_type = entry['type']
        f_year = entry['year']
        f_url = entry['url']
        
        # 1. Ingestion Task (Python)
        ingest_task = PythonOperator(
            task_id=f"ingest_{f_type}_{f_year}",
            python_callable=download_and_extract,
            op_kwargs={'file_type': f_type, 'year': f_year, 'url': f_url}
        )

        # 2. External Table Task (BigQuery)
        table_id = f"ext_{f_type}_{f_year}"
        gcs_path = f"bronze/{f_type}/{f_type}_{f_year}.csv"

        create_ext_table = PythonOperator(
            task_id=f"create_table_{table_id}",
            python_callable=create_external_table,
            op_kwargs={
                'project_id': PROJECT_ID,
                'dataset_id': BRONZE_DATASET_NAME,
                'table_id': table_id,
                'bucket_name': BUCKET_NAME,
                'gcs_path': gcs_path,
            }
        )

        # 3. Link: Ingest -> External Table -> dbt transformation
        ingest_task >> create_ext_table >> transform_data