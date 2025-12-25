import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
from google.cloud import storage

# We specify the filepath for json key file
key_file_path = 'C:\Users\\teffm\Documents\PROGRAMMING\DATA_ENGINEERING\healthcare-data-project-482216-cb3af346ad83.json'
storage_client = storage.Client.from_service_account_json(key_file_path)

buckets = list(storage_client.list_buckets())
print("Successfully connected to GCP. Available buckets:")
for bucket in buckets:
    print(f" - {bucket.name}")
fake = Faker()

BUCKET_NAME = 'health-data-bucket'
DEV_PATH = 'dev/'
PROD_PATH = 'prod/'

DEV_PRODS = 5000
PROD_PRODS = 20000

start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 1, 1)

def create_bucket():
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        if not bucket.exists():
            bucket = storage_client.create_bucket(BUCKET_NAME)
            print(f"Bucket {BUCKET_NAME} created successfully.")
        else:
            print(f"Bucket {BUCKET_NAME} already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")
    
def empty_gcs_folder(path):
    print(f"Emptying folder '{path}' in GCS bucket '{BUCKET_NAME}' ...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix = path)
    for blob in blobs:
        blob.delete()
        print(f"Deleted '{blob.name}'.")
    print(f"Completed emptying folder '{path}'.")

def upload_to_gcs(data, path, filename, file_format):
    print(f"Uploading {filename} in {file_format} format to GCS ...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(path + filename)

    if file_format == 'csv':
        csv_data = data.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
    elif file_format == 'json':
        json_data = "\n".join(data)
        blob.upload_from_string(json_data, content_type='application/json')
    elif file_format == 'parquet':
        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        buffer.seek(0)
        blob.upload_from_string(buffer, content_type='application/octet-stream')
    print(f"Upload of {filename} completed to {path}.")

def generate_patients(num_records):
    print("Generating patient demographic data ...")
    patients = []
    for _ in range(num_records):
        patient_id = fake.unique.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        age = random.randint(0, 100)
        gender = random.choice(['Male', 'Female'])
        zip_code = fake.zipcode()
        insurance_type = random.choice(['Private', 'Medicare', 'Medicaid'])
        registration_date = fake.date_between(start_date=start_date, end_date=end_date)
        
        patients.append({
            'patient_id': patient_id,
            'first_name': first_name,
            'last_name': last_name,
            'age': age,
            'gender': gender,
            'zip_code': zip_code,
            'insurance_type': insurance_type,
            'registration_date': str(registration_date)
        })
        return pd.DataFrame(patients)
    
    