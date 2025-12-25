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
    
def generate_ehr(num_records, patient_ids):
    print("Generating Electronic health records data in newline-delimited JSON format ...")
    ehr_records = []

    for _ in range(num_records):
        patient_id = random.choice(patient_ids)
        visit_date = fake.date_between(start_date=start_date, end_date=end_date)
        diagnosis_code = random.choice(['E11.9', 'I10', 'J45' ,'N18.9', 'Z00.0'])
        diagnosis_desc = {
            'E11.9': 'Type 2 diabetes mellitus without complications',
            'I10': 'Essential (primary) hypertension',
            'J45': 'Asthma',
            'N18.9': 'Chronic kidney disease, unspecified',
            'Z00.0': 'General adult medical examination'
        }[diagnosis_code]
        heart_rate = random.randint(60, 100)
        blood_pressure = f"{random.randint(110, 140)}/{random.randint(70, 90)}"
        temperature = round(random.uniform(97.0, 99.5), 1)
        
        ehr_record = {
            'patient_id': patient_id,
            'visit_date': str(visit_date),
            'diagnosis_code': diagnosis_code,
            'diagnosis_desc': diagnosis_desc,
            'heart_rate': heart_rate,
            'blood_pressure': blood_pressure,
            'temperature': temperature
        }

        ehr_records.append(json.dumps(ehr_record))

    return ehr_records

def generate_claims(num_records, patient_ids):
    print("Generating insurance claims data in parquet format with explicit schema ...")
    claims = []

    for _ in range(num_records):
        claim_id = fake.unique.uuid4()
        patient_id = random.choice(patient_ids)
        provider_id = fake.unique.uuid4()
        service_date = fake.date_between(start_date=start_date, end_date=end_date)
        service_date = datetime.combine(service_date, datetime.min.time())
        diagnosis_code = random.choice(['E11.9', 'I10', 'J45' ,'N18.9', 'Z00.0'])
        procedure_code = random.choice(['99213', '93000', '80050', '71020', '36415'])
        claim_amount = round(random.uniform(100.0, 5000.0), 2)
        status = random.choice(['Submitted', 'Processed', 'Denied', 'Paid'])

        claims.append({
            'claim_id': claim_id,
            'patient_id': patient_id,
            'provider_id': provider_id,
            'service_date': service_date,
            'diagnosis_code': diagnosis_code,
            'procedure_code': procedure_code,
            'claim_amount': float(claim_amount),
            'status': status
        })

        schema = pa.schema([
            ('claim_id', pa.string()),
            ('patient_id', pa.string()),
            ('provider_id', pa.string()),
            ('service_date', pa.timestamp('ms')),
            ('diagnosis_code', pa.string()),
            ('procedure_code', pa.string()),
            ('claim_amount', pa.float64()),
            ('status', pa.string())
        ])

    table = pa.Table.from_pandas(pd.DataFrame(claims), schema=schema)
    return table

def main():
    create_bucket()

    # Development Data
    empty_gcs_folder(DEV_PATH)

    patients_dev = generate_patients(DEV_PRODS)
    upload_to_gcs(patients_dev, DEV_PATH, 'patients_dev.csv', 'csv')

    ehr_dev = generate_ehr(DEV_PRODS, patients_dev['patient_id'].tolist())
    upload_to_gcs(ehr_dev, DEV_PATH, 'ehr_dev.json', 'json')

    claims_dev = generate_claims(DEV_PRODS, patients_dev['patient_id'].tolist())
    upload_to_gcs(claims_dev, DEV_PATH, 'claims_dev.parquet', 'parquet')

    # Production Data
    empty_gcs_folder(PROD_PATH)

    patients_prod = generate_patients(PROD_PRODS)
    upload_to_gcs(patients_prod, PROD_PATH, 'patients_prod.csv', 'csv')

    ehr_prod = generate_ehr(PROD_PRODS, patients_prod['patient_id'].tolist())
    upload_to_gcs(ehr_prod, PROD_PATH, 'ehr_prod.json', 'json')

    claims_prod = generate_claims(PROD_PRODS, patients_prod['patient_id'].tolist())
    upload_to_gcs(claims_prod, PROD_PATH, 'claims_prod.parquet', 'parquet')

print("Data generation and upload completed successfully.")

if __name__ == "__main__":
    main()
