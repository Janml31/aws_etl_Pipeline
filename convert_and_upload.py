
import pandas as pd
from deltalake.writer import write_deltalake
import boto3
import os
import shutil

csv_file_path = 'business_data.csv'
df = pd.read_csv(csv_file_path)

local_delta_table_path = 'business_data_delta'

if os.path.exists(local_delta_table_path):
    shutil.rmtree(local_delta_table_path)


delta_table = write_deltalake(local_delta_table_path, df)

s3_bucket = 'samplebucketforetl'
s3_prefix = 'deltatables/business_data_delta'  

s3_client = boto3.client('s3')

for root, dirs, files in os.walk(local_delta_table_path):
    for file in files:
        local_file_path = os.path.join(root, file)
        s3_key = os.path.join(s3_prefix, file)
        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"Uploaded {local_file_path} to S3://{s3_bucket}/{s3_key}")

print("Delta table uploaded to S3.")
