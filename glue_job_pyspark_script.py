from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random
import boto3 

sc = SparkContext()
spark = SparkSession(sc)
s3_bucket = "samplebucketforetl"
s3_prefix = "deltatables/business_data_delta/"

s3 = boto3.client('s3')
s3_objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

parquet_objects = [obj for obj in s3_objects.get('Contents', []) if obj['Key'].endswith(".parquet")]

for parquet_obj in parquet_objects:
    parquet_location = f"s3://{s3_bucket}/{parquet_obj['Key']}"

    df = spark.read.parquet(parquet_location)

    empty_columns = [col_name for col_name in df.columns if df.where(col(col_name).isNotNull()).count() == 0]

    if empty_columns:
        print("Empty columns found:", empty_columns)
    else:
        print("No empty columns found.")

    sample_size = 50
    sample_df = df.sample(False, sample_size / df.count())

    sample_csv_location = f"s3://{s3_bucket}/output/sample_sample_{random.randint(1, 1000)}.csv"
    sample_df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(sample_csv_location)

print("Sample collection and processing complete.")
