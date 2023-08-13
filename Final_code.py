from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random
import boto3 

# Initialize Spark context and session
sc = SparkContext()
spark = SparkSession(sc)

# S3 bucket and prefix
s3_bucket = "samplebucketforetl"
s3_prefix = "deltatables/business_data_delta/"

# List objects in the S3 prefix
s3 = boto3.client('s3')
s3_objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

# Process only Parquet files
parquet_objects = [obj for obj in s3_objects.get('Contents', []) if obj['Key'].endswith(".parquet")]

# Function to check for empty columns
def get_empty_columns(df):
    return [col_name for col_name in df.columns if df.where(col(col_name).isNotNull()).count() == 0]

# Iterate through Parquet files
for parquet_obj in parquet_objects:
    parquet_location = f"s3://{s3_bucket}/{parquet_obj['Key']}"

    # Read Parquet file into a DataFrame
    df = spark.read.parquet(parquet_location)

    # Collect a random sample
    sample_size = 50
    sample_df = df.sample(False, sample_size / df.count())

    # Get empty columns in the sample
    empty_columns_in_sample = get_empty_columns(sample_df)

    if empty_columns_in_sample:
        print("Empty columns found in sample:", empty_columns_in_sample)
        
        # Filter non-empty rows in the original data
        non_empty_rows = df
        for col_name in empty_columns_in_sample:
            non_empty_rows = non_empty_rows.filter(~col(col_name).isNull())
        
        # Append non-empty rows to the sample
        sample_df = sample_df.union(non_empty_rows)

    else:
        print("No empty columns found in sample.")

    # Save the sample as a CSV file
    sample_csv_location = f"s3://{s3_bucket}/output/sample_sample_{random.randint(1, 1000)}.csv"
    sample_df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(sample_csv_location)

print("Sample collection and processing complete.")
