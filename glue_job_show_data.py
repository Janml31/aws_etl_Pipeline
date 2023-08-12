from pyspark.context import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext()
spark = SparkSession(sc)
parquet_path = "s3://samplebucketforetl/deltatables/business_data_delta/"
df = spark.read.parquet(parquet_path)
df.show(5)

spark.stop()