from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

try:
    spark = SparkSession.builder \
        .appName("CombineCSVFiles") \
        .getOrCreate()

    input_path = "s3://my-athena-query-bucket123/source/Vehicle 1.csv"
    output_path = "s3://my-athena-query-bucket123/athena-results/"

    # Read CSV file from S3
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Write the DataFrame to S3 in Parquet format
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

except Py4JJavaError as e:
    print("Py4JJavaError Occurred:", e)
