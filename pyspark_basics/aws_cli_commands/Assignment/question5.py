'''5. RetailMart wants to identify inactive customers who haven’t made recent purchases.
Load customer data from DynamoDB and transaction data from S3, and calculate the last
purchase date for each customer using PySpark. Flag customers as inactive if their 
last purchase was more than six months ago, then update this status in DynamoDB.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, current_date
from datetime import datetime

spark = SparkSession.builder \
    .appName("InactiveCustomersAnalysis") \
    .getOrCreate()

customer_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.input.tableName", "Customers") \
    .load()
sales_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/transaction_data.csv")

sales_df = sales_df.withColumn("purchase_date", unix_timestamp("purchase_date", "yyyy-MM-dd").cast("timestamp"))
last_purchase_df = sales_df.groupBy("customer_id") \
    .agg({"purchase_date": "max"}).withColumnRenamed("max(purchase_date)", "last_purchase_date")
last_purchase_df = last_purchase_df.withColumn("inactive_days",(unix_timestamp(current_date()) - unix_timestamp("last_purchase_date")) / (60 * 60 * 24))
inactive_customers_df = last_purchase_df.withColumn("status",(col("inactive_days") > 180).cast("string"))
inactive_customers_df.show()


