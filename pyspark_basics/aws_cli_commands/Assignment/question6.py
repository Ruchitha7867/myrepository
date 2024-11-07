''' RetailMart wants to monitor unusual spending patterns as part of fraud detection.
Load sales transactions from S3 and join with customer data from DynamoDB. 
Calculate the average spending per transaction and flag any transactions that 
exceed a certain threshold as anomalies, then log these flags in DynamoDB. '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()
sales_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/transactions.csv")
customer_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.input.tableName", "Customers") \
    .load()
customer_sales_df = sales_df.join(customer_df, on="customer_id", how="inner")
average_spending = customer_sales_df.select(avg("amount")).first()[0]
anomaly_threshold = average_spending * 1.5
anomalies_df = customer_sales_df.withColumn("is_anomalous", col("amount") > anomaly_threshold)
flagged_transactions_df = anomalies_df.filter(col("is_anomalous") == True)
flagged_transactions_df.show()
flagged_transactions_df.write \
    .format("dynamodb") \
    .option("dynamodb.output.tableName", "FlaggedTransactions") \
    .mode("append") \
    .save()
