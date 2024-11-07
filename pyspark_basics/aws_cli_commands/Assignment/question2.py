'''RetailMart wants to track customer loyalty by identifying repeat purchases. 
Load customer details from DynamoDB and transaction data from S3, then join them on
customer_id. Use PySpark to count repeat purchases per customer, identifying top 
repeat customers for loyalty program targeting.'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerLoyaltyAnalysis").getOrCreate()
customer_df = spark.read.option("header", "true").csv("s3://your-bucket-name/customers.csv")
customer_df.show(5)
transaction_df = spark.read.option("header", "true").csv("s3://your-bucket-name/transactions.csv")
transaction_df.show(5)
joined_df = customer_df.join(transaction_df, on="customer_id", how="inner")
joined_df.show(5)
repeat_purchases_df = joined_df.groupBy("customer_id").count()
repeat_purchases_df.show(5)
top_customers_df = repeat_purchases_df.orderBy("count", ascending=False)
top_customers_df.show(5)
