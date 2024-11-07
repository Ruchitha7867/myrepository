from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, months_between, current_date
spark = SparkSession.builder \
    .appName("ChurnAnalysis") \
    .getOrCreate()
customer_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.input.tableName", "CustomerProfiles") \
    .load()
transaction_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/transaction_history.csv")
last_transaction_df = transaction_df.groupBy("customer_id") \
    .agg(max("transaction_date").alias("last_transaction_date"))
inactive_customers_df = customer_df.join(last_transaction_df, on="customer_id", how="left") \
    .withColumn("months_since_last_purchase", months_between(current_date(), col("last_transaction_date"))) \
    .filter((col("months_since_last_purchase") > 12) | (col("last_transaction_date").isNull()))
total_customers = customer_df.count()
inactive_customers = inactive_customers_df.count()
monthly_churn_rate = (inactive_customers / total_customers) / 12
print(f"Total Customers: {total_customers}")
print(f"Inactive Customers): {inactive_customers}")
print(f"Estimated Monthly Churn Rate: {monthly_churn_rate:.2%}")
inactive_customers_df.write \
    .format("dynamodb") \
    .option("dynamodb.output.tableName", "ChurnedCustomers") \
    .mode("append") \
    .save()
