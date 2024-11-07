'''The company wants to categorize customers into tiers (e.g., "Bronze," "Silver," "Gold")
based on total purchase value. Load transaction data from S3 and customer information
from DynamoDB, join them, then use PySpark to calculate total spending per customer.
Assign categories based on thresholds and save results back to DynamoDB.'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
spark = SparkSession.builder \
    .appName("CustomerTiersAnalysis") \
    .config("spark.jars", "/home/user/path/to/spark-dynamodb-connector.jar") \
    .getOrCreate()
transaction_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/transactions.csv")
customer_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.input.tableName", "Customers") \
    .load()
joined_df = customer_df.join(transaction_df, on="customer_id", how="inner")
total_spending_df = joined_df.groupBy("customer_id").agg(
    sum("purchase_amount").alias("total_spending")
)
total_spending_df = total_spending_df.withColumn(
    "tier",
    when(col("total_spending") < 1000, "Bronze")
    .when(col("total_spending") >= 1000 & (col("total_spending") < 5000), "Silver")
    .otherwise("Gold")
)
total_spending_df.write \
    .format("dynamodb") \
    .option("dynamodb.output.tableName", "CustomersWithTiers") \
    .mode("overwrite") \
    .save()
total_spending_df.show()
