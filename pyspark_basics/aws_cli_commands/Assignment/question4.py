''' To better understand purchasing behavior, load customer data from DynamoDB and join it
 with sales data from S3. Use PySpark to calculate the time intervals between each purchase 
 or individual customers, then find the average transaction interval to identify 
 high-engagement customers.
 '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lag
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CustomerPurchasingBehavior") \
    .getOrCreate()
sales_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/sales_data.csv")
customer_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.input.tableName", "Customers") \
    .load()
joined_df = customer_df.join(sales_df, on="customer_id", how="inner")
joined_df = joined_df.withColumn("purchase_date", unix_timestamp("purchase_date", "yyyy-MM-dd"))
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")
joined_df = joined_df.withColumn("previous_purchase_date", lag("purchase_date").over(window_spec))
joined_df = joined_df.withColumn("time_interval", col("purchase_date") - col("previous_purchase_date"))
average_interval_df = joined_df.filter("time_interval is not null") \
    .groupBy("customer_id") \
    .agg({"time_interval": "avg"}) \
    .withColumnRenamed("avg(time_interval)", "average_transaction_interval")
high_engagement_customers_df.show()
