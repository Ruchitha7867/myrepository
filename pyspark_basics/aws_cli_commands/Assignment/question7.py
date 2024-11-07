from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("Customer Favorite Category Analysis") \
    .getOrCreate()
transactions_df = spark.read.csv("s3://retailmart-sales-data/transactions.csv", header=True, inferSchema=True)
customer_df = spark.read \
    .format("dynamodb") \
    .option("tableName", "CustomerTable") \
    .load()
category_count_df = transactions_df.groupBy("customer_id", "category") \
    .agg(count("*").alias("purchase_count"))
window_spec = Window.partitionBy("customer_id").orderBy(col("purchase_count").desc())
favorite_category_df = category_count_df \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("customer_id", "category") \
    .withColumnRenamed("category", "favorite_category")
customer_profile_df = customer_df.join(favorite_category_df, on="customer_id", how="left")
customer_profile_df.write \
    .format("dynamodb") \
    .option("tableName", "CustomerTable") \
    .mode("overwrite") \
    .save()
spark.stop()

 