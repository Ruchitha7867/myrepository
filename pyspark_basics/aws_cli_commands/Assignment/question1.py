''' As a Data Engineer at RetailMart, you’re tasked with performing a comprehensive Customer Sales Analysis to provide 
insights into purchasing patterns and product performance. First, you initialize a Spark session named "CustomerAnalysis"
to handle large datasets. Then, you load order data from a CSV file stored in S3 and display the initial rows to verify
the data. Focusing on high-value purchases, you filter orders with an amount over ₹1,000, then add a discounted_price 
column reflecting a 10% discount on the original price. Next, you group the sales data by product_category to calculate 
total sales per category, offering insights into the top-selling products. To build a complete customer view, you join
the customer and order DataFrames on customer_id. Additionally, you analyze employee tenure by adding a years_of_experience
column based on joining_date in the employee DataFrame. Finally, you save the cleaned and aggregated sales data back to S3 
in Parquet format for efficient storage and future analysis, equipping RetailMart with actionable insights for strategic decision-making.'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date
spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()
order_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/order_data.csv")
order_df.show(5)
high_value_orders_df = order_df.filter(order_df['amount'] > 1000)
high_value_orders_df.show(5)
high_value_orders_df = high_value_orders_df.withColumn("discounted_price", order_df['amount'] * 0.9)
high_value_orders_df.show(5)
sales_per_category_df = high_value_orders_df.groupBy("product_category").sum("discounted_price")
sales_per_category_df.show()
customer_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/customer_data.csv")
customer_sales_df = customer_df.join(high_value_orders_df, customer_df["customer_id"] == high_value_orders_df["customer_id"])
customer_sales_df.show(5)
employee_df = spark.read.option("header", "true").csv("s3://retailmart-sales-data/employee_data.csv")
employee_df = employee_df.withColumn("years_of_experience", datediff(current_date(), employee_df["joining_date"]) / 365)
employee_df.show(5)
sales_per_category_df.write.parquet("s3://retailmart-sales-data/sales_data.parquet")
