import boto3

# Initialize Athena client
client = boto3.client('athena', region_name='us-east-1')  # Change to your AWS region

# Define your parameters
database_name = 'sample-db'  # Your Athena database name
table_name = 'source_folder'        # Your Athena table name
s3_data_location = 's3://my-athena-query-bucket123/source/'  # Your S3 data location
output_location = 's3://my-athena-query-bucket123/athena-results/'  # S3 location to store query results

# Define the query string to create the table
query_string = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
    id INT,
    name STRING,
    age INT,
    country STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '\"')
LOCATION '{s3_data_location}'
TBLPROPERTIES ('has_encrypted_data'='false');
"""

# Start query execution in Athena
response = client.start_query_execution(
    QueryString=query_string,
    ResultConfiguration={'OutputLocation': output_location}  # S3 bucket to store query results
)

# Extract the query execution ID from the response
query_id = response['QueryExecutionId']

# Print the Query Execution ID (useful for tracking the status)
print(f"Query Execution ID: {query_id}")
