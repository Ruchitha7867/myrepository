import boto3

def list_dynamodb_tables(region):
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name=region)

    # Use the list_tables method to get the list of table names
    response = dynamodb.list_tables()

    # Print the names of all tables
    if 'TableNames' in response:
        print("DynamoDB Tables:")
        for table_name in response['TableNames']:
            print(table_name)
    else:
        print("No tables found.")

# Specify the AWS region you want to list the tables for
region = 'us-east-1' 
list_dynamodb_tables(region)
