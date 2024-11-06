import boto3
from botocore.exceptions import ClientError

def add_item_to_dynamodb(table_name, item_data, region='us-east-1'):
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    # Prepare the item to be added (attributes must match the table schema)
    item = {key: {'S': value} if isinstance(value, str) else {'N': str(value)} for key, value in item_data.items()}
    
    try:
        # Add the item to the DynamoDB table
        response = dynamodb.put_item(
            TableName=table_name,
            Item=item
        )
        print(f"Item added successfully: {response}")
    
    except ClientError as e:
        print(f"Error adding item: {e.response['Error']['Message']}")

# Example usage:
table_name = 'MyTable'  # Replace with your table name
item_data = {
    'employee name': 'John Doe',  # Primary key or sort key
    'id': '12345',                # Additional attributes
    'age': 30,
    'email': 'john@example.com'
}


add_item_to_dynamodb(table_name, item_data)
