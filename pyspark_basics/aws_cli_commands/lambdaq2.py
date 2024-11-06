import boto3
import json

def invoke_lambda_function(function_name, region):
    # Create a Lambda client
    client = boto3.client('lambda', region_name=region)
    
    # Define any payload data (if your Lambda function requires it)
    payload = {
        "key": "value"  # Example payload, modify as needed
    }
    
    # Invoke the Lambda function
    response = client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # For synchronous invocation
        Payload=json.dumps(payload)
    )
    
    # Print the response from the Lambda function
    response_payload = json.load(response['Payload'])
    print("Response:", response_payload)

# Replace 'your-function-name' and 'your-region' with actual values
invoke_lambda_function('my-lambda', 'us-east-1')
