import boto3
 
lambda_client = boto3.client('lambda', region_name='us-east-1')  # Replace with your region
 
def list_lambda_functions():
    try:
        # Call the list_functions API
        response = lambda_client.list_functions()
        if 'Functions' in response:
            print("Lambda Functions in your AWS account:")
            for function in response['Functions']:
                print("Function Name:", function['FunctionName'])
        else:
            print("No Lambda functions found.")
 
    except Exception as e:
        print("Error listing Lambda functions:", str(e))
 
list_lambda_functions()