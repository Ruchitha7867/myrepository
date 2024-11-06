import boto3

def list_cloudwatch_alarms(region):
    # Create a CloudWatch client
    client = boto3.client('cloudwatch', region_name=region)

    # Call DescribeAlarms to list all alarms
    response = client.describe_alarms()

    # Check if alarms exist and print their names
    if 'MetricAlarms' in response and response['MetricAlarms']:
        for alarm in response['MetricAlarms']:
            print("Alarm Name:", alarm['AlarmName'])
    else:
        print("No CloudWatch alarms found in this region.")

# Replace with the region of your choice (e.g., 'us-east-1')
region = 'us-east-1'
list_cloudwatch_alarms(region)
