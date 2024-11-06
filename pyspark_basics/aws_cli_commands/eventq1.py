import boto3

def list_event_buses(region):
    # Create an EventBridge client
    client = boto3.client('events', region_name=region)
    
    # Call the list_event_buses API to get EventBridge event buses
    response = client.list_event_buses()
    
    # Print the name of each event bus
    if 'EventBuses' in response and response['EventBuses']:
        for bus in response['EventBuses']:
            print("Event Bus Name:", bus['Name'])
    else:
        print("No event buses found in this account.")

# Specify the AWS region (e.g., 'us-east-1')
region = 'us-east-1'
list_event_buses(region)
