import boto3

def create_eventbridge_rule(region, rule_name):
    # Create an EventBridge client
    client = boto3.client('events', region_name=region)
    
    # Define the schedule expression for the rule (every 5 minutes)
    schedule_expression = 'rate(5 minutes)'
    
    # Create the rule
    response = client.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State='ENABLED',  # Rule will be active immediately
        Description='This rule triggers every 5 minutes',
    )
    
    # Print the rule ARN (Amazon Resource Name) to verify it's created
    print(f"EventBridge rule created: {response['RuleArn']}")

# Replace with your desired region and rule name
region = 'us-east-1'  # Example region
rule_name = 'Every5MinutesRule'
create_eventbridge_rule(region, rule_name)
