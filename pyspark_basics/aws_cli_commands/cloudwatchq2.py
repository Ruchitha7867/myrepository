import boto3

def create_cpu_utilization_alarm(instance_id, region, threshold, alarm_name):
    # Create CloudWatch and EC2 clients
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)

    # Get the EC2 instance's metrics for CPU utilization
    metric_name = 'CPUUtilization'
    namespace = 'AWS/EC2'

    # Define the alarm parameters
    response = cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        MetricName=metric_name,
        Namespace=namespace,
        Statistic='Average',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': instance_id
            },
        ],
        Period=300,  # 5 minutes
        EvaluationPeriods=1,  # Trigger alarm after 1 period
        Threshold=threshold,  # Set the threshold for the alarm
        ComparisonOperator='GreaterThanThreshold',  # Alarm when CPU > threshold
        AlarmDescription='Alarm when CPU utilization exceeds threshold',
        ActionsEnabled=False  # Disable actions like sending notifications
    )

    print(f"CloudWatch Alarm created: {alarm_name}")

# Replace with your EC2 instance ID, region, threshold, and alarm name
instance_id = 'i-xxxxxxxxxxxxxxxxx'  # Example EC2 instance ID
region = 'us-east-1'  # Example region
threshold = 80.0  # Trigger alarm if CPU > 80%
alarm_name = 'HighCPUUtilizationAlarm'
create_cpu_utilization_alarm(instance_id, region, threshold, alarm_name)
