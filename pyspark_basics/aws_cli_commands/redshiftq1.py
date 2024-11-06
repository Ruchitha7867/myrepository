import boto3

def list_redshift_clusters(region='us-east-1'):
    # Create a Redshift client
    redshift = boto3.client('redshift', region_name=region)
    
    try:
        # List all Redshift clusters
        response = redshift.describe_clusters()
        
        # Check if there are clusters
        if 'Clusters' in response:
            print("Redshift Clusters:")
            for cluster in response['Clusters']:
                print(f"Cluster Identifier: {cluster['ClusterIdentifier']}")
                print(f"Cluster Status: {cluster['ClusterStatus']}")
                print(f"Node Type: {cluster['NodeType']}")
                print(f"Endpoint: {cluster['Endpoint']['Address']}")
                print(f"Port: {cluster['Endpoint']['Port']}")
                print('-' * 50)
        else:
            print("No Redshift clusters found.")

    except Exception as e:
        print(f"Error listing Redshift clusters: {e}")

# Example usage:
region = 'us-east-1' 
list_redshift_clusters(region)
