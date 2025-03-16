#!/usr/bin/python3
import boto3
import sys

# Region we're working with
REGION = "us-east-2"

def check_account_id():
    """Verify the AWS account ID."""
    try:
        sts = boto3.client('sts', region_name=REGION)
        response = sts.get_caller_identity()
        print(f"AWS Account ID: {response['Account']}")
        print(f"IAM User/Role: {response['Arn']}")
        return response['Account']
    except Exception as e:
        print(f"Error getting account ID: {str(e)}")
        return None

def list_all_queues():
    """Try to list all queues in the region."""
    try:
        sqs = boto3.client('sqs', region_name=REGION)
        response = sqs.list_queues()
        
        if 'QueueUrls' in response:
            print("\nAvailable queues:")
            for url in response['QueueUrls']:
                print(f"  {url}")
            return True
        else:
            print("No queues found in this region")
            return False
    except Exception as e:
        print(f"Error listing queues: {str(e)}")
        return False

def direct_queue_check():
    """Try to access the specific queue directly."""
    queue_name = "2025-03-15-youtube-transcription-queue"
    account_id = "635071011057"
    
    queue_url = f"https://sqs.{REGION}.amazonaws.com/{account_id}/{queue_name}"
    print(f"\nTesting direct access to: {queue_url}")
    
    try:
        sqs = boto3.client('sqs', region_name=REGION)
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        print(f"Queue exists! ARN: {response['Attributes']['QueueArn']}")
        return True
    except Exception as e:
        print(f"Error accessing queue: {str(e)}")
        return False

if __name__ == "__main__":
    print("SQS Queue Existence Test")
    print("-----------------------")
    
    account_id = check_account_id()
    if account_id:
        print(f"The queue should be in account: {account_id}")
    
    print("\nAttempting to list queues...")
    list_all_queues()
    
    direct_result = direct_queue_check()
    
    print("\nTest Summary:")
    if direct_result:
        print("✅ Queue exists and is accessible")
    else:
        print("❌ Queue could not be accessed directly")
