#!/usr/bin/python3
import json
import boto3
import sys
import argparse
import os
import botocore.exceptions

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Send YouTube URLs to an SQS queue for processing."
    )
    parser.add_argument(
        "--youtube_url", "-u",
        type=str,
        required=True,
        help="YouTube URL to send to the queue."
    )
    parser.add_argument(
        "--phrase", "-p",
        type=str,
        default="hustle",
        help="Phrase to search for in the transcript."
    )
    return parser.parse_args()

def check_aws_credentials():
    """Verify AWS credentials are available and valid."""
    # Check environment variables
    if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
        return True, "Using AWS credentials from environment variables"
    
    # Check credentials file
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials:
            return True, "Using AWS credentials from credentials file or config"
        else:
            return False, "No AWS credentials found in environment or config files"
    except Exception as e:
        return False, f"Error checking AWS credentials: {str(e)}"

def send_message_to_sqs(youtube_url, phrase):
    # Confirmed working queue URL and region
    queue_url = "https://sqs.us-east-2.amazonaws.com/635071011057/2025-03-15-youtube-transcription-queue"
    region = "us-east-2"
    
    # Check credentials first
    credentials_valid, credentials_message = check_aws_credentials()
    if not credentials_valid:
        print(f"❌ {credentials_message}")
        print("\n📋 AWS Credential Setup Guide:")
        print("  1. Install AWS CLI: pip install awscli")
        print("  2. Configure credentials: aws configure")
        print("  3. Enter your AWS Access Key ID and Secret Access Key when prompted")
        print("  4. Specify region: us-east-2")
        print("\nAlternatively, set these environment variables:")
        print("  export AWS_ACCESS_KEY_ID=your_access_key")
        print("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
        print("  export AWS_DEFAULT_REGION=us-east-2")
        return False
    
    print(f"✅ {credentials_message}")
    
    try:
        # Create SQS client
        sqs = boto3.client('sqs', region_name=region)
        
        # Create message body
        message = {
            "youtube_url": youtube_url,
            "phrase": phrase
        }
        
        # Print what we're sending
        print(f"📤 Sending message to queue: {queue_url}")
        print(f"🔗 YouTube URL: {youtube_url}")
        print(f"🔍 Phrase to search: {phrase}")
        
        # First verify queue exists
        try:
            # Test if we can get queue attributes (will fail if queue doesn't exist or no permission)
            sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['QueueArn']
            )
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'InvalidClientTokenId':
                print(f"❌ Authentication failed: Your AWS credentials are invalid or expired")
                return False
            elif error_code == 'NonExistentQueue':
                print(f"❌ Queue not found: The specified queue does not exist or you don't have access to it")
                return False
            elif error_code == 'AccessDenied':
                print(f"❌ Access denied: You don't have permission to access this queue")
                return False
            else:
                print(f"❌ Queue verification error: {str(e)}")
                return False
        
        # Send message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        print(f"✅ Success! Message sent with ID: {response['MessageId']}")
        return True
        
    except botocore.exceptions.NoCredentialsError:
        print("❌ AWS credentials not found. Please configure your AWS credentials.")
        print("   Run 'aws configure' to set up your credentials.")
        return False
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'InvalidClientTokenId':
            print("❌ Invalid AWS credentials. Your access keys may be incorrect or expired.")
            print("   Run 'aws configure' with valid credentials.")
        elif error_code == 'AccessDenied':
            print("❌ Access denied. Your AWS user doesn't have permission to send messages to this queue.")
            print("   Ask your AWS administrator to grant SQS SendMessage permissions.")
        else:
            print(f"❌ AWS Error: {str(e)}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def main():
    args = parse_arguments()
    success = send_message_to_sqs(args.youtube_url, args.phrase)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
