#!/usr/bin/python3
import argparse
import json
import boto3

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Send YouTube URLs to an SQS queue for processing."
    )
    parser.add_argument(
        "--queue_name", "-q",
        type=str,
        default="2025-03-15-youtube-transcription-queue",
        help="Name of the SQS queue to send messages to."
    )
    parser.add_argument(
        "--region", "-r",
        type=str,
        default="us-east-2",
        help="AWS region for the SQS queue. (Default: 'us-east-2')"
    )
    parser.add_argument(
        "--account_id", "-a",
        type=str,
        default="635071011057",
        help="AWS account ID."
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
        help="Optional phrase to search for (overrides default in processor)."
    )
    return parser.parse_args()

def send_message_to_sqs(region, account_id, queue_name, youtube_url, phrase=None):
    """Send a message to the SQS queue using the queue name instead of URL."""
    sqs = boto3.resource('sqs', region_name=region)
    
    # Get the queue by name
    queue = sqs.get_queue_by_name(
        QueueName=queue_name
    )
    
    # Create message body
    message = {
        "youtube_url": youtube_url
    }
    
    # Add phrase if provided
    if phrase:
        message["phrase"] = phrase
    
    # Send message to SQS
    response = queue.send_message(
        MessageBody=json.dumps(message)
    )
    
    print(f"Message sent to SQS queue: {queue_name}")
    print(f"YouTube URL: {youtube_url}")
    if phrase:
        print(f"Phrase: {phrase}")
    print(f"Message ID: {response.get('MessageId')}")
    
    return response.get('MessageId')

def main():
    args = parse_arguments()
    
    print(f"Sending message to queue: {args.queue_name} in region {args.region}")
    
    message_id = send_message_to_sqs(
        args.region,
        args.account_id,
        args.queue_name,
        args.youtube_url,
        args.phrase
    )
    
    print(f"Successfully sent message with ID: {message_id}")

if __name__ == "__main__":
    main()
