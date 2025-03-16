#!/usr/bin/python3
import json
import boto3
import sys
import argparse

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

def send_message_to_sqs(youtube_url, phrase):
    # Confirmed working queue URL and region
    queue_url = "https://sqs.us-east-2.amazonaws.com/635071011057/2025-03-15-youtube-transcription-queue"
    region = "us-east-2"
    
    try:
        # Create SQS client
        sqs = boto3.client('sqs', region_name=region)
        
        # Create message body
        message = {
            "youtube_url": youtube_url,
            "phrase": phrase
        }
        
        # Print what we're sending
        print(f"Sending message to queue: {queue_url}")
        print(f"YouTube URL: {youtube_url}")
        print(f"Phrase to search: {phrase}")
        
        # Send message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        print(f"Success! Message sent with ID: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        return False

def main():
    args = parse_arguments()
    send_message_to_sqs(args.youtube_url, args.phrase)

if __name__ == "__main__":
    main()
