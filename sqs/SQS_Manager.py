import os
import json
import boto3
import logging
from dotenv import load_dotenv

load_dotenv()

# Set up logging configuration
logging.basicConfig(
    filename='sqs_manager.log',          # Log file location
    level=logging.INFO,                  # Log level (INFO or DEBUG for more detailed logs)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

class SQSManager:
    def __init__(self, queue_url: str):
        try:
            self.sqs_client = boto3.client(
                'sqs',
                region_name=os.getenv('AWS_REGION'),
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            self.queue_url = queue_url
            logging.info(f"Initialized SQSManager with queue URL: {self.queue_url}")
        except Exception as e:
            logging.error(f"Error initializing SQSManager: {str(e)}")

    def send_message(self, message: dict):
        try:
            response = self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message),
                MessageGroupId="default"
            )
            logging.info(f"Sent message to SQS: {message}")
            logging.debug(f"Sent mesage")
            return response
        except Exception as e:
            logging.error(f"Error sending message to SQS: {str(e)}")

    def receive_message(self):
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            messages = response.get('Messages', [])
            if messages:
                logging.info(f"Received message from SQS: {messages[0]}")
            else:
                logging.info("No messages received from SQS.")
            return messages
        except Exception as e:
            logging.error(f"Error receiving message from SQS: {str(e)}")
            return []

    def delete_message(self, receipt_handle: str):
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logging.info(f"Deleted message with receipt handle: {receipt_handle}")
        except Exception as e:
            logging.error(f"Error deleting message from SQS: {str(e)}")
