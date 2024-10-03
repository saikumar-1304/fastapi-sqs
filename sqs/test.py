import os
import json
from dotenv import load_dotenv
from SQS_Manager import SQSManager
from sqs.QueueProcessor import process_sqs_messages

# Load environment variables
load_dotenv()

# Initialize SQS Manager with your SQS queue URL
sqs_manager = SQSManager(queue_url=os.getenv('SQS_QUEUE_URL'))

# Define a test message to send
school = "TestSchool"
subject = "TestSubject"
s3_path = "s3://eonpod-data/CIS/Science/16-09-2024_06-34-24/Anu Mam Agriculture__transcript.txt"  # Example S3 path

sqs_message = {
    "school": school,
    "subject": subject,
    "s3_path": s3_path
}

# Send the message to the queue
def send_test_message():
    try:
        response = sqs_manager.send_message(sqs_message)
        print("Message sent to SQS:", response)
    except Exception as e:
        print(f"Failed to send message: {str(e)}")

# Read a message from the queue and process it
def read_and_process_message():
    try:
        messages = sqs_manager.receive_message()
        if not messages:
            print("No messages to process.")
            return

        for message in messages:
            print("Message received:", message)
            message_body = json.loads(message['Body'])
            print(f"Processing message: {message_body}")
            process_sqs_messages()
            # Simulate processing the message (e.g., downloading the file or any other task)
            print(f"Processing for school: {message_body['school']}, subject: {message_body['subject']}, s3_path: {message_body['s3_path']}")

            # After processing, delete the message from the queue
            sqs_manager.delete_message(message['ReceiptHandle'])
            print("Message deleted from SQS.")

    except Exception as e:
        print(f"Error processing message: {str(e)}")

# Test the flow
if __name__ == "__main__":
    # Step 1: Send the message to the queue
    send_test_message()
    read_and_process_message()
    # Step 2: Read
