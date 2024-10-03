import os
import json
import boto3
import logging
from dotenv import load_dotenv
from SQS_Manager import SQSManager
from FileProcessor import generate_summary_and_quiz, save_result

# Load environment variables
load_dotenv()

# Set up logging configuration
logging.basicConfig(
    filename='file_processor.log',  # Log file location
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

def download_file_from_s3(s3_path, download_directory="downloads"):
    bucket_name, key = s3_path.replace("s3://", "").split("/", 1)

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    local_file_path = os.path.join(download_directory, key.split("/")[-1])

    s3_client = boto3.client('s3')

    try:
        logging.info(f"Downloading {s3_path} from S3 to {local_file_path}")
        s3_client.download_file(bucket_name, key, local_file_path)
        logging.info(f"Downloaded to {local_file_path}")
    except Exception as e:
        logging.error(f"Error downloading file from S3: {str(e)}")
        raise

    return local_file_path

def process_sqs_messages(queue_url: str):
    sqs_manager = SQSManager(queue_url)

    while True:
        try:
            messages = sqs_manager.receive_message()

            if not messages:
                logging.info("No messages in the SQS queue.")
                break

            for message in messages:
                receipt_handle = message['ReceiptHandle']
                message_body = json.loads(message['Body'])

                school = message_body['school']
                subject = message_body['subject']
                s3_path = message_body['s3_path']

                logging.info(f"Processing message: {message_body}")

                # Download file from S3
                local_file_path = download_file_from_s3(s3_path)

                # Read the content of the downloaded file
                try:
                    with open(local_file_path, 'r') as file:
                        content = file.read()
                    logging.info(f"Read content from {local_file_path}")
                except Exception as e:
                    logging.error(f"Error reading file {local_file_path}: {str(e)}")
                    continue

                # Process the file content (generate summary and quiz)
                result = generate_summary_and_quiz(content)

                if result is None:
                    logging.error(f"Failed to generate summary and quiz for {local_file_path}")
                    continue

                # Save the result
                try:
                    save_result(school, subject, os.path.basename(local_file_path), result)
                    logging.info(f"Saved result for {local_file_path}")
                except Exception as e:
                    logging.error(f"Error saving result for {local_file_path}: {str(e)}")
                    continue

                # Delete the message from SQS queue
                try:
                    sqs_manager.delete_message(receipt_handle)
                    logging.info(f"Deleted message with receipt handle: {receipt_handle}")
                except Exception as e:
                    logging.error(f"Error deleting SQS message: {str(e)}")
                    continue

                # Clean up the downloaded file
                try:
                    os.remove(local_file_path)
                    logging.info(f"Deleted local file {local_file_path}")
                except Exception as e:
                    logging.error(f"Error deleting local file {local_file_path}: {str(e)}")

        except Exception as e:
            logging.error(f"Error processing messages: {str(e)}")
            break
