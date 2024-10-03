import os
import json
import boto3
import logging
from dotenv import load_dotenv
from SQS_Manager import SQSManager
from FileProcessor import generate_summary_and_quiz

# Load environment variables
load_dotenv()

# Set up logging configuration
logging.basicConfig(
    filename='file_processor.log',  # Log file location
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

s3_client = boto3.client('s3')

def download_file_from_s3(s3_path, download_directory="downloads"):
    bucket_name, key = s3_path.replace("s3://", "").split("/", 1)

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    local_file_path = os.path.join(download_directory, key.split("/")[-1])

    try:
        logging.info(f"Downloading {s3_path} from S3 to {local_file_path}")
        s3_client.download_file(bucket_name, key, local_file_path)
        logging.info(f"Downloaded to {local_file_path}")
    except Exception as e:
        logging.error(f"Error downloading file from S3: {str(e)}")
        raise

    return local_file_path

def upload_file_to_s3(file_path, s3_target_path):
    bucket_name, key = s3_target_path.replace("s3://", "").split("/", 1)

    try:
        logging.info(f"Uploading {file_path} to {s3_target_path}")
        s3_client.upload_file(file_path, bucket_name, key)
        logging.info(f"Uploaded {file_path} to {s3_target_path}")
    except Exception as e:
        logging.error(f"Error uploading file to S3: {str(e)}")
        raise

def save_result_and_upload(school, subject, filename, result, s3_base_path):
    timestamp = '_'.join(filename.split("_")[:2])
    folder_path = os.path.join('Data', school, subject, timestamp)
    os.makedirs(folder_path, exist_ok=True)

    # Create summary file
    summary_file_path = os.path.join(folder_path, f"{timestamp}_summary.txt")
    quiz_file_path = os.path.join(folder_path, f"{timestamp}_quiz.txt")
    is_complete = False
    try:
        # Save the summary
        with open(summary_file_path, 'w') as f:
            f.write(result['summary'])
        logging.info(f"Summary saved to {summary_file_path}")

        # Save the quiz questions
        with open(quiz_file_path, 'w') as f:
            for question in result['quiz_questions']:
                f.write(f"Question: {question['question']}\n")
                for i, option in enumerate(question['options']):
                    f.write(f"  {chr(65 + i)}. {option}\n")  # A, B, C, D options
                f.write("\n")
            f.write("\nAnswers:\n")
            for i, question in enumerate(result['quiz_questions']):
                f.write(f"  Question {i+1}: {chr(65 + question['correct_answer'])}\n")  # Correct answer
        logging.info(f"Quiz saved to {quiz_file_path}")

        # Upload both files to S3
        summary_s3_path = f"{s3_base_path}/{timestamp}_summary.txt"
        quiz_s3_path = f"{s3_base_path}/{timestamp}_quiz.txt"

        upload_file_to_s3(summary_file_path, summary_s3_path)
        upload_file_to_s3(quiz_file_path, quiz_s3_path)
        is_complete = True
    except Exception as e:
        logging.error(f"Error saving or uploading files for {filename}: {str(e)}")

    return is_complete

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

                # Extract the base path from S3 path to upload the result files back
                base_s3_path = '/'.join(s3_path.split('/')[:-1])

                # Save the result and upload the files
                try:
                    is_complete = save_result_and_upload(school, subject, os.path.basename(local_file_path), result, f"s3://{base_s3_path}")
                    logging.info(f"Saved result for {local_file_path}")
                except Exception as e:
                    logging.error(f"Error saving result for {local_file_path}: {str(e)}")
                    continue

                # Delete the message from SQS queue
                if is_complete:
                    try:
                        sqs_manager.delete_message(receipt_handle)
                        logging.info(f"Deleted message with receipt handle: {receipt_handle}")
                    except Exception as e:
                        logging.error(f"Error deleting SQS message: {str(e)}")
                        continue

                # Clean up the downloaded file
                # try:
                #     os.remove(local_file_path)
                #     logging.info(f"Deleted local file {local_file_path}")
                # except Exception as e:
                #     logging.error(f"Error deleting local file {local_file_path}: {str(e)}")

        except Exception as e:
            logging.error(f"Error processing messages: {str(e)}")
            break
