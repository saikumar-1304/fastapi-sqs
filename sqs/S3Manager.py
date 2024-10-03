import os
import boto3
import threading
import time
from dotenv import load_dotenv

class S3UploadQueue:
    def __init__(self, region_name="ap-south-1"):
        load_dotenv()
        self.s3_queue = []
        self.lock = threading.Lock()
        self.s3_bucket_name = "eonpod-data"

        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("AWS credentials not found. Please check your .env file.")

        self.session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.s3_resource = self.session.resource("s3")
        self.s3_client = self.session.client("s3")
        self.processing_thread = threading.Thread(target=self.process_queue)
        self.processing_thread.daemon = True
        self.processing_thread.start()

    def create_folder_in_s3(self, folder_name):
        folder_key = folder_name if folder_name.endswith('/') else folder_name + '/'

        try:
            self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=folder_key)
            print(f"Folder '{folder_name}' created successfully in bucket '{self.s3_bucket_name}'")
        except Exception as e:
            print(f"Error creating folder in S3: {e}")

    def add_to_queue(self, school, subject, local_directory):
        with self.lock:
            self.s3_queue.append({
                "local_directory": local_directory,
                "school": school,
                "subject": subject,
            })
        print(f"Added to queue: {local_directory} for {subject}")

    def upload_file(self, local_file_path, school, subject, timestamp):
        file_name = os.path.basename(local_file_path)
        s3_object_key = f"{school}/{subject}/{timestamp}/{file_name}"

        try:
            self.s3_client.upload_file(Filename=local_file_path, Bucket=self.s3_bucket_name, Key=s3_object_key)
            print(f"Uploaded {local_file_path} to s3://{self.s3_bucket_name}/{s3_object_key}")
        except Exception as e:
            print(f"Error uploading file {local_file_path}: {e}")

    def count_files_and_upload(self, local_directory, school, subject):
        timestamp = os.path.basename(local_directory)
        self.create_folder_in_s3(folder_name=f"{school}/{subject}/{timestamp}")

        for root, _, files in os.walk(local_directory):
            for file in files:
                local_file_path = os.path.join(root, file)
                self.upload_file(local_file_path, school, subject, timestamp)

    def process_queue(self):
        while True:
            with self.lock:
                if not self.s3_queue:
                    time.sleep(10)
                    continue

                current_task = self.s3_queue.pop(0)

            try:
                local_directory = current_task["local_directory"]
                school = current_task["school"]
                subject = current_task["subject"]
                print(f"Starting S3 upload for {local_directory}")
                self.count_files_and_upload(local_directory, school, subject)
                print("Finished Uploading Files")
            except Exception as e:
                print(f"Error processing S3 upload for {current_task['local_directory']}: {str(e)}")

            time.sleep(20)

    def download_file_from_s3(self, s3_path, local_directory="downloads"):
        s3_url = s3_path.replace("s3://", "")
        bucket_name, object_key = s3_url.split("/", 1)

        if not os.path.exists(local_directory):
            os.makedirs(local_directory)

        local_file_path = os.path.join(local_directory, os.path.basename(object_key))

        try:
            print(f"Downloading {object_key} from bucket {bucket_name} to {local_file_path}")
            self.s3_client.download_file(Bucket=bucket_name, Key=object_key, Filename=local_file_path)
            print(f"Downloaded file to {local_file_path}")
        except Exception as e:
            print(f"Error downloading file from S3: {str(e)}")

        return local_file_path