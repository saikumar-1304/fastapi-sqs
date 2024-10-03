import os
from fastapi import FastAPI, Form, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from SQS_Manager import SQSManager
from QueueProcessor import process_sqs_messages

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize SQS manager
sqs_manager = SQSManager(queue_url=os.getenv('SQS_QUEUE_URL'))

@app.post("/process_files")
async def process_files(
    background_tasks: BackgroundTasks,
    school: str = Form(...),
    subject: str = Form(...),
    s3_path: str = Form(...)
):
    if not s3_path:
        raise HTTPException(status_code=400, detail="No S3 path provided")

    # Prepare message for the queue
    sqs_message = {
        "school": school,
        "subject": subject,
        "s3_path": s3_path
    }

    # Send the message to the queue
    sqs_manager.send_message(sqs_message)

    # Trigger background task to process the queue
    background_tasks.add_task(process_sqs_messages, os.getenv('SQS_QUEUE_URL'))

    return JSONResponse(content={"message": "S3 object has been added to the queue for processing."})

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)