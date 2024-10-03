import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging configuration
logging.basicConfig(
    filename='openai_summary_quiz.log',  # Log file location
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('API_KEY'))


def generate_summary_and_quiz(content: str):
    prompt = f"""
    Given the following lecture transcript, please provide:
    1. A concise summary of the lecture (max 100 words)
    2. 10 multiple-choice quiz questions based on the content

    Lecture transcript:
    {content}

    Please format your response as a JSON object with the following structure:
    {{
      "summary": "Your summary here",
      "quiz_questions": [
        {{
          "question": "Question 1",
          "options": ["Option A", "Option B", "Option C", "Option D"],
          "correct_answer": 0
        }},
        // ... more questions ...
      ]
    }}
    """
    try:
        logging.info("Sending request to OpenAI API for summary and quiz generation.")
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
        )
        result = json.loads(response.choices[0].message.content)
        logging.info("Successfully received response from OpenAI API.")
        return result
    except Exception as e:
        logging.error(f"Error in OpenAI API call: {str(e)}")
        return None


def save_result(school: str, subject: str, filename: str, result: dict):
    try:
        timestamp = '_'.join(filename.split("_")[:2])
        folder_path = os.path.join('Data', school, subject, timestamp)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logging.info(f"Created directory: {folder_path}")

        file_path = os.path.join(folder_path, f"{timestamp}_result.json")
        with open(file_path, 'w') as f:
            json.dump(result, f, indent=2)
        logging.info(f"Successfully saved result to {file_path}")
    except Exception as e:
        logging.error(f"Error saving result for {filename}: {str(e)}")
