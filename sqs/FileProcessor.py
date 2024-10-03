import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
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
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1000
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Error in OpenAI API call: {str(e)}")
        return None

def save_result(school: str, subject: str, filename: str, result: dict):
    timestamp = '_'.join(filename.split("_")[:2])
    folder_path = os.path.join('Data', school, subject, timestamp)
    os.makedirs(folder_path, exist_ok=True)

    with open(os.path.join(folder_path, f"{timestamp}_result.json"), 'w') as f:
        json.dump(result, f, indent=2)
