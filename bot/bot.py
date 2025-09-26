import requests
import pyodbc
from azure.storage.blob import BlobServiceClient
import os
import re
from dotenv import load_dotenv

load_dotenv()

load_dotenv()

# --- Azure Language Service Config ---
endpoint = os.getenv("Language_endpoint")
key = os.getenv("Language_key")
deployment_name =os.getenv("Language_deployment_name")
project_name = os.getenv("Language_project_name")
api_version = os.getenv("Language_api_version")


# --- Azure Blob Config ---
blob_conn_str = os.getenv("blob_conn_str")
container_name = os.getenv("blob_container_name")


url = f"{endpoint}language/:analyze-conversations?projectName={project_name}&deploymentName={deployment_name}&api-version={api_version}"

#DB connection
sql_conn_str = os.getenv("sql_conn_str")


#getting the intent to users query
def get_intent(user_input):
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Content-Type": "application/json"
    }
    body = {
        "kind": "Conversation",
        "analysisInput": {
            "conversationItem": {
                "id": "1",
                "text": user_input,
                "modality": "text",
                "participantId": "user"
            }
        },
        "parameters": {
            "projectName": project_name,
            "deploymentName": deployment_name,
            "stringIndexType": "Utf16CodeUnit"
        }
    }

    url = f"{endpoint}language/:analyze-conversations?projectName={project_name}&deploymentName={deployment_name}&api-version=2023-04-01"
    response = requests.post(url, headers=headers, json=body)
    result = response.json()
    # print(result)

    prediction = result["result"]["prediction"]
    intent = prediction["topIntent"]

    # find confidence score of the topIntent
    confidence = 0.0
    for i in prediction["intents"]:
        if i["category"] == intent:
            confidence = i["confidenceScore"]
            break

    entities = prediction.get("entities", [])
    return intent, confidence, entities


def fetch_job_by_id_or_title(job_input):
    """
    Fetch a job from the database using JobID (number) or JobTitle (string)
    Returns a job object with JobID, Title, Location, or None if not found
    """
    conn = pyodbc.connect(sql_conn_str)
    cursor = conn.cursor()

    # Check if input is numeric (JobID)
    if job_input.isdigit():
        cursor.execute(
            "SELECT JobID, Title, Location FROM defJobs WHERE JobID = ? AND IsActive=1",
            (int(job_input),)
        )
    else:  # Treat as JobTitle
        cursor.execute(
            "SELECT JobID, Title, Location FROM defJobs WHERE Title LIKE ? AND IsActive=1",
            (f"%{job_input}%",)
        )

    job = cursor.fetchone()
    conn.close()
    return job  

def validate_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def validate_phone(phone):
    return re.match(r"^\+?\d{7,15}$", phone)


def bot_loop():
    print("🤖 Bot started! Type 'exit' to quit.")
    session = {"last_job": None}
    
    while True:
        user_input = input("You: ")
        if user_input.lower() == "exit":
            break

        intent, confidence, entities = get_intent(user_input)
        print(f"Detected Intent: {intent} (confidence {confidence:.2f})")

        # Extract Job entity if exists
        job_entity = None
        for entity in entities:
            if entity["category"] == "Job":
                job_entity = entity["text"]
                break

        if intent == "Greeting":
            print("Bot: Hello! How can I help you today?")

        elif intent in ["JobDetails", "JobSearch"]:
            jobs = fetch_jobs()
            if jobs:
                print("Bot: Here are the available jobs:")
                for job in jobs:
                    print(f"- {job.JobID}: {job.Title} : {job.Location}")
            else:
                print("Bot: Sorry, no active job openings found.")
                
        elif intent == "ApplyJob":
            job = None
            job_entity = None
            for ent in entities:
                if ent["category"].lower() in ["jobid", "jobtitle", "jobprofile"]:
                    job_entity = ent["text"].strip()

            if job_entity:
                job = fetch_job_by_id_or_title(job_entity)
            elif session.get("last_job"):
                job = session["last_job"]

            if job:
                print(f"Bot: Great! Applying for {job.Title} (JobID: {job.JobID}). Let's collect your details.")

                # Collect and validate info
                while True:
                    name = input("👉 Please enter your full name: ")
                    if name.strip(): break
                    print("Bot: Name cannot be empty.")

                while True:
                    email = input("👉 Please enter your email: ")
                    if validate_email(email): break
                    print("Bot: Please enter a valid email address.")

                while True:
                    phone = input("👉 Please enter your phone: ")
                    if validate_phone(phone): break
                    print("Bot: Please enter a valid phone number (+countrycode optional).")

                file_path = input("👉 Please provide path to your resume file: ")
                file_name = os.path.basename(file_path)

                # Remove spaces
                file_name_no_spaces = file_name.replace(" ", "")
                resume_url = upload_resume(file_name_no_spaces, name)
                if resume_url:
                    save_application(job.JobID, name, email, phone, resume_url)
                    print(f"Bot: 🎉 Your application for {job.Title} has been submitted successfully!")
                else:
                    print("Bot: Resume upload failed, please try again.")
            else:
                print("Bot: Please tell me the job you want to apply for (title or ID).")

        elif intent == "Thanks":
            print("Bot: You’re welcome! Happy to help 🙂")

        elif intent == "Goodbye":
            print("Bot: Goodbye! Have a great day 👋")
            break

        elif intent == "Help":
            print("Bot: I can help you with:\n1. Show job openings\n2. Apply for a job\n3. Greetings and goodbyes")

        else:
            print("Bot: I'm not sure I understand. Can you rephrase?")


def fetch_jobs():
    try:
        conn = pyodbc.connect(sql_conn_str)
        cursor = conn.cursor()
        cursor.execute("select JobID,Title, Location from [dbo].[defJobs] WHERE IsActive = 1")
        jobs = cursor.fetchall()
        conn.close()
        return jobs
    except Exception as e:
        print("⚠️ DB Error:", e)
        return []

# def upload_resume(file_path: str, candidate_name: str):
#     try:
#         file_name = os.path.basename(file_path)
#         blob_name = f"{candidate_name}_{file_name}"
#         blob_client = container_client.get_blob_client(blob_name)

#         with open(file_path, "rb") as data:
#             blob_client.upload_blob(data, overwrite=True)

#         return blob_client.url
#     except Exception as e:
#         print("⚠️ Blob upload failed:", e)
#         return None

#this is currently path based
def upload_resume(file_name: str, candidate_name: str):
    """
    Upload file_data (bytes) as file_name to blob; return accessible URL (or local placeholder in mock).
    """
    try :
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        blob_client.upload_blob(file_name, overwrite=True)
        return blob_client.url
    except Exception as e:
        print("Failed")
        return None

def save_application(job_id, name, email, phone, resume_url):
    try:
        conn = pyodbc.connect(sql_conn_str)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO datCandidateApplications ( Name, Email, Phone,JobID, ResumeUrl)
            VALUES (?, ?, ?, ?, ?)
        """, ( name, email, phone,job_id, resume_url))
        conn.commit()
        conn.close()
        print("✅ Application saved in DB")
    except Exception as e:
        print("⚠️ DB Insert Error:", e)

if __name__ == "__main__":
    bot_loop()
