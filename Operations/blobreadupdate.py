import requests
import pdfplumber
from io import BytesIO
import pyodbc  
import os
from dotenv import load_dotenv

load_dotenv()

sql_conn_str = os.getenv("sql_conn_str")

# ----- DB Connection -----
def get_db_connection():
    return pyodbc.connect(sql_conn_str)



# ----- Build Full SAS URL -----
def build_resume_url(file_name, storage_account, container_name, sas_token):
    return f"https://{storage_account}.blob.core.windows.net/{container_name}/{file_name}?{sas_token}"

# ----- Parse PDF and return text -----
def parse_pdf_from_url(resume_url):
    response = requests.get(resume_url)
    response.raise_for_status()
    pdf_file = BytesIO(response.content)
    text = ""
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
    except Exception as e:
        print("Failed to parse PDF:", resume_url, e)
    return text

# ----- Main Batch Processing -----
def process_all_resumes(storage_account, container_name, sas_token):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Fetch all records that don't have parsed text yet
    cursor.execute("SELECT ApplicationID, ResumeURL FROM datCandidateApplications WHERE ResumeTxt IS NULL")
    rows = cursor.fetchall()

    for row in rows:
        app_id = row[0]
        file_name = row[1]  # this could be full URL or just file name

        # If only file name, construct full URL
        if not file_name.startswith("https://"):
            resume_url = build_resume_url(file_name, storage_account, container_name, sas_token)
        else:
            resume_url = f"{file_name}?{sas_token}"

        # Parse PDF
        print(f"Processing ApplicationID: {app_id}")
        text = parse_pdf_from_url(resume_url)

        # print(text)

        # Update DB
        cursor.execute("UPDATE datCandidateApplications SET ResumeTxt = ? WHERE ApplicationID = ?", text, app_id)
        conn.commit()

    conn.close()
    print("All resumes processed!")

# ----- Usage -----
storage_account = os.getenv("AccountName")
container_name = os.getenv("blob_container_name")
sas_token = os.getenv("SAStoken")

process_all_resumes(storage_account, container_name, sas_token)
