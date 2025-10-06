import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from openai import OpenAI
import os
from dotenv import load_dotenv
import pyodbc
from datetime import datetime
from azure.search.documents.models import VectorizedQuery
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
import json
from openai import AzureOpenAI

load_dotenv()


# Gmail SMTP setup
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")
GMAIL_USER =os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


#ai search
ENDPOINT = os.getenv("search_ENDPOINT")
API_KEY = os.getenv("search_API_KEY")  
INDEX_NAME =os.getenv("search_INDEX_NAME")


#for embedding
em_endpoint =os.getenv("em_endpoint")
em_deployment_name =os.getenv("em_deployment_name")
em_api_key =os.getenv("em_api_key")


#llm object
endpoint = os.getenv("llm_endpoint")
model_name = os.getenv("llm_model_name")
deployment = os.getenv("llm_deployment")
api_key = os.getenv("llm_api_key")

#db
sql_conn_str = os.getenv("sql_conn_str")



#db
conn = pyodbc.connect(sql_conn_str)

#openAI client object
client = OpenAI(
    base_url = em_endpoint,
    api_key = em_api_key,
) 

#embedding generation
def generate_embedding(text):
    response = client.embeddings.create(
        model=em_deployment_name,
        input=text
    )
    return response.data[0].embedding

#ai seach object
Search_client = SearchClient(
    endpoint=ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(API_KEY)
)



# Azure OpenAI client
llm_client = AzureOpenAI(
    api_version="2024-12-01-preview",
    azure_endpoint=endpoint,
    api_key=api_key,
)

# DB connection
def get_db_connection():
    conn = pyodbc.connect(sql_conn_str)
    return conn


job_id = 4    #which job to search & mail

#getch job title
def get_job_title(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT Title,Description
        FROM defJobs
        WHERE JobID = ?
    """, job_id)
    
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {"title": row[0], "description": row[1]}
    else:
        return None

#usage
job_title = ''
JDText = ''
job_data = get_job_title(job_id)
if job_data:
    job_title = (job_data["title"])
    JDText = (job_data["description"])
    
print(job_title)
companyName = 'XYZ pvt ltd...'

#dynamic query for seacch via LLM

def parse_query(user_query: str):
    system_prompt = """You are a query parser for candidate search.
    Extract structured filters from the user query.
    Output JSON with fields:
    - search_text: string (role)
    - experienceYears: string like 'eq 3', 'ge 5', 'le 2' or null
    - location: string or null
    """

    response = llm_client.chat.completions.create(
        model=deployment,   # use your deployment name here
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        temperature=0
    )

    content = response.choices[0].message.content

    try:
        parsed = json.loads(content)
    except:
        parsed = {"search_text": user_query, "experienceYears": None, "location": None}

    return parsed


#query to filter based on exp and location if
user_input = "I want a Content writer in Pune"

parsed = parse_query(user_input)

print(parsed)

# Build filter
filters = []
if parsed.get("experienceYears"):
    filters.append(f"experienceYears {parsed['experienceYears']}")
if parsed.get("location"):
    filters.append(f"location eq '{parsed['location']}'")

filter_query = " and ".join(filters) if filters else None

job_vector = generate_embedding(JDText)
vector_query = VectorizedQuery(vector=job_vector, fields="resumeVector")

results = Search_client.search(
    search_text=parsed["search_text"],
    vector_queries=[vector_query],
    filter=filter_query,
    top=1,
    select=["id","name","experienceYears","location","email"]
)



# Convert iterator to list as it is once time a
candidates_list = list(results)
# First loop: print
print("Selected Candidates...\n")
for candidate in candidates_list:
    print(f"ID = {candidate['id']}, Name: {candidate['name']}, Email: {candidate['email']}, "
          f"Exp: {candidate['experienceYears']}, Location: {candidate['location']}, "
          f"Score: {candidate['@search.score']:.4f}")

print("DB connection est... \n")
cursor = conn.cursor()
# Second loop: send emails


def mailSend():
    print("Starting mail loop...\n")
    for candidate in candidates_list:
        print(f"Sending email to {candidate['name']} ...")
        yes_link = f"http://127.0.0.1:8000/respond?candidateId={candidate['id']}&response=yes&jobId={job_id}&email={candidate['email']}"
        no_link = f"http://127.0.0.1:8000/respond?candidateId={candidate['id']}&response=no&jobId={job_id}&email={candidate['email']}"

        # Create email
        msg = MIMEMultipart("alternative")
        msg["From"] = GMAIL_USER
        msg["To"] = candidate["email"]
        msg["Subject"] = f"Job Opportunity - {job_title}"

        html_content = f"""
        <p>Dear {candidate['name']},</p>
        <p>We hope this message finds you well.</p>
        <p>We are pleased to invite you to explore a career opportunity with {companyName} for the position of {job_title}.
        Based on your background and skills, we believe you could be a strong fit for this role.</p>
        <p>
            If you are interested: <a href="{yes_link}">YES</a><br>
            If not interested: <a href="{no_link}">NO</a>
        </p>
        <p>Best regards,<br>{companyName}</p>
        """

        text_content = f"Hi {candidate['name']}, Are you interested? Yes: {yes_link} | No: {no_link}"

        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        # Send email
        print("Connecting to SMTP server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, candidate["email"], msg.as_string())
            print(f"Email sent to {candidate['email']}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            MERGE datCandidateMailInfo AS target
            USING (SELECT ? AS CandidateID, ? AS JobID, ? AS Email) AS source
            ON target.CandidateID = source.CandidateID AND target.JobID = source.JobID and target.Email = source.Email
            WHEN MATCHED THEN
                UPDATE SET IsMailSent = 1
            WHEN NOT MATCHED THEN
                INSERT (CandidateID, Email, JobID, IsMailSent)
                VALUES (source.CandidateID, source.Email, source.JobID, 1);
        """, (candidate["id"], job_id, candidate["email"]))
        conn.commit()
        conn.close()

mailSend()
print("Closing DB connection...\n")    



