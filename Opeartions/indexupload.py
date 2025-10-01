from openai import OpenAI
import pyodbc
import pandas as pd
import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from dotenv import load_dotenv

load_dotenv()

#embeddings
endpoint = os.getenv("em_endpoint")
deployment_name = os.getenv("em_deployment_name")
api_key = os.getenv("em_api_key")

#ai search
ENDPOINT = os.getenv("search_ENDPOINT")
API_KEY = os.getenv("search_API_KEY")
INDEX_NAME = os.getenv("search_INDEX_NAME")


#dbconnection
sql_conn_str = os.getenv("sql_conn_str")

#db connection est
conn = pyodbc.connect(sql_conn_str)

#openAI client object
client = OpenAI(
    base_url = endpoint,
    api_key = api_key,
) 

#embedding generation
def generate_embedding(text):
    response = client.embeddings.create(
        model=deployment_name,
        input=text
    )
    return response.data[0].embedding


# Query candidates  - hardcoded candidate
query = """
        SELECT ApplicationID
            ,Name
            ,Email
            ,Phone
            ,JobID
            ,ResumeTxt
            ,ResumeURL
        FROM datCandidateApplications
        """
df_candidates = pd.read_sql(query, conn)  #df of candidates
conn.close()

print(df_candidates.head())
df_candidates['resumeVector'] = df_candidates['resume_text'].apply(generate_embedding)


#seach client object
client = SearchClient(
    endpoint=ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(API_KEY)
)


#to upload candidate info in index acc to df (sql query)
docs = []
def upload_candidates():
    for _, row in df_candidates.iterrows():
        docs.append({
            "id": str(row['ApplicationID']),
            "name": row['Name'],
            "email": row['Email'],
            #"location": row['location'],
            #dont have job_category
            #"experienceYears": row['experience_years'],
            "resumeText": row['ResumeTxt'],
            "resumeVector": row['resumeVector']
        })
        print(row['column1'])
        print('#'*25)
    client.upload_documents(documents=docs)
    print("Uploaded candidates successfully!")

print("Functin calling...")
upload_candidates()
    
