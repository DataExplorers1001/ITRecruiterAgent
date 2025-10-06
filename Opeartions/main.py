#mail response APIs

from fastapi import FastAPI, Query
import pyodbc
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

sql_conn_str = os.getenv("sql_conn_str")

app = FastAPI()


# DB connection
def get_db_connection():
    conn = pyodbc.connect(sql_conn_str)
    return conn


@app.get("/respond")
async def respond(candidateId: int, jobId: int, email: str, response: str):
    is_interested = 1 if response.lower() == "yes" else 0

    # Use context manager
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE datCandidateMailInfo
                SET IsResponded = 1,
                    IsInterested = ?,
                    RespondedDate = GETDATE()
                WHERE CandidateID = ? AND JobID = ? and Email = ?;
            """, (is_interested, candidateId, jobId,email))
            conn.commit()

    return {"message": f"Thanks! Your response '{response}' is recorded."}