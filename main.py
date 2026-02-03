from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import execute_batch
from tenacity import retry, stop_after_attempt, wait_fixed
import os
app = FastAPI()
# -------------------------
# Database Configuration
# -------------------------
DB_CONFIG = {
   "host": os.environ["DB_HOST"],
   "dbname": os.environ.get("DB_NAME", "postgres"),
   "user": os.environ.get("DB_USER", "postgres"),
   "password": os.environ["DB_PASSWORD"],
   "port": int(os.environ.get("DB_PORT", 5432)),
}
def get_connection():
   return psycopg2.connect(
       **DB_CONFIG,
       sslmode="require"
   )
# -------------------------
# Salesforce Wrapper Model
# -------------------------
class CaseLogIn(BaseModel):
   caseId: str
   name: str
   comments: Optional[str] = None
# -------------------------
# Insert Logic (Retry Safe)
# -------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def insert_records(records: List[CaseLogIn]):
   conn = get_connection()
   cur = conn.cursor()
   sql = """
       INSERT INTO case_log_archive
       (case_id, name, comments)
       VALUES (%s, %s, %s)
       ON CONFLICT (case_id) DO NOTHING
   """
   data = [
       (r.caseId, r.name, r.comments)
       for r in records
   ]
   execute_batch(cur, sql, data, page_size=100)
   conn.commit()
   cur.close()
   conn.close()
# -------------------------
# API Endpoint (Salesforce)
# -------------------------
@app.post("/archive")
def archive_case_logs(records: List[CaseLogIn]):
   if not records:
       raise HTTPException(status_code=400, detail="No records received")
   try:
       insert_records(records)
       return {
           "status": "success",
           "received": len(records)
       }
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))
# -------------------------
# Health Check (Important)
# -------------------------
@app.get("/")
def health():
   return {"status": "ok"}
