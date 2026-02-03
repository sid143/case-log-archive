import os
from typing import List, Optional
import psycopg2
from psycopg2.extras import execute_batch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
app = FastAPI()
# -------------------------
# Database Configuration
# -------------------------
DB_CONFIG = {
   "host": os.environ.get("DB_HOST"),
   "user": os.environ.get("DB_USER", "postgres"),
   "password": os.environ.get("DB_PASSWORD"),
   "port": int(os.environ.get("DB_PORT", 5432)),
   "dbname": os.environ.get("DB_NAME"),
}
def get_connection():
   return psycopg2.connect(
       **DB_CONFIG,
       sslmode="require"
   )
# -------------------------
# Pydantic Model
# -------------------------
class CaseLogIn(BaseModel):
   sf_id: str
   caseId: str
   name: str
   comments: Optional[str] = None
# -------------------------
# Insert Logic (No Retry)
# -------------------------
def insert_records(records: List[CaseLogIn]) -> None:
   conn = None
   cur = None
   try:
       conn = get_connection()
       cur = conn.cursor()
       sql = """
       INSERT INTO public.caselog_archive
       (sf_id, case_id, name, comments)
       VALUES (%s, %s, %s, %s)
       """
       data = [
           (r.sf_id, r.caseId, r.name, r.comments)
           for r in records
       ]
       execute_batch(cur, sql, data, page_size=100)
       conn.commit()
   except psycopg2.Error:
       if conn:
           conn.rollback()
       raise
   finally:
       if cur:
           cur.close()
       if conn:
           conn.close()
# -------------------------
# API Endpoint
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
# Health Check
# -------------------------
@app.get("/")
def health():
   return {"status": "ok"}
