import os
import time
import logging
from typing import List, Optional
import psycopg2
from psycopg2.extras import execute_batch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
# -------------------------
# App & Logging
# -------------------------
app = FastAPI()
logging.basicConfig(
   level=logging.INFO,
   format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
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
   caseId: Optional[str] = None
   name: str
   comments: Optional[str] = None
# -------------------------
# UPSERT Logic + Metrics
# -------------------------
def upsert_records(records: List[CaseLogIn]) -> dict:
   conn = None
   cur = None
   cpu_start = time.process_time()
   wall_start = time.perf_counter()
   try:
       conn = get_connection()
       cur = conn.cursor()
       db_start = time.perf_counter()
       sql = """
       INSERT INTO caselog_archive
       (sf_id, case_id, name, comments)
       VALUES (%s, %s, %s, %s)
       ON CONFLICT (sf_id)
       DO UPDATE SET
           case_id = EXCLUDED.case_id,
           name = EXCLUDED.name,
           comments = EXCLUDED.comments;
       """
       data = [
           (r.sf_id, r.caseId, r.name, r.comments)
           for r in records
       ]
       execute_batch(cur, sql, data, page_size=100)
       conn.commit()
       db_end = time.perf_counter()
       cpu_end = time.process_time()
       wall_end = time.perf_counter()
       metrics = {
           "records_processed": len(records),
           "cpu_time_ms": round((cpu_end - cpu_start) * 1000, 2),
           "db_time_ms": round((db_end - db_start) * 1000, 2),
           "total_time_ms": round((wall_end - wall_start) * 1000, 2),
       }
       logger.info(f"UPSERT SUCCESS | {metrics}")
       return metrics
   except psycopg2.Error as e:
       if conn:
           conn.rollback()
       logger.error(f"DB ERROR | {str(e)}")
       raise e
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
       metrics = upsert_records(records)
       return {
           "status": "success",
           **metrics
       }
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))
# -------------------------
# Health Check
# -------------------------
@app.get("/")
def health():
   return {"status": "ok"}
