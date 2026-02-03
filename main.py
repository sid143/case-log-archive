from fastapi import FastAPI
from typing import List
import psycopg2
from psycopg2.extras import execute_batch
from tenacity import retry, stop_after_attempt, wait_fixed
import os
app = FastAPI()
DB_CONFIG = {
   "host": os.environ["DB_HOST"],
   "dbname": "caselog_archive",
   "user": "postgres",
   "password": os.environ["DB_PASSWORD"],
   "port": 5432
}
def get_connection():
   return psycopg2.connect(**DB_CONFIG)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def insert_records(records):
   conn = get_connection()
   cur = conn.cursor()
   sql = """
   INSERT INTO caselog_archive (
       sf_id,
       name,
       case_id,
       comments,
       status,
       created_date,
       last_modified_date
   )
   VALUES (
       %(sf_id)s,
       %(name)s,
       %(case_id)s,
       %(comments)s,
       %(status)s,
       %(created_date)s,
       %(last_modified_date)s
   )
   ON CONFLICT (sf_id) DO NOTHING
   """
   execute_batch(cur, sql, records)
   conn.commit()
   cur.close()
   conn.close()
@app.post("/archive/case-logs")
def archive_case_logs(payload: List[dict]):
   failed = []
   for record in payload:
       try:
           insert_records([record])
       except Exception as e:
           failed.append({
               "sf_id": record.get("sf_id"),
               "error": str(e)
           })
   return {
       "received": len(payload),
       "inserted": len(payload) - len(failed),
       "failed": failed
   }