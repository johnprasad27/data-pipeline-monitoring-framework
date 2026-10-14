import sys
sys.path.insert(0, '.')
import json
from database.db import Database

db = Database()
db.connect()

# Query pipeline_registry for customer_pipeline_2
reg = db.fetch_all("SELECT pipeline_name, pipeline_type, source_config, interval_seconds, max_retries, enabled FROM pipeline_registry")
print("ALL REGISTRY ROWS:")
for r in reg:
    print(r["pipeline_name"], r["pipeline_type"], json.dumps(r["source_config"], indent=2))

exec_names = db.fetch_all("SELECT DISTINCT pipeline_name FROM pipeline_execution")
print("\nDISTINCT PIPELINE NAMES IN EXECUTION:")
for e in exec_names:
    print(e["pipeline_name"])

latest_execs = db.fetch_all("""
    SELECT execution_id, pipeline_name, source_type, status, rows_processed, 
           error_message, failure_type, failure_category, retryable, 
           data_quality_score, data_quality_status
    FROM pipeline_execution 
    ORDER BY execution_id DESC 
    LIMIT 10
""")
print("\nLATEST 10 EXECUTIONS:")
for e in latest_execs:
    print(e["execution_id"], e["pipeline_name"], e["status"], repr(e["error_message"]))
print("\nEXECUTIONS:")
for e in execs:
    print("Execution ID:", e["execution_id"])
    print("Status:", e["status"])
    print("Rows:", e["rows_processed"])
    print("Error Message:", e["error_message"])
    print("Failure Category:", e["failure_category"])
    print("Quality Status:", e["data_quality_status"])
    print("Execution Log:", e.get("execution_log"))
    print("-" * 40)

db.close()
