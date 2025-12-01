import requests
import json
from snowflake.snowpark import Session
from concurrent.futures import ThreadPoolExecutor, as_completed

def test_package():
    print("package is working fine")

def run(glue_job_name, database_name, connection_name,thread_count,source_type,sf_schema,jdbc_url):
    # Primary Lambda function URL
    main_url = "https://dlyrgca723ixrtmda4tsueqkeq0yeabm.lambda-url.ap-south-1.on.aws/"
    fallback_url = "https://b2u2z34dxz3mylxlw4yce2rvz40lnvbr.lambda-url.ap-south-1.on.aws/"

    # Payload for main Lambda
    payload = {
        "glue_job_name": glue_job_name,
        "database_name": database_name,
        "connection_name": connection_name,
        "thread_count": thread_count,
        "source_type": source_type,
        "sf_schema": sf_schema,
        "jdbc_url": jdbc_url
    }

    headers = {"Content-Type": "application/json"}

    # --- Step 1: Trigger main Lambda ---
    response = requests.post(main_url, json=payload, headers=headers)
    print(f"Primary Lambda response: {response.text}")

    try:
        resp_json = response.json()
    except Exception:
        resp_json = {}

    # Extract 'status' from response (assuming your Lambda returns it)
    status = resp_json.get("status", "").upper()

    # --- Step 2: Check status and trigger fallback if not SUCCEEDED ---
    if status != "SUCCEEDED":
        fallback_payload = {
            "message": "Primary Lambda failed",
            "original_status": status,
            "response": resp_json
        }
        fb_resp = requests.post(fallback_url, json=fallback_payload, headers=headers)
        return f"Primary failed with status: {status}. Fallback triggered. Response: {fb_resp.status_code}, {fb_resp.text}"

    return f"Primary Lambda succeeded. Status: {status}, Response: {response.text}"
def call_merge_proc(session, schema_name, table_name, partition_col):
    """Helper function to call the merge procedure."""
    print(f"Calling procedure for {schema_name}.{table_name} (partition_col={partition_col})")
    result = session.call(
        "STAGGING.PROC.MERGE_INCREMENTAL_DATA",
        schema_name,
        table_name,
        partition_col
    )
    print(f" Completed: {schema_name}.{table_name}")
    return result
def process_tables(schema_name: str,type:str, max_workers: int = 5):
    """
    Fetch all table metadata for the given schema_name from Snowflake table,
    then call a stored procedure for each table in parallel.
    """
    session = Session.builder.getOrCreate()

    if type.lower()=='batch':
        query = f"""
        SELECT database_name, table_name, partition_col
        FROM STAGGING.CONFIG.PARTITION_TABLE
        WHERE database_name = '{schema_name}'
        """
    else:
        query = f"""
        SELECT database_name, table_name, partition_col
        FROM STAGGING.CONFIG.STREAM_PARTITION_TABLE
        WHERE database_name = '{schema_name}'
        """
   
    rows = session.sql(query).collect()

    if not rows:
        print(f"No tables found for schema_name: {schema_name}")
        return

    # Prepare list of parameters
    tasks = []
    for row in rows:
        database_name = row["DATABASE_NAME"].upper()
        table_name = row["TABLE_NAME"].upper()
        partition_col = row["PARTITION_COL"]
        tasks.append((database_name, table_name, partition_col))

    # Run parallel calls
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(call_merge_proc, session, db, tbl, col): (db, tbl)
            for db, tbl, col in tasks
        }

        for future in as_completed(futures):
            db, tbl = futures[future]
            try:
                result = future.result()
                print(f" {db}.{tbl} -> {result}")
            except Exception as e:
                print(f" Error processing {db}.{tbl}: {str(e)}")

    print("All parallel procedure calls completed.")


    