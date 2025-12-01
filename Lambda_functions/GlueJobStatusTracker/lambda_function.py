import json
import boto3
import os
from datetime import datetime
import time

glue_client = boto3.client("glue")


# Define required parameters
REQUIRED_PARAMS = [
    "glue_job_name",
    "database_name",
    "connection_name",
    "thread_count",
    "source_type",
    "sf_schema",
    "jdbc_url"
]

def lambda_handler(event, context):
    max_wait = 13 * 60
    wait_interval = 30  # check every 30 seconds
    elapsed = 10
    print("Received event:", event)

    # Parse JSON body from API Gateway / Postman
    try:
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)
        elif body is None:
            body = {}
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Invalid JSON body: {str(e)}"})
        }

    # Validate required parameters
    missing_params = [param for param in REQUIRED_PARAMS if param not in body or body[param] == ""]
    if missing_params:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": f"Missing required parameters: {', '.join(missing_params)}"
            })
        }

    # Prepare Step Function input
    step_input = {"extraParams": body}

    try:
        # Start Glue job
        response = glue_client.start_job_run(
            JobName=body["glue_job_name"],
            Arguments={
                "--database_name": body["database_name"],
                "--connection_name": body["connection_name"],
                "--thread_count": body["thread_count"],
                "--source_type": body["source_type"],
                "--sf_schema": body["sf_schema"],
                "--jdbc_url": body["jdbc_url"]

            }
        )
        run_id = response["JobRunId"]
        print(f"Glue Job started. RunId: {run_id}")


        # Poll Glue job status
        while elapsed < max_wait:
            status_resp = glue_client.get_job_run(JobName=body["glue_job_name"], RunId=run_id, PredecessorsIncluded=False)
            status = status_resp["JobRun"]["JobRunState"]
            print(f"Current status: {status}")
            if status == "SUCCEEDED":
                print("Glue Job succeeded")
                break
            elif status in ["FAILED", "STOPPED", "TIMEOUT"]:
                raise Exception(f"Glue Job ended with status: {status}")
            time.sleep(wait_interval)  # wait 30 seconds before next poll
            elapsed += wait_interval

        print(f"Glue Job finished with status: {status}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Glue job finished",
                "glueJobName": body["glue_job_name"],
                "runId": run_id,
                "status": status
            })
        }

    except Exception as e:
        print(f"Error while starting Glue Function: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": f"Error while starting Glue Function: {str(e)}"
            })
        }