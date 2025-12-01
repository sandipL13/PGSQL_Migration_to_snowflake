#Main Glue Job Script for MySQL and postgresql to S3 ETL with Multi-threading and CDC Support

import sys
import boto3
import json
import pytz
import traceback
from pyspark.sql import Row
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, max as sp_max, greatest, coalesce,current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import requests
import json


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secret_name = "snowflake"         
region_name = "ap-south-1"
secret = get_secret(secret_name, region_name)
sf_user = secret["USERNAME"]
sf_password = secret["PASSWORD"]

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sf_url = "vdqiiha-zc94797.snowflakecomputing.com"
sf_database = "STAGGING"
sf_schema = "CONFIG"  # <-- Dynamic from Glue job parameter
sf_warehouse = "DEV_CITADEL_WH"
sf_role = "DEV_ETL_DEVOPS"
# ------------------------------------------
# Logging
# ------------------------------------------
logger = glueContext.get_logger()


ist = pytz.timezone("Asia/Kolkata")
current_time = datetime.now(ist)
load_date = current_time.strftime("%Y-%m-%d")
load_hour = current_time.strftime("%H")

BUCKET_NAME = "ohl-ganit"  # Your S3 bucket
LOG_PREFIX = "glue-logs"

# ------------------------------------------
# DynamoDB metadata table for CDC tracking
# ------------------------------------------\
STREAM_LOAD_TYPE_TABLE='STREAM_TABLE_LOAD_TYPE'
LOAD_TYPE_TABLE='TABLE_LOAD_TYPE'
session = boto3.Session(region_name="ap-south-1")
dynamodb = session.resource("dynamodb")
stream_load_type_table = dynamodb.Table(STREAM_LOAD_TYPE_TABLE)
load_type_table = dynamodb.Table(LOAD_TYPE_TABLE)



def init_s3_logger(bucket_name, component_name, level=logging.INFO):
    """Initialize logger that logs to S3"""
    log_stream = StringIO()
    logger_obj = logging.getLogger(f"{component_name}")
    logger_obj.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in logger_obj.handlers[:]:
        logger_obj.removeHandler(handler)

    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger_obj.addHandler(handler)

    logger_obj.info(f"Logger initialized for {component_name}")
    return logger_obj, log_stream
def upload_logs_to_s3(log_stream, bucket_name, component_name):
    """Upload logs to S3"""
    try:
        s3 = boto3.client("s3")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        s3_key = f"{LOG_PREFIX}/{component_name}/{timestamp}/app.log"

        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=log_stream.getvalue().encode("utf-8"),
        )
        print(f"✅ Logs uploaded to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload logs: {str(e)}")
        return False

# ------------------------------------------
# Fetch load type mapping from Snowflake
# ------------------------------------------
def scan_table(table,component_logger):
   
    try:
        print(f"Fetching  from DynamoDB: {table}")
        # Scan the table
        response = table.scan()
        items = response.get("Items", [])

        # Handle pagination (in case of large data)
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))

        # Create the map
        result = []
        for item in response.get('Items', []):
            database_name=item.get("database_name","").strip() or None
            table_name = item.get("table_name","").strip() or None
            partition_col = item.get("partition_col", "").strip() or None
            result.append((database_name,table_name, partition_col))

        print(f"Done:get_table-{len(result)}")
        return result

    except Exception as e:
        print(f"Failed to fetch load type metadata from DynamoDB: {str(e)}")
        traceback.print_exc()
        #send_slack_notification(DATABASE_NAME, "failed")
        sys.exit(1)


def send_slack_notification(DATABASE_NAME, status):
    webhook_url = "https://hooks.slack.com/services/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" 
    if status.lower() == "success":
        color = "#36a64f"  # Green
        message = f":white_check_mark: *{DATABASE_NAME}* completed successfully!"
    else:
        color = "#ff0000"  # Red
        message = f":x: *{DATABASE_NAME}* failed. Please check the logs."

    payload = {
        "attachments": [
            {
                "color": color,
                "title": "Job Notification",
                "text": message
            }
        ]
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        print(f"Failed to send notification: {response.text}")
    else:
        print("Notification sent successfully.")

def update_snowflake_table(table_name,results,component_logger):
    try:
        schema = StructType([
            StructField("database_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("partition_col", StringType(), True)
        ])
        df = spark.createDataFrame(results, schema)
        sfOptions = {
        "sfURL": sf_url,
        "sfDatabase": sf_database,
        "sfSchema": sf_schema,
        "sfWarehouse": sf_warehouse,
        "sfRole": sf_role,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        }
        df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", table_name.upper()).mode("overwrite").save()
    except Exception as e:
        logger.error(f"[FAILED] Table {table_name}: {str(e)}")
        traceback.print_exc()
        print(f"Error")
        #send_slack_notification(DATABASE_NAME, "failed")
        raise e
        


# ------------------------------------------
# Main execution
# ------------------------------------------
metadata_pairs = [
    (load_type_table, "partition_table"),
    (stream_load_type_table, "stream_partition_table"),
]

try:
    job_logger, job_log_stream = init_s3_logger(BUCKET_NAME, "DYNAMODB_TO_SNOWFLAKE_UPDATE_JOB")
    job_logger.info("=" * 50)
    job_logger.info("Starting Glue Job: DYNAMODB_TO_SNOWFLAKE_UPDATE_JOB")
    job_logger.info("=" * 50)
    for dynamo_table, snowflake_table in metadata_pairs:
        job_logger.info(f"\n--- Processing: {snowflake_table} ---")
        print(f"Started: Fetching details for table_name and load_type from {dynamo_table.name}")
        result = scan_table(dynamo_table,job_logger)
        update_snowflake_table(snowflake_table, result,job_logger)
        job_logger.info(f"Successfully completed: {snowflake_table}")
    job_logger.info("\n" + "=" * 50)
    job_logger.info("Job completed successfully!")
    job_logger.info("=" * 50)
except Exception as e:
    job_logger.error(f"\nJob failed with error: {str(e)}")
    job_logger.exception(traceback.format_exc())
    raise e
finally:
    upload_logs_to_s3(job_log_stream, BUCKET_NAME, "DYNAMODB_TO_SNOWFLAKE_UPDATE_JOB")
job.commit()
