#Main Glue Job Script for MySQL and postgresql to S3 ETL with Multi-threading and CDC Support

import sys
import boto3
import json
import pytz
import traceback
from pyspark.sql import Row
from datetime import datetime ,timedelta
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

# ------------------------------------------
# Parse arguments (from Step Function)
# ------------------------------------------
required_args = ["JOB_NAME", "database_name", "thread_count", "source_type", "connection_name","jdbc_url","sf_schema"]


args = getResolvedOptions(sys.argv, required_args)
# ------------------------------------------
# Glue context & Spark session
# ------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
sp=spark.builder.config("spark.sql.shuffle.partitions","128").config("spark.sql.adaptive.enabled", "true").config("spark.sql.adaptive.coalescePartitions.enabled", "true").config("spark.sql.files.maxPartitionBytes", "134217728").config("spark.default.parallelism", "128").config("spark.sql.analyzer.maxIterations","500").config("spark.sql.autoBroadcastJoinThreshold","-1").config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY").config("spark.sql.parquet.compression.codec", "snappy").appName("glue-saps4").getOrCreate()
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------
# Parameters
# ------------------------------------------
DATABASE_NAME = args["database_name"]
THREAD_COUNT = int(args["thread_count"])
CONNECTION_NAME = args["connection_name"]
SOURCE_TYPE=args["source_type"]
jdbc_url=args["jdbc_url"]

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sf_url = "vdqiiha-zc94797.snowflakecomputing.com"
sf_database = "RAW"
sf_schema = args["sf_schema"]  # <-- Dynamic from Glue job parameter
sf_warehouse = "DEV_CITADEL_WH"
sf_role = "DEV_ETL_DEVOPS"
sf_user = "DEV_ETL_SYS_USER"
sf_password = "4VX86clTXC5OWc"
# ------------------------------------------
# Logging
# ------------------------------------------
logger = glue_context.get_logger()


ist = pytz.timezone("Asia/Kolkata")
current_time = datetime.now(ist)
load_date = current_time.strftime("%Y-%m-%d")
load_hour = current_time.strftime("%H")
# ------------------------------------------
# S3 target
# ------------------------------------------
BUCKET_NAME = "ohl-ganit"
if SOURCE_TYPE.lower() == "mysql":
    TARGET_BASE_PATH = f"s3://{BUCKET_NAME}/MySQL/"
    datetime_cast = "DATETIME"
    user = "ganit_ro_user"
    password = "72RmpaD426d6VQZrANiD8Hnl"
elif SOURCE_TYPE.lower() == "postgresql":
    TARGET_BASE_PATH = f"s3://{BUCKET_NAME}/PostgreSQL/"
    datetime_cast = "TIMESTAMP"
    user = "glue_ro_user"
    password = "5y4umdxep4CNn5yVxHNQttRwb77Vdfzb"
else:
    raise ValueError(f"Unsupported source type: {SOURCE_TYPE}")
# jdbc_url = "jdbc:mysql://oms-prod.csygmcby9gzi.ap-south-1.rds.amazonaws.com:3306/prod_oms"

# ------------------------------------------
# DynamoDB metadata table for CDC tracking
# ------------------------------------------\
RUN_TIME_TABLE='TABLE_RUN_TIME_METADATA'
LOAD_TYPE_TABLE='STREAM_TABLE_LOAD_TYPE'
session = boto3.Session(region_name="ap-south-1")
dynamodb = session.resource("dynamodb")
run_time_table = dynamodb.Table(RUN_TIME_TABLE)
load_type_table = dynamodb.Table(LOAD_TYPE_TABLE)
print("Done:Read all required data !")
def get_last_run_timestamp(database_name,table_name):
    print("""Get last run timestamp from DynamoDB.""")
    key = {"database_name": database_name, "table_name": table_name}
    response = run_time_table.get_item(Key=key)
    if "Item" in response:
        logger.info(f"INFO: Item retrieved successfully: {response['Item']}")
        print("Done:get_last_run_timestamp")
        return response["Item"]["last_updated_on"]
    else:
        raise ValueError(f"ERROR: Item with key {key} does not exist in table {METADATA_TABLE}")

def update_last_run_timestamp(database_name,table_name,last_updated_on):
    print("""Update last run timestamp after successful run.""")
    run_time_table.update_item(
        Key={"database_name": database_name, "table_name": table_name},
        UpdateExpression="SET last_updated_on = :last_updated_on",
        ExpressionAttributeValues={":last_updated_on": last_updated_on},
        ReturnValues="UPDATED_NEW",
    )
    print("Done:update_last_run_timestamp")
def get_incremental_metadata(database_name, table_name):
    """
    Fetch incremental column names (incremental_col1, incremental_col2, incremental_col3)
    for a given database.table from DynamoDB table LOAD_TYPE_TABLE.
    """
    try:
        key = {"database_name": database_name, "table_name": table_name}
        response = load_type_table.get_item(Key=key)

        if "Item" not in response:
            logger.warning(f"No incremental metadata found for {database_name}.{table_name}")
            return None, None, None

        item = response["Item"]
        incremental_col1 = item.get("incremental_col1")
        incremental_col2 = item.get("incremental_col2")
        incremental_col3 = item.get("incremental_col3")
        incremental_col1 = incremental_col1.strip() if incremental_col1 else None
        incremental_col2 = incremental_col2.strip() if incremental_col2 else None
        incremental_col3 = incremental_col3.strip() if incremental_col3 else None

        print(f"Incremental cols for {database_name}.{table_name}: "
                    f"{incremental_col1}, {incremental_col2}, {incremental_col3}")
        print("Done:get_incremental_metadata")            
        return incremental_col1, incremental_col2, incremental_col3

    except Exception as e:
        logger.error(f"Error fetching incremental metadata for {database_name}.{table_name}: {str(e)}")
        traceback.print_exc()
        return None, None, None

# ------------------------------------------
# Fetch load type mapping from Snowflake
# ------------------------------------------
def get_table_load_type_map(database_name):
    """
    Reads load_type metadata for all tables belonging to a given database
    from DynamoDB and returns a dictionary with keys as (database_name, table_name)
    and values as load_type.
    """
    try:
        print(f"Fetching load type metadata from DynamoDB for database: {database_name}")
        # Query using database_name as the partition key
        response = load_type_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name)
        )

        items = response.get("Items", [])

        # Handle pagination (in case of large data)
        while "LastEvaluatedKey" in response:
            response = load_type_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name),
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))

        # Create the map
        table_load_type_map = {
            (item["database_name"], item["table_name"]): {
            "load_type": item.get("load_type", "full_load"),
            "partition_col": item.get("partition_col"),
            "is_partitioned": item.get("is_partitioned")
          }
            for item in items
        }

        print(f"Found {len(table_load_type_map)} tables for database '{database_name}'")
        print(table_load_type_map)
        print("Done:get_table_load_type_map")
        return table_load_type_map

    except Exception as e:
        print(f"Failed to fetch load type metadata from DynamoDB: {str(e)}")
        traceback.print_exc()
        send_slack_notification(DATABASE_NAME, "failed")
        sys.exit(1)

def to_dt(val):
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
    return val
# ------------------------------------------
# Worker: Process each table
# ------------------------------------------
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

def process_table(database_name, table_name, load_type,partition_col,is_partitioned):    
    print(f"process table function started:{database_name}-{table_name}-{load_type}-{partition_col}-{is_partitioned}")
    try:
        logger.info(f"[START] ({database_name}.{table_name}) load_type={load_type}")
        incremental_col1, incremental_col2, incremental_col3 = get_incremental_metadata(database_name, table_name)
        incremental_cols = [col for col in [incremental_col1, incremental_col2, incremental_col3] if col]

         # Query logic CAST({col} AS {datetime_cast}) 
        if load_type == "full_load":
            query = f"SELECT * FROM {table_name}"
            print("Query For Full Load",query)
        elif load_type == "cdc_load":
            try:
                last_updated_on = get_last_run_timestamp(database_name, table_name)
            except Exception as e:
                logger.warning(f"No DynamoDB entry for {database_name}.{table_name}: {e}")
            if not last_updated_on:
                logger.info(f"Skipping CDC for {database_name}.{table_name}: No last_updated_on in DynamoDB.")
                print(f"Skipped CDC for {table_name}: No last_updated_on found in DynamoDB")
                return ("SKIPPED", database_name, table_name, None)
            try:
                last_updated_on_dt = datetime.strptime(last_updated_on, "%Y-%m-%d %H:%M:%S.%f")  # With microseconds
            except ValueError:
                last_updated_on_dt = datetime.strptime(last_updated_on, "%Y-%m-%d %H:%M:%S")
            last_updated_on_dt = last_updated_on_dt - timedelta(minutes=30)    
            last_updated_on_trunc = last_updated_on_dt.replace(second=0, microsecond=0)
            last_updated_on_str = last_updated_on_trunc.strftime("%Y-%m-%d %H:%M:%S")      
            if incremental_cols:
                where_conditions = " OR ".join([f"({col}) > CAST('{last_updated_on_str}' AS {datetime_cast})" for col in incremental_cols])
                query = f"SELECT * FROM {table_name} WHERE {where_conditions}"
                print("Query For CDC load",query)
            else:
                logger.warning(f"No incremental columns found for {database_name}.{table_name}. Skipping CDC extraction.")
                return ("SKIPPED", database_name, table_name, None)    
        else:
            raise Exception(f"Unknown load_type: {load_type}")
        if is_partitioned=="TRUE":
            NUM_PARTITIONS=2
            min_max_query = f"(SELECT MIN({partition_col}) AS min_val, MAX({partition_col}) AS max_val FROM {table_name}) AS bounds"
            bounds_df = spark.read \
                        .format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", min_max_query) \
                        .option("user", user) \
                        .option("password", password) \
                        .load()
            try:
                 min_val, max_val = bounds_df.first()
                 print(f"MIN/MAX bounds-{table_name}-min-{min_val}-max-{max_val}")
                 if min_val is None:
                     min_val = 0  
                 if max_val is None:
                     max_val = 0
            except TypeError:
                print("Warning: Could not determine MIN/MAX bounds. Check if the table is empty.")
                min_val, max_val = 0, 1
            print(f"Reading table with numpartition: {table_name}")
            df= spark.read \
                             .format("jdbc") \
                             .option("url", jdbc_url) \
                             .option("dbtable", f"({query}) AS tmp") \
                             .option("user", user) \
                             .option("password", password) \
                             .option("partitionColumn", partition_col) \
                             .option("lowerBound", int(min_val)) \
                             .option("upperBound", int(max_val)) \
                             .option("numPartitions", NUM_PARTITIONS) \
                             .option("fetchsize", 10000) \
                             .option("zeroDateTimeBehavior", "convertToNull") \
                             .load()        
        else:   
            print(f"Reading table without numpartition: {table_name}")
            df = spark.read \
                 .format("jdbc") \
                 .option("url", jdbc_url) \
                 .option("dbtable", f"({query}) AS tmp") \
                 .option("user", user) \
                 .option("password", password) \
                 .option("zeroDateTimeBehavior", "convertToNull") \
                 .load()
        # target_file_size_mb = 128
        # estimated_partition_count = int((28 * 1024) / target_file_size_mb)
        # df = df.repartition(estimated_partition_count)
        df=df.withColumn("insert_timestamp",current_timestamp())
        record_count = df.count()
        print(f"Record count for {table_name}: {record_count}")
        #  If no data, skip S3 write & timestamp update
        if record_count == 0:
            logger.info(f"No new data for {database_name}.{table_name}. Skipping write and timestamp update.")
            return ("SUCCESS",database_name, table_name, df)
        
        # target_path = f"{TARGET_BASE_PATH}{database_name}/{table_name}/"
        target_path = (
            f"{TARGET_BASE_PATH}{database_name}/{table_name}/"
           f"{load_date}/{load_hour}/"
        )
        write_mode = "overwrite" if load_type == "full_load" else "append"
        df.write.mode(write_mode).option("compression", "snappy").option("maxRecordsPerFile", "100000").parquet(target_path)
        logger.info(f"[DONE] Table {table_name} → {target_path} (mode={write_mode})")
        sfOptions = {
        "sfURL": sf_url,
        "sfDatabase": sf_database,
        "sfSchema": sf_schema,
        "sfWarehouse": sf_warehouse,
        "sfRole": sf_role,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        }
        df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", table_name.upper()).mode(write_mode).save()
        print("update timestamp")
        if incremental_cols:
            agg_exprs = [sp_max(col(c)).alias(c) for c in incremental_cols]
            max_vals = df.agg(*agg_exprs).collect()[0].asDict()
            # max_ts = max([v for v in max_vals.values() if v is not None])
            non_null_vals = [v for v in max_vals.values() if v is not None]
            normalized_vals = [to_dt(v) for v in non_null_vals if v is not None] #new added
            print(f"{table_name},non_null_vals:-{non_null_vals}")
            max_ts = str(max(normalized_vals)) #new added
            update_last_run_timestamp(database_name,table_name,max_ts)
            print(f"Updated timestamp for table:{table_name}",max_ts)
            
        return ("SUCCESS",database_name,table_name,df)

    except Exception as e:
        logger.error(f"[FAILED] Table {table_name}: {str(e)}")
        traceback.print_exc()
        print(f"Error For Table :{table_name}")
        #send_slack_notification(DATABASE_NAME, "failed")
        raise e
        return {table_name: f"FAILED: {str(e)}"}

# ------------------------------------------
# Main execution
# ------------------------------------------
print("started : Fetching details for table_name and load_type")
table_load_type_map = get_table_load_type_map(DATABASE_NAME)

print("Prepare list of tables to load") 
tables_to_load = [
    (db, tbl, info["load_type"], info["partition_col"], info["is_partitioned"])
    for (db, tbl), info in table_load_type_map.items()
]
# excluded_tables = {
#     "requests" ,"camps" , "packages" ,"orders"
   
# }

# filtered_tables_to_load = [
#     (db, tbl, load_type)
#     for (db, tbl, load_type) in tables_to_load
#     if tbl in excluded_tables
# ]

print(tables_to_load)
results = []
with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
    futures = {executor.submit(process_table , db, tbl, load_type,partition_col,is_partitioned): (db, tbl,load_type,partition_col,is_partitioned)
        for db, tbl, load_type , partition_col , is_partitioned in tables_to_load}
    for future in as_completed(futures):
        status,database_name,table_name, df = future.result()
        if df is not None:
            row_count = df.count()
            print(f"Finished reading: {table_name} (Rows: {row_count})")
        else:
            row_count = 0 
            print(f"Skipped table: {table_name} (No data loaded)")
        results.append((status,database_name,table_name,row_count))

print("All threads completed. Summary:")
audit_rows = []
audit_time = datetime.now(ist)
for (status, db, tbl, row_count) in results:
    audit_rows.append(
        Row(
            database_name=db,
            table_name=tbl,
            layer="RAW",
            status=status,
            row_count=(row_count),
            inserted_at=audit_time
        )
    )
schema = StructType([
    StructField("DATABASE_NAME", StringType(), True),
    StructField("TABLE_NAME", StringType(), True),
    StructField("LAYER", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("ROW_COUNT", IntegerType(), True),
    StructField("INSERTED_AT", TimestampType(), True)
])    
audit_df = spark.createDataFrame(audit_rows,schema)
audit_df.show()
sfOptions = {
    "sfURL": sf_url,
    "sfDatabase": "AUDIT",
    "sfSchema": "AUDIT_PROGRAM",
    "sfWarehouse": sf_warehouse, #update object
    "sfRole": sf_role,
    "sfUser": sf_user,
    "sfPassword": sf_password,
}
audit_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "ETL_AUDIT_LOG") \
    .mode("append") \
    .save()
#send_slack_notification(DATABASE_NAME, "success")
print("✅ Audit data written to Snowflake table: ETL_AUDIT_LOG")
for res in results:
    print(res)
print("TOTAL TABLE WRITTEN:",len(results))

job.commit()