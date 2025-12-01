# THIS JOB WILL WRITE LOAD_TYPE (TABLE IS FULL LOAD OR CDC LOAD) INFO TO dynamodb ,This will be one time activity.  

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sp=spark.builder.config("spark.sql.shuffle.partitions","04").config("spark.sql.analyzer.maxIterations","500").config("spark.sql.autoBroadcastJoinThreshold","-1").appName("glue-saps4").getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET_NAME = "ohl-ganit"
TARGET_BASE_PATH = f"s3://{BUCKET_NAME}/LOAD_TYPE_METADATA/stream_load_type.csv"
LOAD_TYPE_TABLE='STREAM_TABLE_LOAD_TYPE'
session = boto3.Session(region_name="ap-south-1")
dynamodb = session.resource("dynamodb")
load_type_table = dynamodb.Table(LOAD_TYPE_TABLE)
df = spark.read.option("header", "true").csv(TARGET_BASE_PATH)
records = df.collect()
for row in records:
    # Convert Spark Row to Python dict
    row_dict = row.asDict()

    item = {
        "database_name": (row_dict.get("database_name")),
        "table_name": (row_dict.get("table_name")),
        "load_type": (row_dict.get("load_type", "full_load")),
        "incremental_col1":(row_dict.get("incremental_col1")),
        "incremental_col2":(row_dict.get("incremental_col2")),
        "incremental_col3":(row_dict.get("incremental_col3")),
        "partition_col":(row_dict.get("partition_col")),
        "is_partitioned":(row_dict.get("is_partitioned"))
    }
    load_type_table.put_item(Item=item)
job.commit()
