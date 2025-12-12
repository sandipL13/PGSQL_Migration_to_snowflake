import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import snowflake.connector

# required parameter: database_name
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Mapping database_name → task_name
arr = [
    {
        "database_name": "batch_consents",
        "task_name": "CONSENT_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_cds",
        "task_name": "BATCH_CDS_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_ets_lab",
        "task_name": "BATCH_ETS_LAB_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_accounts",
        "task_name": "BATCH_PROD_ACCOUNTS_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_cdpapi",
        "task_name": "BATCH_PROD_CDPAPI_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_citadelv2",
        "task_name": "BATCH_PROD_CITADELV2_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_health_api",
        "task_name": "BATCH_PROD_HEALTH_API_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_oms",
        "task_name": "BATCH_PROD_OMS_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_partner_api",
        "task_name": "BATCH_PROD_PARTNER_API_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_payment",
        "task_name": "BATCH_PROD_PAYMENT_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_clr",
        "task_name": "BATCH_CLR_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_feedback",
        "task_name": "BATCH_FEEDBACK_DATA_DAILY_LOAD"
    },{
        "database_name": "batch_geomark",
        "task_name": "BATCH_GEOMARK_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_patients_service",
        "task_name": "BATCH_PATIENTS_SERVICE_DATA_DAILY_LOAD"
    },
    {
        "database_name": "batch_prod_gringotts",
        "task_name": "BATCH_PROD_GRINGOTTS_DATA_DAILY_LOAD"
    },
      {
        "database_name": "prod_odinapi",
        "task_name": "BATCH_PROD_ODINAPI_API_DATA_DAILY_LOAD"
    },  {
        "database_name": "stream_prod_citadelv2",
        "task_name": "STREAM_PROD_CITADELV2_DATA_DAILY_LOAD"
    },
      {
        "database_name": "stream_prod_oms",
        "task_name": "STREAM_PROD_OMS_DATA_DAILY_LOAD"
    },
      {
        "database_name": "stream_prod_scheduler_api",
        "task_name": "STREAM_PROD_SCHEDULER_API_DATA_DAILY_LOAD"
    },
      {
        "database_name": "stream_revenue_register",
        "task_name": "STREAM_REVENUE_REGISTER_DATA_DAILY_LOAD"
    }
]

# Get task_name for the given database_name
task_name = None
for item in arr:
    if item["database_name"].lower() == args["database_name"].lower():
        task_name = item["task_name"]
        break

if not task_name:
    raise Exception(f"Task not found for database_name: {args['database_name']}")

print(f"Database Name: {args['database_name']}, Task to Run: {task_name}")

# Snowflake connection

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


conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account="vdqiiha-zc94797",
    warehouse="PRD_STAGE_WH",
    database="TASK_DB",
    schema="MERGE_TASKS"
)

cur = conn.cursor()
print("Connection established.")

# Fully qualified task name
full_task_name = f'"TASK_DB"."MERGE_TASKS"."{task_name}"'

# Resume Task
resume_stmt = f"ALTER TASK {full_task_name} RESUME;"
print("Running:", resume_stmt)
# cur.execute(resume_stmt)

print("Task resumed.")

# Execute Task
execute_stmt = f"EXECUTE TASK {full_task_name};"
print("Running:", execute_stmt)
cur.execute(execute_stmt)

print("Task executed.")

cur.close()
conn.close()

job.commit()
