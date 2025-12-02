# PGSQL_Migration_to_snowflake
Code base for migration of source pgsql and mysql database to snowflake via glue.

End-to-End Project Flow:

This project uses Snowflake as the primary orchestrator, supported by AWS services such as Lambda, Glue, S3, and DynamoDB. 
The complete flow is described below:

1. Orchestration Triggered from Snowflake

The workflow begins in Snowflake using a Snowflake Utility Function.
This function triggers an AWS Lambda Function, passing the required parameters for ingestion.

2. Lambda Invokes Glue Job

The Lambda function:
Initiates the AWS Glue Job with the provided parameters
Waits for the Glue job to complete
Handles any success or failure response from Glue

3. Data Ingestion via Glue Job

The AWS Glue job performs the following tasks:
Connects to the source database
Extracts data based on table-specific configurations
Writes the ingested data to:
Amazon S3 (Raw Layer)
Snowflake (Raw Layer)
Ingestion table details like incremental column , partition column will be taken from DynamoDB table
Runtime Metadata Table from DynamoDb is used for incremental/CDC loads.
Logging will be stored in s3 only.

 4. Raw to Staging Layer (Snowflake)

After raw ingestion completes, the Snowflake Utility Module triggers the process_table function.
This function:
Calls the Snowflake stored procedure associated with the table
Executes MERGE statements to build and update the Stagging Layer

5. Building the Gold Layer

The Gold Layer is prepared by executing business logic queries on top of the Stagging Layer.
This includes:
Data transformations
Business rule applications
Dimensional modeling
Aggregations and metrics computation
The output forms the high-quality curated dataset for analytics and downstream consumption.

----------------------------------------------------------------------------------------------------
S3 Folder structure:
1. BUCKET_NAME 
        -SOURCE_DB_NAME
            -TABLE_NAME
                -DATE
                    -HOUR
                        -dataFile.parquet
Ex.s3://ohl-ganit/PostgreSQL/clr/availed_coupon_snapshots/2025-11-05/12/
