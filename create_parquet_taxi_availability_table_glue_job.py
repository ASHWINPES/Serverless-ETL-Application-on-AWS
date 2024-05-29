import sys
import boto3

client = boto3.client('athena')

SOURCE_TABLE_NAME = 'taxi_availability_data_bucket_1'
NEW_TABLE_NAME = 'taxi_availability_data_parquet_tbl'
NEW_TABLE_S3_BUCKET = 's3://taxi-availability-data-parquet-bucket-1/'
MY_DATABASE = 'taxi_availability_database'
QUERY_RESULTS_S3_BUCKET = 's3://query-results-location-taxi-availability-1'

# Refresh the table
queryStart = client.start_query_execution(
    QueryString = f"""
    CREATE TABLE {NEW_TABLE_NAME} WITH
    (external_location='{NEW_TABLE_S3_BUCKET}',
    format='PARQUET',
    write_compression='SNAPPY',
    partitioned_by = ARRAY['time'])
    AS

    SELECT
        taxi_count AS total_taxis_available
        ,taxi_count / 100 AS total_taxis_in_hundreds
        ,time
        ,row_ts
    FROM "{MY_DATABASE}"."{SOURCE_TABLE_NAME}"

    ;
    """,
    QueryExecutionContext = {
        'Database': f'{MY_DATABASE}'
    }, 
    ResultConfiguration = { 'OutputLocation': f'{QUERY_RESULTS_S3_BUCKET}'}
)

# Checking for validity of AWS Glue job
# list of responses
resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

# get the response
response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

# wait until query finishes
while response["QueryExecution"]["Status"]["State"] not in resp:
    response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])
    
# if it fails, exit and give the Athena error message in the logs
if response["QueryExecution"]["Status"]["State"] == 'FAILED':
    sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])