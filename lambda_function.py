import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Retrieve environment variables for S3, Glue catalog, and data operation settings
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Extract bucket name and object key from the event triggered by S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Reading the JSON content from the specified S3 bucket and object key using AWS Wrangler
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Flatten the JSON content (normalize 'items' into a table-like structure)
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write the transformed dataframe to S3 in Parquet format and register it in AWS Glue catalog
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,  # Path to store the transformed data in S3
            dataset=True,  # Specify it's a dataset (with partitioning and cataloging)
            database=os_input_glue_catalog_db_name,  # Glue database name for the data catalog
            table=os_input_glue_catalog_table_name,  # Glue table name for storing the data schema
            mode=os_input_write_data_operation  # Operation mode (e.g., 'overwrite', 'append')
        )

        return wr_response  # Return the response from the write operation

    except Exception as e:
        # If an error occurs, log the error message and raise the exception
        print(e)
        print(f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.")
        raise e  # Re-raise the exception for logging and handling
