import requests
import boto3
import config
import time
import json
from  sffunction import getSFConnection,load_json_to_sf, truncate_table
from snowflake.connector import ProgrammingError


def lambda_handler(event, context):
    s3_client = boto3.client('s3',**config.aws_connection) # getting S3 client object
    response = requests.get(config.exchange_url) # Making REST API call and getting response
    final_response = json.dumps(list(response.json()['rates'].values()), indent=2) # Preparing required Json String

    file_name = f'exchange/exch_{int(time.time() * 1000)}.json' # Preparing file name

    try:
        s3_client.put_object(Body=final_response, Bucket=config.aws_bucket, Key=file_name) # Write file to S3
    except Exception as e:
        print("can not upload file on S3", e)
        raise e
    else:
        print(f'{file_name} successfully uploaded')

        try:
            conn = getSFConnection(config.connectionDict) # Getting Snowflake connection object
        except Exception as e:
            print(f"issue with Snowflake connection {config.connectionDict}", e)
        else:
            exchange_metadata = {
                'table_name': config.sf_table_name,
                'stage_name': config.sf_stage,
                'file_name_pattern': file_name
            }
            try:
                truncate_table(conn, config.sf_table_name) # Truncating table
                load_json_to_sf(conn,exchange_metadata) # Loading data in Snowflake Stage table.
            except ProgrammingError as err:
                print(f"error while truncating {config.sf_table_name} table and loading file {file_name}", err)
                raise err
            finally:
                conn.close()





