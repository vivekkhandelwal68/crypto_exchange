aws_bucket=<AWS_BUCKET_NAME>
sf_account = <SNOWFLAKE_ACCOUNT_PREFIX>
sf_database_name = <SNOWFLAKE_DATABASE_NAME>
sf_schema_name = 'public'
sf_table_name = 'EXCHANGE_STG'
sf_stage = 's3_stage'
sf_user = <SNOWFLAKE_USER_NAME>
sf_password = <SNOWFLAKE_PASSWORD>
sf_warehouse = 'COMMON_WH'
login_timeout = 20

exchange_url = 'https://api.coingecko.com/api/v3/exchange_rates'


connectionDict = {
    'account': sf_account,
    'user': sf_user,
    'password': sf_password,
    'warehouse': sf_warehouse,
    'database': sf_database_name,
    'schema': sf_schema_name,
    'login_timeout': login_timeout

}


aws_connection = {
    'aws_access_key_id': <AWS_ACCESS_KEY_ID>,
    'aws_secret_access_key': <AWS_SECRET_ACCESS_KEY>
}


