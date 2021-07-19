import snowflake.connector as sfconnector

def getSFConnection(connectionDict):
    """
This function connects with Snowflake and provide connection object. All it needs is connection dictionary as parameter.
example:  getSFConnection(connectionDict = {
    'account': sf_account,
    'user': sf_user,
    'password': sf_password,
    'warehouse': sf_warehouse,
    'database': sf_database_name,
    'schema': sf_schema_name,
    'login_timeout': login_timeout

})

"""
    try:
        conn = sfconnector.connect(**connectionDict)
        return conn
    except Exception as e:
        print("Seems like issue with connection. Check Parameters!!", e)
        raise e

def truncate_table(conn, table_name):

    """
This function can truncate a  snowflake table. All it needs is a connection object and table name.
example: truncate_table(conn, '<table_name>')

"""
    command = """
                TRUNCATE TABLE {table_name}
                ;
    """.format(table_name=table_name)
    print(command)
    cur = conn.cursor()
    cur.execute(command)

def load_json_to_sf(conn, metadata):

    """
This function lods file from external stage to snowflake table. All it needs a dictionary containing required parameters.
example:        load_json_to_sf(conn, {
                'table_name': config.sf_table_name,
                'stage_name': config.sf_stage,
                'file_name_pattern': file_name
            })
"""

    command = """
                copy into {table_name} from (
                select $1 AS RAW_DATA,
                METADATA$FILENAME AS FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_DATE
                FROM @{stage_name})
                PATTERN='{file_name_pattern}'
                FILE_FORMAT=(TYPE=JSON STRIP_OUTER_ARRAY = FALSE)
                PURGE=TRUE
                ;
    """.format(**metadata)
    print(command)
    cur = conn.cursor()
    cur.execute(command)






