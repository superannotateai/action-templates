"""
Python Version Compatibility: 3.8, 3.9, 3.10, 3.11
Dependencies:
superannotate library
databricks-sql-connector library

This action gets all the annotation data of the provided item IDs, transforms it, and inserts it into the Databricks Delta table.
Before running the script, make sure to set the following environment variables:

You can define key-value variables from the Secrets page of the Actions tab in Orchestrate. You can then mount
Please refer to the documentation for more details: https://doc.superannotate.com/docs/create-automation#secret

- SA_TOKEN: The SuperAnnotate SDK token. With this key, the client will be automatically initialized.
- DB_TOKEN: The Databricks access token.

You also need to provide function arguments in the Event object, while setting up a pipeline:

- sa_component_ids - comma separated string of the component ids which should move to Databricks
- databricks_columns - comma separated string of column names, which should should be used to upload data to Databricks
- db_server_hostname - Databricks SQL connector hostname
- db_http_path - Databricks connector http path
- db_catalog - destination catalog name in Databricks
- db_schema - destination schema name in Databricks
- db_table - destination table name in Databricks
- db_volume - destination volume name in Databricks
- create_delta_table - boolean value to create a new table in Databricks

The 'handler' function triggers the script upon an event [Fired in explore].
"""
import os
import csv
from datetime import datetime
import tempfile
from superannotate import SAClient
from databricks import sql

### Constants
SA_TOKEN = ""
DB_TOKEN = ""
SA_COMPONENT_IDS_ARG = 'sa_component_ids'
DB_COLUMN_NAMES_ARG = 'databricks_columns'
DB_SERVER_HOSTNAME_ARG = 'db_server_hostname'
DB_HTTP_PATH_ARG = 'db_http_path'
DB_CATALOG_ARG = 'db_catalog'
DB_SCHEMA_ARG = 'db_schema'
DB_TABLE_ARG = 'db_table'
DB_VOLUME_ARG = 'db_volume'
CREATE_DELTA_TABLE_ARG = 'create_delta_table'
TEMP_DIR = tempfile.gettempdir()

def validate_context(context):
    if not context:
        print(">>> Invalid context >>>", context)
        return False
    items = context.get('items')
    if not items:
        print(">>> Invalid items list in context >>>", items)
        return False
    project_id = context.get('project_id')
    team_id = context.get('team_id')

    if not project_id or not team_id:
        print(">>> Invalid context >>>", context)
        return False

    return {
        'items': items,
        'project_id': project_id,
        'team_id': team_id
    }

def find_componets_values(annotation, ids):
    values = {}
    all_ids = ids.copy()
    for component_id in all_ids:
        for instance in annotation.get("instances", []):
            if "element_path" in instance.keys():
                id_in_path = instance.get("element_path")[0]
                if id_in_path is not None and id_in_path == component_id:
                    value = instance['attributes'][0]['name']
                    values[component_id] = str(value)
        if component_id not in values:
            values[component_id] = ''
    return values

def argument_parser(event, argument):
    arg = event.get(argument)
    if not arg:
        print(f"Missing argument: {argument}")
        raise Exception(f"Missing argument: {argument}")
    return arg

def argument_str_to_list(arg_str):
    arg_list_tmp = arg_str.split(",")
    arg_list = [s.strip() for s in arg_list_tmp]
    return arg_list

def db_connect(server_hostname, http_path, access_token, staging_allowed_local_path):
    connection = sql.connect(
                        server_hostname = server_hostname,
                        http_path = http_path,
                        access_token = access_token,
                        staging_allowed_local_path=staging_allowed_local_path,
    )
    return {
        'connection': connection,
        'cursor': connection.cursor()
    }

def db_disconnect(connection):
    connection['cursor'].close()
    connection['connection'].close()

def db_execute(cursor, query):
    cursor.execute(query)
    print(cursor.fetchall())

def write_csv_file(csv_full_file_name, db_column_names, annotation_data, sa_component_ids):
    with open(csv_full_file_name, 'w', newline='\n') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=db_column_names)
        writer.writeheader()
        for annotation in annotation_data:
            sa_values = find_componets_values(annotation, sa_component_ids)
            writer.writerow(sa_values)

def db_upload_to_volume(csv_full_file_name, csv_file_name, db_catalog, db_schema, db_volume, db_connection):
    query = f"""
        PUT '{csv_full_file_name}' 
        INTO '/Volumes/{db_catalog}/{db_schema}/{db_volume}/superannotate/{csv_file_name}' OVERWRITE
        """
    db_execute(db_connection['cursor'], query)
    return

def db_create_table(db_catalog, db_schema, db_table, db_connection):
    query = f"CREATE TABLE IF NOT EXISTS {db_catalog}.{db_schema}.{db_table};"
    db_execute(db_connection['cursor'], query)
    return

def db_volume_to_table(csv_file_name, db_table_name, db_catalog, db_schema, db_volume, db_connection):
    query = f"""
        COPY INTO {db_catalog}.{db_schema}.{db_table_name}
        FROM '/Volumes/{db_catalog}/{db_schema}/{db_volume}/superannotate/{csv_file_name}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('mergeSchema' = 'true', 'header' = 'true', 'multiLine' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true');
        """
    db_execute(db_connection['cursor'], query)
    return

def db_delete_from_volume(csv_file_name, db_catalog, db_schema, db_volume, db_connection):
    query = f"REMOVE '/Volumes/{db_catalog}/{db_schema}/{db_volume}/superannotate/{csv_file_name}'"
    db_execute(db_connection['cursor'], query)
    return 

def sa_print(text, is_error=False):
    if is_error:
        print(f">>> SA ERROR: {text} !!! >>> >>> >>> >>>")
    else:
        print(f">>> SA: {text} >>> >>> >>> >>>")

def handler(event, context):
    if 'SA_TEAM_TOKEN' in os.environ and 'DB_ACCESS_TOKEN' in os.environ:
        SA_TOKEN = os.environ['SA_TEAM_TOKEN']
        DB_TOKEN = os.environ['DB_ACCESS_TOKEN']
        sa = SAClient(SA_TOKEN)
        print(SA_TOKEN, DB_TOKEN)
    else:
        sa_print("Missing environment variables", is_error=True)
        raise Exception("Missing environment variables")

    sa_print("Starting the action")
    sa_data = validate_context(context)
    if not sa_data:
        sa_print("Input parameters are invalid", is_error=True)
        return

    sa_component_ids = argument_str_to_list(argument_parser(event, SA_COMPONENT_IDS_ARG))
    db_column_names = argument_str_to_list(argument_parser(event, DB_COLUMN_NAMES_ARG))
    db_server_hostname = argument_parser(event, DB_SERVER_HOSTNAME_ARG)
    db_http_path = argument_parser(event, DB_HTTP_PATH_ARG)
    db_catalog = argument_parser(event, DB_CATALOG_ARG)
    db_schema = argument_parser(event, DB_SCHEMA_ARG)
    db_table = argument_parser(event, DB_TABLE_ARG)
    db_volume = argument_parser(event, DB_VOLUME_ARG)
    create_delta_table = argument_parser(event, CREATE_DELTA_TABLE_ARG).lower() == "true"

    sa_print("Downloading annotations")
    annotation_data = sa.get_annotations(sa_data['project_id'], items=sa_data['items'])
    table_name = f'{db_table}_{datetime.now().strftime("%m%d%Y_%H%M%S")}'
    csv_file_name = table_name+".csv"
    csv_full_file_name = f'{TEMP_DIR}/{csv_file_name}'
    sa_print("Saving annotations to CSV file")
    write_csv_file(csv_full_file_name, db_column_names, annotation_data, sa_component_ids)

    sa_print("Connecting to Databricks SQL connector")
    staging_allowed_local_path = TEMP_DIR
    db_connection = db_connect(db_server_hostname, db_http_path, DB_TOKEN, staging_allowed_local_path)
    sa_print("Upliadng CSV file to Databricks volume")
    db_upload_to_volume(csv_full_file_name, csv_file_name, db_catalog, db_schema, db_volume, db_connection)
    if create_delta_table:
        sa_print("Creating a new table in Databricks")
        db_create_table(db_catalog, db_schema, table_name, db_connection)
        sa_print("Addind data to the table from volume in Databricks")
        db_volume_to_table(csv_file_name, table_name, db_catalog, db_schema, db_volume, db_connection)
        sa_print("Deleting CSV file from Databricks volume")
        db_delete_from_volume(csv_file_name, db_catalog, db_schema, db_volume, db_connection)
    db_disconnect(db_connection)
    sa_print("Action completed successfully")
    return