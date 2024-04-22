# Integration for Pushing Data from Superannotate to Databricks

## Introduction
This Python action is designed to facilitate the seamless integration and transfer of data to Databricks. It simplifies the process of uploading datasets from various sources directly into your Databricks catalog, making data management more efficient and less error-prone.

## Action Parameters
Before you begin using this action, ensure you have following action parameters in Orchestrate pipeline:



## Usage
1. In your SuperAnnotate team [create a Secret](https://doc.superannotate.com/docs/create-automation#secrets) with following values:
    - `DB_ACCESS_TOKEN`: Databricks [personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html),
    - `SA_TEAM_TOKEN`: Superannotate [token for Python SDK](https://doc.superannotate.com/docs/token-for-python-sdk)
2. In your SuperAnnotate team [create an action](https://doc.superannotate.com/docs/pipeline-components#actions) from template. Pick `Insert data into Databricks Delta Table` action template
3. In your SuperAnnotate project [create a pipeline](https://doc.superannotate.com/docs/create-automation#create-a-pipeline)
    - Drang and drop **Event** -> **Item** -> **Fired in Explore**
    - Drang and drop the action created in step 2
    - Connect event and action
4. Click on the action and:
    - Set the secret value (chose the one created in step 1)
    - Set Event object with following values:
        - `sa_component_ids`: comma separated string of the component ids which should move to Databricks
        - `databricks_columns`: comma separated string of column names, which should should be used to upload data to Databricks
        - `db_server_hostname`: Databricks SQL connector hostname
        - `db_http_path`: Databricks connector http path
        - `db_catalog`: destination catalog name in Databricks
        - `db_schema`: destination schema name in Databricks
        - `db_table`: destination table name in Databricks
        - `db_volume`: destination volume name in Databricks
        - `create_delta_table`: boolean value to create a new table in Databricks
5. Press Save
Now every time when you [run a pipeline from Explore](https://doc.superannotate.com/docs/bulk-actions#run-pipeline), data from selected items will be moved to Databricks.