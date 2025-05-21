import os
from google.cloud import bigquery
from google.oauth2 import service_account


# Initialize BigQuery client
client = bigquery.Client(
    credentials=service_account.Credentials.from_service_account_file("credentials/gcp_authen.json")
)

# Set your dataset details
project_id = "your-project-id"
dataset_id = "your-dataset-name"

# Get list of tables in the dataset
tables = client.list_tables(f"{project_id}.{dataset_id}")

# Drop tables starting with 'gx_'
for table in tables:
    table_name = table.table_id
    if table_name.startswith("gx_"):
        full_table_id = f"{project_id}.{dataset_id}.{table_name}"
        print(full_table_id)
        print(f"Dropping table: {full_table_id}")
        client.delete_table(full_table_id, not_found_ok=True)