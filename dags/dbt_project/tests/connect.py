from google.cloud import bigquery

client = bigquery.Client(project="your-project-id")
dataset_ref = client.dataset("your-dataset-name")
tables = list(client.list_tables(dataset_ref))
print(f"Total tables: {len(tables)}")
for table in tables:
    print(table.table_id)