# DuckLake-SAS
SQL-powered Lakehouse table format that simplifies data management by using a standard relational database for metadata and Parquet files for storage in SAS Viya

<img width="734" height="344" alt="image" src="https://github.com/user-attachments/assets/c6dfc1cb-b75c-4da7-a02a-cc4aac37542e" />


# DuckLake Features Demonstrated in this code: 

Basic DataOps with SQL

- Table Creation
- Update Operations
- Insert Operations
- Delete Rows

Snapshots and Time Travel

- Creating table snapshots
- Querying historical data
- Restoring previous states

Change Data Capture (CDC) and Upserting

- Capturing incremental data changes
- Identifying inserts, updates, and deletes
- Implementing upserts (MERGE / INSERT ... ON CONFLICT)

Schema Evolution and Retrieving Information Schema

- Evolving table schemas (adding/modifying/dropping columns)
- Managing backward-compatible changes
- Querying INFORMATION_SCHEMA for metadata
- Retrieving table, column, and constraint details

Refer: https://ducklake.select/docs/stable/
