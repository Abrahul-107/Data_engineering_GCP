from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

query_for_create = """
    -- Step 1: Create a new table
    CREATE TABLE `datapipeline-434821.rahul.employees` (
    id INT64,
    name STRING,
    age INT64,
    join_date DATE
    );

    -- Step 2: Insert dummy data into the new tabl

"""
query_for_insert = """
    INSERT INTO `datapipeline-434821.rahul.employees` (id, name, age, join_date)
    VALUES
    (1, 'John Doe', 28, '2022-01-15'),
    (2, 'Jane Smith', 34, '2023-03-22'),
    (3, 'Alice Brown', 29, '2021-07-11'),
    (4, 'Bob Johnson', 45, '2020-10-05');

"""


# Execute the query and get the results
query_job = client.query(query_for_create)  # API request
query_job = client.query(query_for_insert)
