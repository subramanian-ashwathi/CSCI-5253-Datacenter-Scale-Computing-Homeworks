# Import necessary libraries
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
import pandas as pd
import os
from dags.etl_scripts.google_storage import download_from_gcs, BUCKET_NAME, CREDENTIALS_PATH

# Function to load data into a PostgreSQL database
def load_data(table_file, table_name, key):
    # Define the database connection URL
    db_url = os.getenv("DATABASE_CONNECTION_STRING")
    conn = create_engine(db_url)
    download_from_gcs(BUCKET_NAME, table_file, table_file, CREDENTIALS_PATH)

    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = (
            insert(table.table)
            .values(data)
            .on_conflict_do_nothing(index_elements=[key])
        )
        result = conn.execute(stmt)
        return result.rowcount


    pd.read_parquet(table_file).to_sql(
        table_name,
        conn,
        if_exists="append",
        index=False,
        method=insert_on_conflict_nothing,
    )
    print(table_name, " loaded!")


# Function to load data into a PostgreSQL database
def load_fact_data(table_file, table_name):
    # Define the database connection URL
    db_url = os.getenv("DATABASE_CONNECTION_STRING")
    conn = create_engine(db_url)

    download_from_gcs(BUCKET_NAME, table_file, table_file, CREDENTIALS_PATH)

    pd.read_parquet(table_file).to_sql(
        table_name,
        conn,
        if_exists="replace",
        index=True,
    )
    print(table_name, " loaded!")


# Load each DataFrame into the database
# dfs[0].to_sql(
#     "Animal Dimension",
#     conn,
#     if_exists="append",
#     index=True,
#     index_label="Animal Key",
# )
# dfs[1].to_sql(
#     "Outcome Type Dimension",
#     conn,
#     if_exists="append",
#     index=True,
#     index_label="Outcome Type Key",
# )
# dfs[2].to_sql(
#     "Outcome Subtype Dimension",
#     conn,
#     if_exists="append",
#     index=True,
#     index_label="Outcome Subtype Key",
# )
# dfs[3].to_sql(
#     "Age Dimension", conn, if_exists="append", index=True, index_label="Age Key"
# )
# dfs[4].to_sql(
#     "Date Dimension", conn, if_exists="append", index=True, index_label="Date Key"
# )
# dfs[5].to_sql("Fact Animal Outcome", conn, if_exists="append", index=False)
