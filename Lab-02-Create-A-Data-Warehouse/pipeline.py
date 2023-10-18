# Import necessary libraries
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import argparse

# Function to extract data from a source (CSV in this case)
def extract_data(source):
    return pd.read_csv(source)

# Function to transform the extracted data
def transform_data(data):
    # Create a copy of the data
    new_data = data.copy()

    # Replace NaN values in the 'Name' column with "N/A"
    new_data['Name'] = new_data['Name'].replace(np.nan, "N/A")

    # Split 'MonthYear' into 'Month' and 'Year' columns
    new_data[['Month', 'Year']] = new_data['MonthYear'].str.split(" ", expand=True)

    # Create a new 'Sex' column and handle 'Unknown' values
    new_data['Sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)

    # Split 'Sex' into 'Property' and 'Sex' columns
    new_data[['Property', 'Sex']] = new_data['Sex'].str.split(" ", expand=True)

    # Drop unnecessary columns
    new_data.drop(columns=['MonthYear', 'Sex upon Outcome'], inplace=True)

    # Extract relevant columns for dimension tables
    animal_dimension = new_data[["Animal ID", "Name", "Date of Birth", "Animal Type", "Property", "Sex", "Breed", "Color"]]
    outcome_type_dimension = new_data[["Outcome Type"]].drop_duplicates()
    outcome_subtype_dimension = new_data[["Outcome Subtype"]].drop_duplicates()
    age_dimension = new_data[["Age upon Outcome"]].drop_duplicates()
    date_dimension = new_data[["DateTime"]].drop_duplicates()

    # Create mapping keys for dimension tables
    new_data["Outcome Type Key"] = new_data["Outcome Type"].map(pd.Series(data = outcome_type_dimension.index, index = outcome_type_dimension["Outcome Type"]).to_dict())
    new_data["Animal Key"] = new_data["Animal ID"].map(pd.Series(data = animal_dimension.index, index = animal_dimension["Animal ID"]).to_dict())
    new_data["Outcome Subtype Key"] = new_data["Outcome Subtype"].map(pd.Series(data = outcome_subtype_dimension.index, index = outcome_subtype_dimension["Outcome Subtype"]).to_dict())
    new_data["Age Key"] = new_data["Age upon Outcome"].map(pd.Series(data = age_dimension.index, index = age_dimension["Age upon Outcome"]).to_dict())
    new_data["Date Key"] = new_data["DateTime"].map(pd.Series(data = date_dimension.index, index = date_dimension["DateTime"]).to_dict())

    # Extract data for the facts table
    facts_table = new_data[["Outcome Type Key", "Animal Key", "Outcome Subtype Key", "Age Key", "Date Key"]]

    # Create a list of output DataFrames
    output_dfs = [animal_dimension, outcome_type_dimension, outcome_subtype_dimension, age_dimension, date_dimension, facts_table]

    return output_dfs

# Function to load data into a PostgreSQL database
def load_data(dfs):
    # Define the database connection URL
    db_url = "postgresql+psycopg2://ashwathi:sunbeam@db:5432/shelter"
    conn = create_engine(db_url)

    # Load each DataFrame into the database
    dfs[0].to_sql("Animal Dimension", conn, if_exists="append", index=True, index_label="Animal Key")
    dfs[1].to_sql("Outcome Type Dimension", conn, if_exists="append", index=True, index_label="Outcome Type Key")
    dfs[2].to_sql("Outcome Subtype Dimension", conn, if_exists="append", index=True, index_label="Outcome Subtype Key")
    dfs[3].to_sql("Age Dimension", conn, if_exists="append", index=True, index_label="Age Key")
    dfs[4].to_sql("Date Dimension", conn, if_exists="append", index=True, index_label="Date Key")
    dfs[5].to_sql("Fact Animal Outcome", conn, if_exists="append", index=False)

# Main section
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    args = parser.parse_args()

    print("Starting...")
    # Extract data from the source CSV
    df = extract_data(args.source)
    # Transform the data into the required format
    new_dfs = transform_data(df)
    # Load the transformed data into the PostgreSQL database
    load_data(new_dfs)
    print("Complete!!")
