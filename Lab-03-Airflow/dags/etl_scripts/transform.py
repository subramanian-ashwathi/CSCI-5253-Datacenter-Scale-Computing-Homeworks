# Import necessary libraries
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import argparse
import os
from pathlib import Path
import tempfile
from dags.etl_scripts.google_storage import download_from_gcs, upload_to_gcs, BUCKET_NAME, CREDENTIALS_PATH

api_to_csv_column_map = {
    "animal_id": "Animal ID",
    "name": "Name",
    "datetime": "DateTime",
    "monthyear": "MonthYear",
    "date_of_birth": "Date of Birth",
    "outcome_type": "Outcome Type",
    "outcome_subtype": "Outcome Subtype",
    "animal_type": "Animal Type",
    "sex_upon_outcome": "Sex upon Outcome",
    "age_upon_outcome": "Age upon Outcome",
    "breed": "Breed",
    "color": "Color",
}

# Function to transform the extracted data
def transform_data(source_csv, target_dir):
    # Create a copy of the data
    # tmp_file = tempfile.TemporaryFile()
    # tmp_filename = tmp_file.name 
    print("Downloading from Google Storage...")
    download_from_gcs(BUCKET_NAME, source_csv, source_csv, CREDENTIALS_PATH)
    new_data = pd.read_csv(source_csv)
    new_data = new_data.rename(columns=api_to_csv_column_map)

    print(new_data.shape)
    # tmp_file.close()

    # Replace NaN values in the 'Name' column with "N/A"
    new_data["Name"] = new_data["Name"].replace(np.nan, "N/A")
    # Replace * in the 'Name' column with empty string
    new_data["Name"] = new_data["Name"].str.replace("*", "")

    # Extract 'Month' and 'Year' columns from 'MonthYear' 
    new_data["Month"] = pd.DatetimeIndex(new_data['MonthYear']).month
    new_data["Year"] = pd.DatetimeIndex(new_data['MonthYear']).year

    # Create a new 'Sex' column and handle 'Unknown' values
    new_data["Sex"] = new_data["Sex upon Outcome"].replace("Unknown", np.nan)

    # Split 'Sex' into 'Property' and 'Sex' columns
    new_data[["Property", "Sex"]] = new_data["Sex"].str.split(" ", expand=True)

    # Drop unnecessary columns
    new_data.drop(columns=["MonthYear", "Sex upon Outcome"], inplace=True)

    # Extract relevant columns for dimension tables
    animal_dimension = new_data[
        [
            "Animal ID",
            "Name",
            "Date of Birth",
            "Animal Type",
            "Property",
            "Sex",
            "Breed",
            "Color",
        ]
    ]
    outcome_type_dimension = new_data[["Outcome Type"]].drop_duplicates()
    outcome_subtype_dimension = new_data[["Outcome Subtype"]].drop_duplicates()
    age_dimension = new_data[["Age upon Outcome"]].drop_duplicates()
    date_dimension = new_data[["DateTime"]].drop_duplicates()

    # Create mapping keys for dimension tables
    new_data["Outcome Type Key"] = new_data["Outcome Type"].map(
        pd.Series(
            data=outcome_type_dimension.index,
            index=outcome_type_dimension["Outcome Type"],
        ).to_dict()
    )
    new_data["Animal Key"] = new_data["Animal ID"].map(
        pd.Series(
            data=animal_dimension.index, index=animal_dimension["Animal ID"]
        ).to_dict()
    )
    new_data["Outcome Subtype Key"] = new_data["Outcome Subtype"].map(
        pd.Series(
            data=outcome_subtype_dimension.index,
            index=outcome_subtype_dimension["Outcome Subtype"],
        ).to_dict()
    )
    new_data["Age Key"] = new_data["Age upon Outcome"].map(
        pd.Series(
            data=age_dimension.index, index=age_dimension["Age upon Outcome"]
        ).to_dict()
    )
    new_data["Date Key"] = new_data["DateTime"].map(
        pd.Series(data=date_dimension.index, index=date_dimension["DateTime"]).to_dict()
    )

    # Extract data for the facts table
    facts_table = new_data[
        ["Outcome Type Key", "Animal Key", "Outcome Subtype Key", "Age Key", "Date Key"]
    ]

    # # Create a list of output DataFrames
    # output_dfs = [
    #     animal_dimension,
    #     outcome_type_dimension,
    #     outcome_subtype_dimension,
    #     age_dimension,
    #     date_dimension,
    #     facts_table,
    # ]

    # return output_dfs

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    animal_dimension_file = target_dir + "/animal_dimension.parquet"
    outcome_type_dimension_file = target_dir + "/outcome_type_dimension.parquet"
    outcome_subtype_dimension_file = target_dir + "/outcome_subtype_dimension.parquet"
    age_dimension_file = target_dir + "/age_dimension.parquet"
    date_dimension_file = target_dir + "/date_dimension.parquet"
    facts_file = target_dir + "/facts_table.parquet"

    animal_dimension.to_parquet(animal_dimension_file)
    outcome_type_dimension.to_parquet(outcome_type_dimension_file)
    outcome_subtype_dimension.to_parquet(outcome_subtype_dimension_file)
    age_dimension.to_parquet(age_dimension_file)
    date_dimension.to_parquet(date_dimension_file)
    facts_table.to_parquet(facts_file)

    upload_to_gcs(BUCKET_NAME, animal_dimension_file, animal_dimension_file, CREDENTIALS_PATH)
    upload_to_gcs(BUCKET_NAME, outcome_type_dimension_file, outcome_type_dimension_file, CREDENTIALS_PATH)
    upload_to_gcs(BUCKET_NAME, outcome_subtype_dimension_file, outcome_subtype_dimension_file, CREDENTIALS_PATH)
    upload_to_gcs(BUCKET_NAME, age_dimension_file, age_dimension_file, CREDENTIALS_PATH)
    upload_to_gcs(BUCKET_NAME, date_dimension_file, date_dimension_file, CREDENTIALS_PATH)
    upload_to_gcs(BUCKET_NAME, facts_file, facts_file, CREDENTIALS_PATH)

    


