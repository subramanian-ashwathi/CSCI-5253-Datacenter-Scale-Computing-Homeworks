from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data, load_fact_data
from etl_scripts.extract import extract_data

airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
csv_target_dir = airflow_home + "/data/{{ ds }}/downloads"
csv_target_file = csv_target_dir + "/outcomes_{{ ds }}.csv"

pq_target_dir = airflow_home + "/data/{{ ds }}/processed"

with DAG(
    dag_id="outcomes_dag", start_date=datetime(2023, 11, 1), schedule_interval="@daily"
) as dag:

    extract = PythonOperator(
        task_id = "extract",
        python_callable = extract_data,
        provide_context=True,
        op_kwargs = {
            "output_dir": csv_target_dir,
            "output_file": csv_target_file
        }
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            "source_csv": csv_target_file,
            "target_dir": pq_target_dir,
        },
    )

    load_animal_dim = PythonOperator(
        task_id="load_animal_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": pq_target_dir + "/animal_dimension.parquet",
            "table_name": "Animal Dimension",
            "key": "Animal Key",
        },
    )

    load_outcome_type_dim = PythonOperator(
        task_id="load_outcome_type_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": pq_target_dir + "/outcome_type_dimension.parquet",
            "table_name": "Outcome Type Dimension",
            "key": "Outcome Type Key",
        },
    )

    load_outcome_subtype_dim = PythonOperator(
        task_id="load_outcome_subtype_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": pq_target_dir + "/outcome_subtype_dimension.parquet",
            "table_name": "Outcome Subtype Dimension",
            "key": "Outcome Subtype Key",
        },
    )

    load_date_dim = PythonOperator(
        task_id="load_date_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": pq_target_dir + "/date_dimension.parquet",
            "table_name": "Date Dimension",
            "key": "Date Key",
        },
    )

    load_age_dim = PythonOperator(
        task_id="load_age_dim",
        python_callable=load_data,
        op_kwargs={
            "table_file": pq_target_dir + "/age_dimension.parquet",
            "table_name": "Age Dimension",
            "key": "Age Key",
        },
    )

    load_fact_animal_outcome = PythonOperator(
        task_id="load_fact_animal_outcome",
        python_callable=load_fact_data,
        op_kwargs={
            "table_file": pq_target_dir + "/facts_table.parquet",
            "table_name": "Fact Animal Outcome",
        },
    )

    (
        extract
        >> transform
        >> [
            load_animal_dim,
            load_outcome_type_dim,
            load_outcome_subtype_dim,
            load_date_dim,
            load_age_dim,
        ]
        >> load_fact_animal_outcome
    )
