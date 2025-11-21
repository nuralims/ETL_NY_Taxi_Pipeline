from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
import os
from fastparquet import ParquetFile
import wget

default_args = {
    "owner": "your name",
    "start_date": datetime(2025, 11, 13),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "your.email@example.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def postgres_connect():
    try:
        pg_hook = PostgresHook(postgres_conn_id='airflow_db')
        engine = create_engine(pg_hook.get_uri())
        logging.info("Successfully connected to Postgres")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Postgres: {e}")
        

def bigquery_connect():
    try:
        key_path = "/opt/airflow/secrets/<file>.json"
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logging.info("Successfully connected to BigQuery")
        return client
    except Exception as e:
        logging.error(f"Error connecting to BigQuery: {e}")
        raise

def insert_parquet_to_postgres():
    try:
        file_path = "/opt/airflow/data/yellow_tripdata_2025-01.parquet"
        logging.info(f"Reading PARQUET file from: {file_path}")

        pf = ParquetFile(file_path)
        total_row_groups = len(pf.row_groups)
        logging.info(f"Total row groups: {total_row_groups}")

        engine = postgres_connect()

        first = True
        batch_size = 1000

        for rg_index, df_chunk in enumerate(pf.iter_row_groups()):
            total_rows = len(df_chunk)
            logging.info(
                f"[RG {rg_index+1}/{total_row_groups}] Total rows: {total_rows}"
            )
            
            if first:
                df_chunk.columns = [c.lower() for c in df_chunk.columns]
                df_chunk.head(0).to_sql(
                    name="raw_yellow_taxi_data",
                    con=engine,
                    if_exists="replace",
                    index=False,
                )
                first = False

            
            for i in range(0, total_rows, batch_size):
                df_batch = df_chunk.iloc[i: i + batch_size]

                df_batch.to_sql(
                    name="raw_yellow_taxi_data",
                    con=engine,
                    if_exists="append",
                    index=False,
                )

                logging.info(
                    f"[RG {rg_index+1}] Inserted batch {i//batch_size + 1} "
                    f"({len(df_batch)} rows)"
                )

        logging.info("All row groups inserted in 1000-row batches!")

    except Exception as e:
        logging.error(f"Error inserting parquet into Postgres: {e}")
        raise

def transfer_data_postgres_to_bigquery(batch_size: int = 50000):
    try:
        pg_engine = postgres_connect()

        bq_client = bigquery_connect()
        table_id = "<dataset>.ny_taxi.fact_trips_yellow_taxi_data"

        query = "SELECT * FROM public_mart.fact_trips LIMIT 500"

        total_rows_loaded = 0
        chunk_index = 0

        for chunk_df in pd.read_sql(query, con=pg_engine, chunksize=batch_size):
            chunk_index += 1

            write_disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if chunk_index == 1
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition
            )

            logging.info(
                f"Loading chunk {chunk_index} with {len(chunk_df)} rows to BigQuery "
                f"(write_disposition={write_disposition})"
            )

            job = bq_client.load_table_from_dataframe(
                chunk_df,
                table_id,
                job_config=job_config
            )
            job.result()

            total_rows_loaded += len(chunk_df)
            logging.info(f"Finished chunk {chunk_index}, total loaded so far: {total_rows_loaded} rows")

        logging.info(f"Successfully transferred {total_rows_loaded} records to BigQuery")

    except Exception as e:
        logging.error(f"Error transferring data: {e}")
        raise


with DAG(
    'ny_taxi_etl_pg_bq',
    default_args=default_args,
    description='Transfer data from CSV to BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract_data = BashOperator(
    task_id="extract_data",
    bash_command="""
        wget -O /opt/airflow/data/yellow_tripdata_2025-01.parquet \
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    """
)
    data_to_pg = PythonOperator(
        task_id="extract_data_to_pg",
        python_callable=insert_parquet_to_postgres,
    )
    
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --select stg_yellow_tripdata"
        )
    )
    
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --select fact_trips"
        )
    )
    
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transfer_data_postgres_to_bigquery,
        op_kwargs={"batch_size": 50000},
    )
    
    extract_data >> data_to_pg >> dbt_run_staging >> dbt_run_marts >> transform_data
