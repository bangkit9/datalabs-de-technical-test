import pandas as pd
import logging
from typing import Dict, Any, Generator
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from sqlalchemy.engine import Engine

# --- Konstanta Global ---
POSTGRES_CONN_ID: str = "postgres_staging_db"
STAGING_SCHEMA: str = "staging"
SOURCE_DATA_PATH: Path = Path("/opt/airflow/data")

# --- Konstanta Lokal (Pindah ke Fungsi) ---
POSTGRES_WRITE_CHUNK_SIZE: int = 1000 # Tetap di sini, atau didefinisikan di task utama

TABLE_FILE_MAP: Dict[str, str] = {
    "stg_customers": "customers.csv",
    "stg_products": "products.csv",
    "stg_marketing_campaigns": "marketing_campaigns.csv",
    "stg_transactions": "transactions.csv",
    "stg_transaction_items": "transaction_items.csv",
}

def get_data_chunks(csv_file: str) -> Generator[pd.DataFrame, None, None]:
    # Memindahkan definisi CHUNK_SIZE ke lokal
    PANDAS_READ_CHUNK_SIZE: int = 50000 
    
    filepath = SOURCE_DATA_PATH / csv_file
    logging.info(f"Reading CSV from {filepath} with chunk size {PANDAS_READ_CHUNK_SIZE}...")
    
    try:
        return pd.read_csv(
            filepath, 
            sep=',', 
            chunksize=PANDAS_READ_CHUNK_SIZE, 
            encoding='utf-8'
        )
    except Exception as e:
        logging.error(f"FATAL: Error reading CSV file {csv_file}. Details: {e}")
        raise

def transform_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    for col in chunk.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            chunk[col] = pd.to_datetime(
                chunk[col], 
                errors='coerce', 
                infer_datetime_format=True
            )
    return chunk

@task
def load_csv_to_staging(table_name: str, csv_file: str):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine: Engine = hook.get_sqlalchemy_engine()
    
    # Mendefinisikan konstanta DB write di sini
    WRITE_CHUNK_SIZE: int = 1000
    
    total_rows_loaded: int = 0
    is_first_chunk: bool = True

    try:
        data_chunks = get_data_chunks(csv_file)
        
        for chunk in data_chunks:
            
            chunk = transform_chunk(chunk)
            
            load_method: str = "replace" if is_first_chunk else "append"
            
            logging.info(
                f"Loading chunk ({len(chunk)} rows) to {STAGING_SCHEMA}.{table_name} "
                f"using '{load_method}' method..."
            )
            
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=STAGING_SCHEMA,
                if_exists=load_method,
                index=False,
                chunksize=WRITE_CHUNK_SIZE
            )
            
            total_rows_loaded += len(chunk)
            is_first_chunk = False
            
    except Exception as e:
        logging.error(f"Critical failure during load process for {table_name}: {e}")
        raise

    logging.info(f"SUCCESS: Total {total_rows_loaded} rows loaded into {STAGING_SCHEMA}.{table_name}.")

@dag(
    dag_id="task_1_load_to_staging",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'staging', 'ingestion']
)
def load_staging_data_dag():
    start = EmptyOperator(task_id="start_ingestion")
    
    load_tasks = []
    for table_name, csv_file in TABLE_FILE_MAP.items():
        load_tasks.append(
            load_csv_to_staging.override(
                task_id=f"load_{table_name}"
            )(table_name=table_name, csv_file=csv_file)
        )

    end = EmptyOperator(task_id="end_ingestion")

    start >> load_tasks >> end

load_staging_data_dag()