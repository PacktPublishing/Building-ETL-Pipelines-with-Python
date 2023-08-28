# etl_pipeline.py
import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_CONNECTION, TABLE_NAME, FILE_PATH

def extract_data(file_path):
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error occurred during data extraction: {e}")
        raise
    return df

def transform_data(df):
    try:
        df = df.dropna()
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
        df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    except Exception as e:
        print(f"Error occurred during data transformation: {e}")
        raise
    return df

def load_data(df, table_name, database_connection):
    try:
        engine = create_engine(database_connection)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error occurred during data loading: {e}")
        raise

def run_etl_pipeline():
    df = extract_data(FILE_PATH)
    df = transform_data(df)
    load_data(df, TABLE_NAME, DATABASE_CONNECTION)
