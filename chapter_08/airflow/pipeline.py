from datetime import datetime, timedelta
import configparser
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import yaml

# import pipeline configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the ETL steps as Python functions:
def extract_data(filepath):
    return pd.read_csv(filepath)

def transform_crashes(crash_df):
    # Transformation logic for crashes data
    transformed_crashes = crash_df[['CRASH_RECORD_ID', 'NUM_UNITS', 'TOTAL_INJURIES', 'CRASH_DATE']]
    transformed_crashes = transformed_crashes.fillna(method='ffill')
    transformed_crashes['CRASH_RECORD_ID'] = transformed_crashes['CRASH_RECORD_ID'].astype(str)
    transformed_crashes['NUM_UNITS'] = transformed_crashes['NUM_UNITS'].astype(int)
    transformed_crashes['TOTAL_INJURIES'] = transformed_crashes['TOTAL_INJURIES'].astype(int)
    transformed_crashes = transformed_crashes.rename(columns={'CRASH_RECORD_ID': 'CRASH_ID'})
    return transformed_crashes

def transform_vehicles(vehicle_df, crash_df):
    # Transformation logic for vehicles data
    transformed_vehicles = vehicle_df[['CRASH_RECORD_ID', 'MAKE', 'MODEL', 'VEHICLE_YEAR']]
    transformed_vehicles = transformed_vehicles.fillna(method='ffill')
    transformed_vehicles['CRASH_RECORD_ID'] = transformed_vehicles['CRASH_RECORD_ID'].astype(str)
    transformed_vehicles['VEHICLE_YEAR'] = transformed_vehicles['VEHICLE_YEAR'].astype(int)
    transformed_vehicles = transformed_vehicles.rename(
        columns={'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'})
    transformed_vehicle = pd.merge(crash_df, transformed_vehicles, on='CRASH_ID')
    return transformed_vehicle

def transform_people(people_df, vehicle_df):
    # Transformation logic for people data
    transformed_people = people_df[
        ['CRASH_RECORD_ID', 'PERSON_ID', 'VEHICLE_ID', 'PERSON_AGE', 'PERSON_TYPE', 'PERSON_SEX']]
    transformed_people = transformed_people.fillna(method='ffill')
    transformed_people['CRASH_RECORD_ID'] = transformed_people['CRASH_RECORD_ID'].astype(str)
    transformed_people['VEHICLE_ID'] = transformed_people['VEHICLE_ID'].astype(int)
    transformed_people['PERSON_AGE'] = transformed_people['PERSON_AGE'].astype(int)
    transformed_crash = pd.merge(vehicle_df, transformed_people, on=['CRASH_ID', 'VEHICLE_ID'])
    return transformed_crash

def load_vehicle(vehicle_df):
    # Load data into the vehicle table
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        -- SQL query to create or update the vehicle table
    """)
    conn.commit()
    # Insert data into the vehicle table using vehicle_df and psycopg2


def load_crash(crash_df):
    # Load data into the crash table
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        -- SQL query to create or update the crash table
    """)
    conn.commit()
    # Insert data into the crash table using crash_df and psycopg2


def get_db_connection():
    # Get database connection based on the configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    conn = psycopg2.connect(
        host=config.get('postgresql', 'host'),
        port=config.get('postgresql', 'port'),
        database=config.get('postgresql', 'database'),
        user=config.get('postgresql', 'user'),
        password=config.get('postgresql', 'password')
    )
    return conn


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 0
}
dag = DAG('chicago_dmv', default_args=default_args, schedule_interval=timedelta(days=1))

# Define the tasks
task_extract_crashes = PythonOperator(
    task_id='extract_crashes',
    python_callable=extract_data(config_data['crash_filepath']),
    dag=dag
)
task_extract_vehicles = PythonOperator(
    task_id='extract_vehicles',
    python_callable=extract_data(config_data['vehicle_filepath']),
    dag=dag
)
task_extract_people = PythonOperator(
    task_id='extract_people',
    python_callable=extract_data(config_data['people_filepath']),
    dag=dag
)
task_transform_crashes = PythonOperator(
    task_id='transform_crashes',
    python_callable=transform_crashes,
    op_kwargs={'crash_df': "{{ task_instance.xcom_pull(task_ids='extract_crashes') }}"},
    dag=dag
)
task_transform_vehicles = PythonOperator(
    task_id='transform_vehicles',
    python_callable=transform_vehicles,
    op_kwargs={
        'vehicle_df': "{{ task_instance.xcom_pull(task_ids='extract_vehicles') }}",
        'crash_df': "{{ task_instance.xcom_pull(task_ids='transform_crashes') }}"
    },
    dag=dag
)
task_transform_people = PythonOperator(
    task_id='transform_people',
    python_callable=transform_people,
    op_kwargs={
        'people_df': "{{ task_instance.xcom_pull(task_ids='extract_people') }}",
        'vehicle_df': "{{ task_instance.xcom_pull(task_ids='transform_vehicles') }}"
    },
    dag=dag
)
task_load_vehicle = PythonOperator(
    task_id='load_vehicle',
    python_callable=load_vehicle,
    op_kwargs={'vehicle_df': "{{ task_instance.xcom_pull(task_ids='transform_vehicles') }}"},
    dag=dag
)
task_load_crash = PythonOperator(
    task_id='load_crash',
    python_callable=load_crash,
    op_kwargs={'crash_df': "{{ task_instance.xcom_pull(task_ids='transform_people') }}"},
    dag=dag
)

# Define the task dependencies
task_extract_crashes >> task_transform_crashes
task_extract_vehicles >> task_transform_vehicles
task_extract_people >> task_transform_people
task_transform_vehicles >> task_transform_people
task_transform_people >> task_load_vehicle
task_transform_people >> task_load_crash
