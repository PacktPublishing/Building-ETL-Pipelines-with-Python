from datetime import datetime, timedelta
import configparser
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Define the ETL steps as Python functions:
def extract_crashes():
    return pd.read_csv('data/traffic_crashes.csv')

def extract_vehicles():
    return pd.read_csv('data/traffic_crash_vehicle.csv')

def extract_people():
    return pd.read_csv('data/traffic_crash_people.csv')


def transform_crashes(df_crashes):
    # Transformation logic for crashes data
    transformed_crashes = df_crashes[['CRASH_RECORD_ID', 'NUM_UNITS', 'TOTAL_INJURIES', 'CRASH_DATE']]
    transformed_crashes = transformed_crashes.fillna(method='ffill')
    transformed_crashes['CRASH_RECORD_ID'] = transformed_crashes['CRASH_RECORD_ID'].astype(str)
    transformed_crashes['NUM_UNITS'] = transformed_crashes['NUM_UNITS'].astype(int)
    transformed_crashes['TOTAL_INJURIES'] = transformed_crashes['TOTAL_INJURIES'].astype(int)
    transformed_crashes = transformed_crashes.rename(columns={'CRASH_RECORD_ID': 'CRASH_ID'})
    return transformed_crashes


def transform_vehicles(df_vehicles, df_crashes):
    # Transformation logic for vehicles data
    transformed_vehicles = df_vehicles[['CRASH_RECORD_ID', 'MAKE', 'MODEL', 'VEHICLE_YEAR']]
    transformed_vehicles = transformed_vehicles.fillna(method='ffill')
    transformed_vehicles['CRASH_RECORD_ID'] = transformed_vehicles['CRASH_RECORD_ID'].astype(str)
    transformed_vehicles['VEHICLE_YEAR'] = transformed_vehicles['VEHICLE_YEAR'].astype(int)
    transformed_vehicles = transformed_vehicles.rename(
        columns={'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'})
    transformed_vehicle = pd.merge(df_crashes, transformed_vehicles, on='CRASH_ID')
    return transformed_vehicle


def transform_people(df_people, df_vehicle):
    # Transformation logic for people data
    transformed_people = df_people[
        ['CRASH_RECORD_ID', 'PERSON_ID', 'VEHICLE_ID', 'PERSON_AGE', 'PERSON_TYPE', 'PERSON_SEX']]
    transformed_people = transformed_people.fillna(method='ffill')
    transformed_people['CRASH_RECORD_ID'] = transformed_people['CRASH_RECORD_ID'].astype(str)
    transformed_people['VEHICLE_ID'] = transformed_people['VEHICLE_ID'].astype(int)
    transformed_people['PERSON_AGE'] = transformed_people['PERSON_AGE'].astype(int)
    transformed_crash = pd.merge(df_vehicle, transformed_people, on=['CRASH_ID', 'VEHICLE_ID'])
    return transformed_crash


def load_vehicle(df_vehicle):
    # Load data into the vehicle table
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        -- SQL query to create or update the vehicle table
    """)
    conn.commit()
    # Insert data into the vehicle table using df_vehicle and psycopg2


def load_crash(df_crash):
    # Load data into the crash table
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        -- SQL query to create or update the crash table
    """)
    conn.commit()
    # Insert data into the crash table using df_crash and psycopg2


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
    python_callable=extract_crashes,
    dag=dag
)
task_extract_vehicles = PythonOperator(
    task_id='extract_vehicles',
    python_callable=extract_vehicles,
    dag=dag
)
task_extract_people = PythonOperator(
    task_id='extract_people',
    python_callable=extract_people,
    dag=dag
)
task_transform_crashes = PythonOperator(
    task_id='transform_crashes',
    python_callable=transform_crashes,
    op_kwargs={'df_crashes': "{{ task_instance.xcom_pull(task_ids='extract_crashes') }}"},
    dag=dag
)
task_transform_vehicles = PythonOperator(
    task_id='transform_vehicles',
    python_callable=transform_vehicles,
    op_kwargs={
        'df_vehicles': "{{ task_instance.xcom_pull(task_ids='extract_vehicles') }}",
        'df_crashes': "{{ task_instance.xcom_pull(task_ids='transform_crashes') }}"
    },
    dag=dag
)
task_transform_people = PythonOperator(
    task_id='transform_people',
    python_callable=transform_people,
    op_kwargs={
        'df_people': "{{ task_instance.xcom_pull(task_ids='extract_people') }}",
        'df_vehicle': "{{ task_instance.xcom_pull(task_ids='transform_vehicles') }}"
    },
    dag=dag
)
task_load_vehicle = PythonOperator(
    task_id='load_vehicle',
    python_callable=load_vehicle,
    op_kwargs={'df_vehicle': "{{ task_instance.xcom_pull(task_ids='transform_vehicles') }}"},
    dag=dag
)
task_load_crash = PythonOperator(
    task_id='load_crash',
    python_callable=load_crash,
    op_kwargs={'df_crash': "{{ task_instance.xcom_pull(task_ids='transform_people') }}"},
    dag=dag
)

# Define the task dependencies
task_extract_crashes >> task_transform_crashes
task_extract_vehicles >> task_transform_vehicles
task_extract_people >> task_transform_people
task_transform_vehicles >> task_transform_people
task_transform_people >> task_load_vehicle
task_transform_people >> task_load_crash
