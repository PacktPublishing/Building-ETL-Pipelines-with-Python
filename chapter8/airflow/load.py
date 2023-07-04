import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the ETL steps as Python functions:
def extract_crashes():
    return pd.read_csv('data/traffic_crashes.csv')

def extract_vehicles():
    return pd.read_csv('data/traffic_crash_vehicle.csv')

def extract_people():
    return pd.read_csv('data/traffic_crash_people.csv')

def transform_crashes(df_crashes):
    df_crashes = df_crashes[['CRASH_RECORD_ID', 'NUM_UNITS', 'TOTAL_INJURIES', 'CRASH_DATE']]
    df_crashes = df_crashes.fillna(method='ffill')
    df_crashes['CRASH_RECORD_ID'] = df_crashes['CRASH_RECORD_ID'].astype(str)
    df_crashes['NUM_UNITS'] = df_crashes['NUM_UNITS'].astype(int)
    df_crashes['TOTAL_INJURIES'] = df_crashes['TOTAL_INJURIES'].astype(int)
    df_crashes = df_crashes.rename(columns={'CRASH_RECORD_ID': 'CRASH_ID'})
    return df_crashes

def transform_vehicles(df_vehicles, df_crashes):
    df_vehicles = df_vehicles[['CRASH_RECORD_ID', 'MAKE', 'MODEL', 'VEHICLE_YEAR']]
    df_vehicles = df_vehicles.fillna(method='ffill')
    df_vehicles['CRASH_RECORD_ID'] = df_vehicles['CRASH_RECORD_ID'].astype(str)
    df_vehicles['VEHICLE_YEAR'] = df_vehicles['VEHICLE_YEAR'].astype(int)
    df_vehicles = df_vehicles.rename(
        columns={'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'})
    df_vehicle = pd.merge(df_crashes, df_vehicles, on='CRASH_ID')
    return df_vehicle

def transform_people(df_people, df_vehicle):
    df_people = df_people[['CRASH_RECORD_ID', 'PERSON_ID', 'VEHICLE_ID', 'PERSON_AGE', 'PERSON_TYPE', 'PERSON_SEX']]
    df_people = df_people.fillna(method='ffill')
    df_people['CRASH_RECORD_ID'] = df_people['CRASH_RECORD_ID'].astype(str)
    df_people['VEHICLE_ID'] = df_people['VEHICLE_ID'].astype(int)
    df_people['PERSON_AGE'] = df_people['PERSON_AGE'].astype(int)
    df_crash = pd.merge(df_vehicle, df_people, on=['CRASH_ID', 'VEHICLE_ID'])
    return df_crash

def load_vehicle(df_vehicle):
    conn = psycopg2.connect(
        host=config.get('postgresql', 'host'),
        port=config.get('postgresql', 'port'),
        database=config.get('postgresql', 'database'),
        user=config.get('postgresql', 'user'),
        password=config.get('postgresql', 'password')
    )
    cur = conn.cursor()
    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS chicago_dmv.Vehicle ( 
             CRASH_UNIT_ID SERIAL PRIMARY KEY, 
             CRASH_ID TEXT(128), 
             CRASH_DATE    TIMESTAMP, 
        VEHICLE_ID INTEGER, 
        VEHICLE_MAKE TEXT(15), 
        VEHICLE_MODEL TEXT(15), 
        VEHICLE_YEAR INTEGER, 
        VEHICLE_TYPE TEXT(20) 
    ); 
""")
conn.commit()
insert(conn, df_vehicle, 'chicago_dmv.Vehicle')

def load_crash(df_crash):
    conn = psycopg2.connect(
        host=config.get('postgresql', 'host'),
        port=config.get('postgresql', 'port'),
        database=config.get('postgresql', 'database'),
        user=config.get('postgresql', 'user'),
        password=config.get('postgresql', 'password')
    )
    cur = conn.cursor()
    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS chicago_dmv.CRASH ( 
            CRASH_UNIT_ID SERIAL PRIMARY KEY, 
            CRASH_ID TEXT, 
            PERSON_ID TEXT, 
            VEHICLE_ID INTEGER, 
            NUM_UNITS NUMERIC, 
            TOTAL_INJURIES NUMERIC 
        ); 
    """)
    conn.commit()
    insert(conn, df_crash, 'chicago_dmv.CRASH')

def load_data():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

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

return dag