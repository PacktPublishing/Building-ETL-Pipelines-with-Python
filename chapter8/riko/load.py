import configparser
from riko.modules import csv, select, merge, groupby, transform
from riko.sources import dict as riko_dict
from riko.sinks import sql as riko_sql

def load_data():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Define the Riko pipeline
    pipeline = transform(
        riko_dict({
            'df_crashes': {'url': 'data/traffic_crashes.csv'},
            'df_vehicles': {'url': 'data/traffic_crash_vehicle.csv'},
            'df_people': {'url': 'data/traffic_crash_people.csv'}
        }),
        [
            select(
                {'columns': ['CRASH_RECORD_ID', 'NUM_UNITS', 'TOTAL_INJURIES', 'CRASH_DATE']},
                keys=['df_crashes']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': {'NUM_UNITS': 0, 'TOTAL_INJURIES': 0}}},
                    {'to_datetime': {'column': 'CRASH_DATE'}},
                    {'rename_columns': {'columns': {'CRASH_RECORD_ID': 'CRASH_ID'}}}
                ],
                keys=['df_crashes']
            ),
            select(
                {'columns': ['CRASH_RECORD_ID', 'MAKE', 'MODEL', 'VEHICLE_YEAR']},
                keys=['df_vehicles']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': ''}},
                    {'astype': {'column': 'VEHICLE_YEAR', 'dtype': 'Int64'}},
                    {'rename_columns': {
                        'columns': {'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'}}}
                ],
                keys=['df_vehicles']
            ),
            select(
                {'columns': ['CRASH_RECORD_ID', 'PERSON_ID', 'VEHICLE_ID', 'PERSON_AGE', 'PERSON_TYPE', 'PERSON_SEX']},
                keys=['df_people']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': ''}},
                    {'astype': {'column': 'PERSON_AGE', 'dtype': 'Int64'}}
                ],
                keys=['df_people']
            ),
            merge(
                {'dataframes': ['df_vehicles', 'df_crashes'], 'on': 'CRASH_ID', 'how': 'left'},
                keys=['df_vehicle']
            ),
            merge(
                {'dataframes': ['df_people', 'df_vehicle'], 'on': ['CRASH_ID', 'VEHICLE_ID'], 'how': 'left'},
                keys=['df_crash']
            ),
            groupby(
                {'by': ['CRASH_UNIT_ID', 'CRASH_ID'], 'agg': {
                    'PERSON_ID': {'function': 'join', 'kwargs': {'sep': ','}},
                    'VEHICLE_ID': {'function': 'first'},
                    'NUM_UNITS': {'function': 'sum'}, 'TOTAL_INJURIES': {'function': 'sum'}
                }},
                keys=['df_crash']
            ),
            riko_sql(
                {
                    'table': 'chicago_dmv.Vehicle',
                    'dsn': 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                        host=config.get('postgresql', 'host'),
                        port=config.get('postgresql', 'port'),
                        database=config.get('postgresql', 'database'),
                        user=config.get('postgresql', 'user'),
                        password=config.get('postgresql', 'password')
                    ),
                    'primary_key': ['CRASH_UNIT_ID'],
                    'columns': {
                        'CRASH_UNIT_ID': {'type': 'int'},
                        'CRASH_ID': {'type': 'string'},
                        'CRASH_DATE': {'type': 'datetime'},
                        'VEHICLE_ID': {'type': 'int'},
                        'VEHICLE_MAKE': {'type': 'string'},
                        'VEHICLE_MODEL': {'type': 'string'},
                        'VEHICLE_YEAR': {'type': 'int'},
                        'VEHICLE_TYPE': {'type': 'string'}
                    }
                },
                keys=['df_vehicle']
            ),
            riko_sql(
                {
                    'table': 'chicago_dmv.CRASH',
                    'dsn': 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                        host=config.get('postgresql', 'host'),
                        port=config.get('postgresql', 'port'),
                        database=config.get('postgresql', 'database'),
                        user=config.get('postgresql', 'user'),
                        password=config.get('postgresql', 'password')
                    ),
                    'primary_key': ['CRASH_UNIT_ID', 'CRASH_ID'],
                    'columns': {
                        'CRASH_UNIT_ID': {'type': 'int'},
                        'CRASH_ID': {'type': 'string'},
                        'PERSON_ID': {'type': 'string'},
                        'VEHICLE_ID': {'type': 'int'},
                        'NUM_UNITS': {'type': 'float'},
                        'TOTAL_INJURIES': {'type': 'float'}
                    }
                },
                keys=['df_crash']
            )
        ]
    )

# Execute the Riko pipeline
for item in pipeline:
    pass