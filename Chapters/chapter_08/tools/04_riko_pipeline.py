# Import Modules
import yaml
import configparser
from riko.modules import csv, select, merge, groupby, transform
from riko.sources import dict as riko_dict
from riko.sinks import sql as riko_sql

# Import file configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


def pipeline():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Define the Riko pipeline
    pipeline = transform( # import all files
        riko_dict({
            'crash_df': {'url': config_data['crash_filepath']},
            'vehicle_df': {'url': config_data['vehicle_filepath']},
            'people_df': {'url': config_data['people_filepath']}
        }),
        [
            # Select and Transform Crash Data
            select( 
                {'columns': config_data['crash_columns_list']},
                keys=['crash_df']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': {'NUM_UNITS': 0, 'TOTAL_INJURIES': 0}}},
                    {'to_datetime': {'column': 'CRASH_DATE'}},
                    {'rename_columns': {'columns': config_data['crash_columns_rename_dict']}}
                ],
                keys=['crash_df']
            ),

            # Select and Transform Vehicle Data
            select(
                {'columns': config_data['vehicle_columns_list']},
                keys=['vehicle_df']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': ''}},
                    {'astype': {'column': 'VEHICLE_YEAR', 'dtype': 'Int64'}},
                    {'rename_columns': {
                        'columns': config_data['vehicle_columns_rename_dict']}}
                ],
                keys=['vehicle_df']
            ),

            # Select and Transform People Data
            select(
                {'columns': config_data['people_columns_list']},
                keys=['people_df']
            ),
            transform(
                [
                    {'drop_duplicates': []},
                    {'fillna': {'value': ''}},
                    {'astype': {'column': 'PERSON_AGE', 'dtype': 'Int64'}}
                ],
                keys=['people_df']
            ),

            # Merge and Transform Crash and Vehicle Data
            merge(
                {'dataframes': ['vehicle_df', 'crash_df'], 'on': 'CRASH_ID', 'how': 'left'},
                keys=['merged_crash_vehicle_df']
            ),
            # Merge and Transform Vehicle and People Data
            merge(
                {'dataframes': ['people_df', 'vehicle_df'], 'on': ['CRASH_ID', 'VEHICLE_ID'], 'how': 'left'},
                keys=['merged_people_vehicle_df']
            ),

            # Group and Transform 'Crash-Vehicle' Merge Data
            groupby(
                {'by': ['CRASH_UNIT_ID', 'CRASH_ID'], 'agg': {
                    'PERSON_ID': {'function': 'join', 'kwargs': {'sep': ','}},
                    'VEHICLE_ID': {'function': 'first'},
                    'NUM_UNITS': {'function': 'sum'}, 'TOTAL_INJURIES': {'function': 'sum'}
                }},
                keys=['grouped_merged_crash_vehicle_df']
            ),

            # Connect to PSQL and Import 'Crash-Person' Merge Data
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
                keys=['psql_merged_people_vehicle_df']
            ),
            
            # Connect to PSQL and Import Grouped 'Crash-Vehicle' Merge Data
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
                keys=['psql_grouped_merged_crash_vehicle_df']
            )
        ]
    )

    # Execute the Riko pipeline
    for item in pipeline:
        pass