import configparser
from metl import Pipeline
from metl.loaders.postgresql_loader import PostgreSQLLoader
from metl.extractors.dataframe_extractor import DataframeExtractor
from metl.transformers.pandas_transformer import PandasTransformer
import yaml

# Import pipeline configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load_data() function as a pipeline using the METL framework:
def load_data():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Create the METL pipeline
    pipeline = Pipeline([
        DataframeExtractor({
            'crash_df': {'path': config_data['crash_filepath']},
            'vehicle_df': {'path': config_data['vehicle_filepath'],
            'people_df': {'path': config_data['people_filepath']}
        }),
        PandasTransformer({
            'crash_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': {'NUM_UNITS': 0, 'TOTAL_INJURIES': 0}}),
                ('to_datetime', {'column': 'CRASH_DATE'}),
                ('rename_columns', {'columns': {'CRASH_RECORD_ID': 'CRASH_ID'}})
            ],
            'vehicle_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': ''}),
                ('astype', {'column': 'VEHICLE_YEAR', 'dtype': 'Int64'}),
                ('rename_columns', {'columns': {'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'}})
            ],
            'people_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': ''}),
                ('astype', {'column': 'PERSON_AGE', 'dtype': 'Int64'})
            ]
        }),
        PandasTransformer({
            'df_vehicle': [
                ('select_columns', {'columns': ['CRASH_UNIT_ID', 'CRASH_ID', 'CRASH_DATE', 'VEHICLE_ID', 'VEHICLE_MAKE',
                                                'VEHICLE_MODEL', 'VEHICLE_YEAR', 'VEHICLE_TYPE']}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'vehicle_df', 'on': 'CRASH_ID'},
                    {'dataframe': 'crash_df', 'on': 'CRASH_ID'}
                ], 'how': 'left'}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'people_df', 'on': ['CRASH_ID', 'VEHICLE_ID']}
                ], 'how': 'left'})
            ],
            'df_crash': [
                ('select_columns',
                 {'columns': ['CRASH_UNIT_ID', 'CRASH_ID', 'PERSON_ID', 'VEHICLE_ID', 'NUM_UNITS', 'TOTAL_INJURIES']}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'people_df', 'on': ['CRASH_ID', 'VEHICLE_ID']}
                ], 'how': 'left'}),
                ('groupby', {'by': ['CRASH_UNIT_ID', 'CRASH_ID'], 'agg': {
                    'PERSON_ID': {'function': 'join', 'kwargs': {'sep': ','}},
                    'VEHICLE_ID': {'function': 'first'},
                    'NUM_UNITS': {'function': 'sum'},
                    'TOTAL_INJURIES': {'function': 'sum'}
                }})
            ]
        }),
        PostgreSQLLoader({
            'dsn': 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                host=config.get('postgresql', 'host'),
                port=config.get('postgresql', 'port'),
                database=config.get('postgresql', 'database'),
                user=config.get('postgresql', 'user'),
                password=config.get('postgresql', 'password')
            ),
            'tables': {
                'chicago_dmv.Vehicle': {
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
                'chicago_dmv.CRASH': {
                    'primary_key': ['CRASH_UNIT_ID', 'CRASH_ID'],
                    'columns': {
                        'CRASH_UNIT_ID': {'type': 'int'},
                        'CRASH_ID': {'type': 'string'},
                        'PERSON_ID': {'type': 'string'},
                        'VEHICLE_ID': {'type': 'int'},
                        'NUM_UNITS': {'type': 'float'},
                        'TOTAL_INJURIES': {'type': 'float'}
                    }
                }
            }
        })
    ])

    # Run the METL pipeline
    pipeline.run()

# Call the load_data() function to execute the data pipeline
load_data()
