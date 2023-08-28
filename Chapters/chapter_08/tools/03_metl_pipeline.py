# Import Modules
import yaml
import configparser
from metl import Pipeline
from metl.extractors.dataframe_extractor import DataframeExtractor
from metl.transformers.pandas_transformer import PandasTransformer
from metl.loaders.postgresql_loader import PostgreSQLLoader

# Import pipeline configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load_data() function as a pipeline using the METL framework:
def load_data():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Create the METL pipeline
    pipeline = Pipeline([

        # Extract Data with METL
        DataframeExtractor({
            'crash_df': {'path': config_data['crash_filepath']},
            'vehicle_df': {'path': config_data['vehicle_filepath']},
            'people_df': {'path': config_data['people_filepath']}
        }),

        # Transform Data with METL
        PandasTransformer({
            'crash_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': {'NUM_UNITS': 0, 'TOTAL_INJURIES': 0}}),
                ('to_datetime', {'column': 'CRASH_DATE'}),
                ('rename_columns', {'columns': config_data['crash_columns_rename_dict']})
            ],
            'vehicle_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': ''}),
                ('astype', {'column': 'VEHICLE_YEAR', 'dtype': 'Int64'}),
                ('rename_columns', {'columns': config_data['vehicle_columns_rename_dict']})
            ],
            'people_df': [
                ('drop_duplicates', []),
                ('fillna', {'value': ''}),
                ('astype', {'column': 'PERSON_AGE', 'dtype': 'Int64'}),
                ('rename_columns', {'columns': config_data['people_columns_rename_dict']})
            ]
        }),

        # Merge Transform Data with METL
        PandasTransformer({
            'vehicle_crash_df': [
                ('select_columns', {'columns': config_data['crash_columns_list']}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'vehicle_df', 'on': 'CRASH_ID'},
                    {'dataframe': 'crash_df', 'on': 'CRASH_ID'}
                ], 'how': 'left'}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'people_df', 'on': ['CRASH_ID', 'VEHICLE_ID']} ], 'how': 'left'})
            ],
            'person_crash_df': [
                ('select_columns', {'columns': config_data['vehicle_columns_list']}),
                ('merge_dataframes', {'dataframes': [
                    {'dataframe': 'people_df', 'on': ['CRASH_ID', 'VEHICLE_ID']}], 'how': 'left'}),
                ('groupby', {'by': ['CRASH_UNIT_ID', 'CRASH_ID'], 'agg': {
                    'PERSON_ID': {'function': 'join', 'kwargs': {'sep': ','}},
                    'VEHICLE_ID': {'function': 'first'},
                    'NUM_UNITS': {'function': 'sum'},
                    'TOTAL_INJURIES': {'function': 'sum'}
                }})
            ]
        }),

        # Load Data to PSQL with METL
        PostgreSQLLoader({
            'dsn': 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                host=config.get('postgresql', 'host'),
                port=config.get('postgresql', 'port'),
                database=config.get('postgresql', 'database'),
                user=config.get('postgresql', 'user'),
                password=config.get('postgresql', 'password')
            ),
            'tables': {
                config_data['vehicle_table_PSQL'] : {
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
                config_data['crash_table_PSQL']: {
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
