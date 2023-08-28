# Import Modules
import yaml
import configparser
import petl as etl
from petl.io.sources import DictSource

# Import file configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

def load_data():

    # Read the Configuration File
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Define the pETL pipeline
    crash_path = etl.fromdicts(DictSource({'crash_df': config_data['crash_filepath']}))
    vehicle_path = etl.fromdicts(DictSource({'vehicle_df': config_data['crash_filepath']}))
    people_path = etl.fromdicts(DictSource({'people_df': config_data['people_filepath']}))

    # Extract Crash Data with pETL
    crash_df = etl.select(crash_path, tuple(config_data['crash_columns_list']))

    # Transform Crash Data with pETL
    transform_crash_df = etl.transform(crash_df, [('filldown', 'CRASH_RECORD_ID'),
                                                ('convert', 'NUM_UNITS', int),
                                                ('convert', 'TOTAL_INJURIES', int),
                                                ('filldown', 'CRASH_DATE'),
                                                ('rename', config_data['crash_columns_rename_dict'])])

    # Extract Vehicle Data with pETL
    vehicle_df = etl.select(vehicle_path,  tuple(config_data['vehicle_columns_list']))

    # Transform Vehicle Data with pETL
    transform_vehicle_df = etl.transform(vehicle_df, [('filldown', 'CRASH_RECORD_ID'),
                                                    ('convert', 'VEHICLE_YEAR', int),
                                                    ('filldown', 'MAKE'),
                                                    ('filldown', 'MODEL'),
                                                    ('rename', config_data['vehicle_columns_rename_dict'])])

    # Extract People Data with pETL
    people_df = etl.select(people_path, tuple(config_data['people_columns_list']))

    # Transform People Data with pETL
    transform_people_df = etl.transform(people_df, [('filldown', 'CRASH_RECORD_ID'),
                                                    ('filldown', 'PERSON_ID'),
                                                    ('convert', 'VEHICLE_ID', int),
                                                    ('convert', 'PERSON_AGE', int),
                                                    ('filldown', 'PERSON_TYPE'),
                                                    ('filldown', 'PERSON_SEX'),
                                                    ('rename', config_data['people_columns_rename_dict'])])

    # Prepare Data for Output
    vehicle_crash_df = etl.join(transform_vehicle_df, transform_crash_df, key='CRASH_ID')
    person_crash_df = etl.join(transform_people_df, transform_vehicle_df, key=('CRASH_ID', 'VEHICLE_ID'))
    person_crash_df = etl.aggregate(person_crash_df, key=('CRASH_UNIT_ID', 'CRASH_ID'),
                                    aggregation={'PERSON_ID': lambda rows: ','.join(set(rows)),
                                                'VEHICLE_ID': 'first',
                                                'NUM_UNITS': 'sum',
                                                'TOTAL_INJURIES': 'sum'})

    etl.todb(vehicle_crash_df, config.get('postgresql', 'dsn'), config_data['vehicle_table_PSQL'],
            create=True, schema=None, commit=True, overwrite=False, error_handler=None, batch_size=None)
    etl.todb(person_crash_df, config.get('postgresql', 'dsn'), config_data['crash_table_PSQL'],
             create=True, schema=None, commit=True,overwrite=False, error_handler=None, batch_size=None)
