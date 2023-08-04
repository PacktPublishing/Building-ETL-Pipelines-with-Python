import configparser
import petl as etl
from petl.io.sources import DictSource

def load_data():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Define the pETL pipeline
    df_crashes = etl.fromdicts(DictSource({'df_crashes': 'data/traffic_crashes.csv'}))
    df_vehicles = etl.fromdicts(DictSource({'df_vehicles': 'data/traffic_crash_vehicle.csv'}))
    df_people = etl.fromdicts(DictSource({'df_people': 'data/traffic_crash_people.csv'}))
    df_crashes = etl.select(df_crashes, ('CRASH_RECORD_ID', 'NUM_UNITS', 'TOTAL_INJURIES', 'CRASH_DATE'))
    df_crashes = etl.transform(df_crashes, [
        ('filldown', 'CRASH_RECORD_ID'),
        ('convert', 'NUM_UNITS', int),
        ('convert', 'TOTAL_INJURIES', int),
        ('filldown', 'CRASH_DATE'),
        ('rename', {'CRASH_RECORD_ID': 'CRASH_ID'})
    ])
    df_vehicles = etl.select(df_vehicles, ('CRASH_RECORD_ID', 'MAKE', 'MODEL', 'VEHICLE_YEAR'))
    df_vehicles = etl.transform(df_vehicles, [
        ('filldown', 'CRASH_RECORD_ID'),
        ('convert', 'VEHICLE_YEAR', int),
        ('filldown', 'MAKE'),
        ('filldown', 'MODEL'),
        ('rename', {'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'})
    ])
    df_people = etl.select(df_people,
                           ('CRASH_RECORD_ID', 'PERSON_ID', 'VEHICLE_ID', 'PERSON_AGE', 'PERSON_TYPE', 'PERSON_SEX'))
    df_people = etl.transform(df_people, [
        ('filldown', 'CRASH_RECORD_ID'),
        ('filldown', 'PERSON_ID'),
        ('convert', 'VEHICLE_ID', int),
        ('convert', 'PERSON_AGE', int),
        ('filldown', 'PERSON_TYPE'),
        ('filldown', 'PERSON_SEX')
    ])
    df_vehicle = etl.join(df_vehicles, df_crashes, key='CRASH_ID')
    df_crash = etl.join(df_people, df_vehicle, key=('CRASH_ID', 'VEHICLE_ID'))
    df_crash = etl.aggregate(df_crash, key=('CRASH_UNIT_ID', 'CRASH_ID'), aggregation={
        'PERSON_ID': lambda rows: ','.join(set(rows)),
        'VEHICLE_ID': 'first',
        'NUM_UNITS': 'sum',
        'TOTAL_INJURIES': 'sum'
    })

etl.todb(df_vehicle, config.get('postgresql', 'dsn'), 'chicago_dmv.Vehicle', create=True, schema=None, commit=True,
         overwrite=False, error_handler=None, batch_size=None)
etl.todb(df_crash, config.get('postgresql', 'dsn'), 'chicago_dmv.CRASH', create=True, schema=None, commit=True,
         overwrite=False, error_handler=None, batch_size=None)

