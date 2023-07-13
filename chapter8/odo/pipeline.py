from odo import odo
import yaml

# Import pipeline configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load_data() function as a pipeline using Odo:
def load_data():
    # Extract data
    crash_df = odo('data/traffic_crashes.csv', dshape=config_data['extract']['crash_df'])
    vehicle_df = odo('data/traffic_crash_vehicle.csv', dshape=config_data['extract']['vehicle_df'])
    people_df = odo('data/traffic_crash_people.csv', dshape=config_data['extract']['people_df'])

    # Transform data
    crash_df = crash_df.drop_duplicates()
    crash_df = crash_df.fillna({'NUM_UNITS': 0, 'TOTAL_INJURIES': 0})
    crash_df['CRASH_DATE'] = odo(crash_df['CRASH_DATE'], dshape=config_data['transform']['to_datetime'])
    crash_df = crash_df.rename(columns={'CRASH_RECORD_ID': 'CRASH_ID'})

    vehicle_df = vehicle_df.drop_duplicates()
    vehicle_df = vehicle_df.fillna('')
    vehicle_df['VEHICLE_YEAR'] = odo(vehicle_df['VEHICLE_YEAR'], dshape=config_data['transform']['astype'])
    vehicle_df = vehicle_df.rename(columns={'CRASH_RECORD_ID': 'CRASH_ID', 'MAKE': 'VEHICLE_MAKE', 'MODEL': 'VEHICLE_MODEL'})

    people_df = people_df.drop_duplicates()
    people_df = people_df.fillna('')
    people_df['PERSON_AGE'] = odo(people_df['PERSON_AGE'], dshape=config_data['transform']['astype'])

    df_vehicle = odo(config_data['transform']['df_vehicle'], dshape=config_data['transform']['df_vehicle'])
    df_crash = odo(config_data['transform']['df_crash'], dshape=config_data['transform']['df_crash'])

    # Load data
    odo(df_vehicle, config_data['load']['dsn'], table=config_data['load']['tables']['chicago_dmv.Vehicle'], if_exists='replace')
    odo(df_crash, config_data['load']['dsn'], table=config_data['load']['tables']['chicago_dmv.CRASH'], if_exists='replace')

# Call the load_data() function to execute the data pipeline
load_data()
