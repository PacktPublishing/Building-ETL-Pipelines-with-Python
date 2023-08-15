# Import Modules
import yaml
from odo import odo
from chapter_08.etl.transform import (
    transform_crash_data,
    transform_vehicle_data,
    transform_people_data
)

# Import database configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load_data() function as a pipeline using Odo:
def main():

    # Step 1: Extract Data with odo
    crash_df = odo('data/traffic_crashes.csv', dshape=config_data['extract']['crash_df'])
    vehicle_df = odo('data/traffic_crash_vehicle.csv', dshape=config_data['extract']['vehicle_df'])
    people_df = odo('data/traffic_crash_people.csv', dshape=config_data['extract']['people_df'])

    # Step 2: Transform Data
    transformed_crashes_df = transform_crash_data(crash_df)
    transformed_vehicle_df = transform_vehicle_data(vehicle_df)
    transformed_people_df = transform_people_data(people_df)

    # Step 3: Load Data to PSQL with odo
    odo(transformed_crashes_df, config_data['load']['dsn'],
        table=config_data['load']['tables']['chicago_dmv.CRASH'], if_exists='replace')
    odo(transformed_vehicle_df, config_data['load']['dsn'],
        table=config_data['load']['tables']['chicago_dmv.Vehicle'], if_exists='replace')
    odo(transformed_people_df, config_data['load']['dsn'],
        table=config_data['load']['tables']['chicago_dmv.PEOPLE'], if_exists='replace')

# Call the load_data() function to execute the data pipeline
if __name__ == '__main__':
    main()
