# Import Modules
import yaml
import odo
import configparser
from Chapters.chapter_08 import extract_data
from Chapters.chapter_08 import (
    transform_crash_data,
    transform_vehicle_data,
    transform_people_data
)

# Import database configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load_data() function as a pipeline using Odo:
def main():

    # Step 1: Extract Data with odo
    crash_df = odo.odo(extract_data(filepath=config_data['crash_filepath'],
                                    select_cols=config_data['crash_columns_list'],
                                    rename_cols=config_data['crash_columns_rename_dict']))
    vehicle_df = odo.odo(extract_data(filepath=config_data['vehicle_filepath'],
                                      select_cols=config_data['vehicle_columns_list'],
                                      rename_cols=config_data['vehicle_columns_rename_dict']))
    people_df = odo.odo(extract_data(filepath=config_data['people_filepath'],
                                     select_cols=config_data['people_columns_list'],
                                     rename_cols=config_data['people_columns_rename_dict']))

    # Step 2: Transform Data
    transformed_crashes_df = odo.odo(transform_crash_data(crash_df),
                                     dshape=config_data['transform']['transformed_crashes_df'])
    transformed_vehicle_df = odo.odo(transform_vehicle_data(vehicle_df),
                                     dshape=config_data['transform']['transformed_vehicle_df'])
    transformed_people_df = odo.odo(transform_people_data(people_df),
                                     dshape=config_data['transform']['transformed_people_df'])

    # Config for PostgreSQL
    config = configparser.ConfigParser()
    config.read('config.ini')
    postgre_config = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
        host=config['POSTGRESQL']['host'],
        port=config['POSTGRESQL']['port'],
        dbname=config['POSTGRESQL']['database'],
        user=config['POSTGRESQL']['user'],
        password=config['POSTGRESQL']['password']
    )

    # Step 3: Load Data to PSQL with odo
    odo.odo(transformed_crashes_df,
            config=postgre_config,
            dshape=config_data['crash_create_PSQL'],
            table=config_data['crash_table_PSQL'],
            if_exists='replace')
    odo.odo(transformed_vehicle_df,
            config=postgre_config,
            dshape=config_data['vehicle_create_PSQL'],
            table=config_data['vehicle_table_PSQL'],
            if_exists='replace')
    odo.odo(transformed_people_df,
            config=postgre_config,
            dshape=config_data['people_create_PSQL'],
            table=config_data['people_table_PSQL'],
            if_exists='replace')

# Call the load_data() function to execute the data pipeline
if __name__ == '__main__':
    main()
