# create a command-line runnable pipeline
from etl.extract import extract_data
from etl.transform import transform_data
import etl.load as load

import yaml

# import pipeline configuration
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


def run_pipeline():
    # Step 1: Extract data
    crashes_df = extract_data(config_data['crash_filepath'])
    vehicle_df = extract_data(config_data['vehicle_filepath'])
    people_df = extract_data(config_data['people_filepath'])

    # Step 2: Transform data
    crashes_transformed_df = transform_data(crashes_df)
    vehicle_transformed_df = transform_data(vehicle_df)
    people_transformed_df = transform_data(people_df)

    # Step 3: Load data
    load.load_data(df=crashes_transformed_df,
                   postgre_table=config_data['crash_table_PSQL'],
                   postgre_schema=config_data['crash_insert_PSQL'])
    load.load_data(df=vehicle_transformed_df,
                   postgre_table=config_data['vehicle_table_PSQL'],
                   postgre_schema=config_data['vehicle_insert_PSQL'])
    load.load_data(df=people_transformed_df,
                   postgre_table=config_data['people_table_PSQL'],
                   postgre_schema=config_data['people_insert_PSQL'])


if __name__ == "__main__":
    run_pipeline()
