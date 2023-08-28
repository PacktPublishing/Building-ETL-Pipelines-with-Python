# Import Modules
import yaml
import bonobo

# Import ETL Activities
from Chapters.chapter_08 import extract_data
from Chapters.chapter_08 import (
    transform_crash_data,
    transform_vehicle_data,
    transform_people_data
)
from Chapters.chapter_08.etl.load import load_data

# Import Configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Step 1: Extract data
def extract_all_data() -> list:
    crashes_df = extract_data(filepath=config_data['crash_filepath'],
                              select_cols=config_data['crash_columns_list'],
                              rename_cols=config_data['crash_columns_rename_dict'])
    vehicle_df = extract_data(filepath=config_data['vehicle_filepath'],
                              select_cols=config_data['vehicle_columns_list'],
                              rename_cols=config_data['vehicle_columns_rename_dict'])
    people_df = extract_data(filepath=config_data['people_filepath'],
                              select_cols=config_data['people_columns_list'],
                              rename_cols=config_data['people_columns_rename_dict'])
    return [crashes_df, vehicle_df, people_df]

# Step 2: Transform Data
def transform_all_data(data: list) -> list:
    transformed_crashes_df = transform_crash_data(data[0])
    transformed_vehicle_df = transform_vehicle_data(data[1])
    transformed_people_df = transform_people_data(data[2])
    return [transformed_crashes_df, transformed_vehicle_df, transformed_people_df]

# Step 3: Load Data
def load_all_data(transformed_data: list) -> list:
    load_data(df=transformed_data[0],
              create_PSQL=config_data['crash_create_PSQL'],
              insert_PSQL=config_data['crash_insert_PSQL'])
    load_data(df=transformed_data[1],
              create_PSQL=config_data['vehicle_create_PSQL'],
              insert_PSQL=config_data['vehicle_insert_PSQL'])
    load_data(df=transformed_data[2],
              create_PSQL=config_data['people_create_PSQL'],
              insert_PSQL=config_data['people_insert_PSQL'])

# Define the Bonobo pipeline
def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract_all_data, transform_all_data, load_all_data)
    return graph

# Define the main function to run the Bonobo pipeline
def main():
    # Set the options for the Bonobo pipeline
    options = {
        'services': [],
        'plugins': [],
        'log_level': 'INFO',
        'log_handlers': [bonobo.logging.StreamHandler()],
        'use_colors': True,
        'graph': get_graph()
    }
    # Run the Bonobo pipeline
    bonobo.run(**options)

if __name__ == '__main__':
    main()
