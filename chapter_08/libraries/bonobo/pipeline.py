import psycopg2
import configparser
import bonobo
from extract import extract_data
from transform import transform_data

import yaml

# import pipeline configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


# Define the load process as a Bonobo graph
def load(data):
    # Extract and transform the data
    data = transform_data(extract_data())
    df_vehicle = data['df_vehicle']
    df_crash = data['df_crash']

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Connect to the database
    conn = psycopg2.connect(
        host=config.get('postgresql', 'host'),
        port=config.get('postgresql', 'port'),
        database=config.get('postgresql', 'database'),
        user=config.get('postgresql', 'user'),
        password=config.get('postgresql', 'password')
    )

    # Define the Postgresql query to insert data into the vehicle table
    insert_vehicle_query = '''INSERT INTO chicago_dmv.Vehicle (CRASH_UNIT_ID, CRASH_ID, CRASH_DATE, VEHICLE_ID, VEHICLE_MAKE, VEHICLE_MODEL, VEHICLE_YEAR, VEHICLE_TYPE) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''

    # Convert the dataframe to a list of tuples
    data_vehicle = [tuple(x) for x in df_vehicle.values]

    # Execute the Postgresql query to insert data into the vehicle table
    with conn.cursor() as cur:
        cur.executemany(insert_vehicle_query, data_vehicle)
        conn.commit()

    # Define the Postgresql query to insert data into the crash table
    insert_crash_query = '''INSERT INTO chicago_dmv.CRASH (CRASH_UNIT_ID, CRASH_ID, PERSON_ID, VEHICLE_ID, NUM_UNITS, TOTAL_INJURIES) 
                            VALUES (%s, %s, %s, %s, %s, %s);'''

    # Convert the dataframe to a list of tuples
    data_crash = [tuple(x) for x in df_crash.values]

    # Execute the Postgresql query to insert data into the crash table
    with conn.cursor() as cur:
        cur.executemany(insert_crash_query, data_crash)
        conn.commit()

    # Close the database connection
    conn.close()


# Define the Bonobo pipeline
def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract_data, transform_data, load)
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
