# import dependent modules
from odo import odo
import yaml

# Import database configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# extract data
def extract_data(filepath: object) -> object:
    """
       Simple Extract Function in Python with Error Handling
       :param filepath: str, file path to CSV data
       :output: pandas dataframe, extracted from CSV data
    """
    try:
        # Read the CSV file and store it in a dataframe
        crash_df = odo('data/traffic_crashes.csv', dshape=config_data['extract']['crash_df'])
        vehicle_df = odo('data/traffic_crash_vehicle.csv', dshape=config_data['extract']['vehicle_df'])
        people_df = odo('data/traffic_crash_people.csv', dshape=config_data['extract']['people_df'])

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    return df
