# import dependent modules
import pandas as pd

# extract data
def extract_data(crash_filepath="data/traffic_crashes.csv",
                 vehicle_filepath="data/traffic_crash_vehicle.csv",
                 people_filepath="data/traffic_crash_people.csv"):
    """
       Simple Extract Function in Python with Error Handling
       :param crash_filepath: str, file path to chicago crash data CSV
       :param vehicle_filepath: str, file path to chicago vehicle data CSV
       :param people_filepath: str, file path to chicago people data CSV
       :output: list, list of dataframes imported in the following order: [crash, vehicle, people]
    """
    try:
        # Read the traffic crashes CSV file and store it in a dataframe
        df_crashes = pd.read_csv(crash_filepath)

        # Read the traffic crash vehicle CSV file and store it in a dataframe
        df_vehicles = pd.read_csv(vehicle_filepath)

        # Read the traffic crash People CSV file and store it in a dataframe
        df_people = pd.read_csv(people_filepath)

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    return [df_crashes, df_vehicles, df_people]
