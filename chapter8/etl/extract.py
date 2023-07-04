# import dependent modules
import pandas as pd

# extract data
def extract_data(filepath):
    """
       Simple Extract Function in Python with Error Handling
       :param cfilepath: str, file path to CSV data
       :output: dataframes, imported from CSV data
    """
    try:
        # Read the CSV file and store it in a dataframe
        df = pd.read_csv(filepath)

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    return df
