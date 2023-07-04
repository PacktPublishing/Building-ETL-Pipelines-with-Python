# import modules
import pandas as pd

# transform data
def transform_data(dataframe_list):
    """
       Simple Transformation Function in Python with Error Handling
       :param dataframe_list: list of dataframes imported in the following order: [crash, vehicle, people]
       :output:
    """

    # read extracted traffic crashes, traffic vehicles, and people
    crashes_df = dataframe_list[0]
    vehicles_df = dataframe_list[1]
    people_df = dataframe_list[2]

    # drop duplicate rows
    df = df.drop_duplicates()

    # handle missing values

    # replace missing values in numeric columns with the mean
    df.fillna(df.mean(), inplace=True)

    # replace missing values in categorical columns with the mode
    df.fillna(df.mode().iloc[0], inplace=True)

    # convert columns to appropriate data types
    df['CRASH_DATE'] = pd.to_datetime(df['CRASH_DATE'], format='%m/%d/%Y')
    df['POSTED_SPEED_LIMIT'] = df ['POSTED_SPEED_LIMIT'].astype('int32')

    # merge the three dataframes into a single dataframe
    merge_01_df = pd.merge(df, df2, on='CRASH_RECORD_ID')
    all_data_df = pd.merge(merge_01_df, df3, on='CRASH_RECORD_ID')

    # drop unnecessary columns
    df = df[['CRASH_UNIT_ID', 'CRASH_ID', 'CRASH_DATE', 'VEHICLE_ID', 'VEHICLE_MAKE', 'VEHICLE_MODEL',
             'VEHICLE_YEAR', 'VEHICLE_TYPE', 'PERSON_ID', 'PERSON_TYPE', 'PERSON_SEX', 'PERSON_AGE',
             'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH', 'DATE_POLICE_NOTIFIED']]