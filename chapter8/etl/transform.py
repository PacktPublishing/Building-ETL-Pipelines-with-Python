# import modules
import pandas as pd

# transform data
def transform_data(df):
    """
       Simple Transformation Function in Python with Error Handling
       :param df: pandas dataframe, extracted data
       :output: pandas dataframe, transformed data
    """

    # drop duplicate rows
    df = df.drop_duplicates()

    # replace missing values in numeric columns with the mean
    df.fillna(df.mean(), inplace=True)

    # replace missing values in categorical columns with the mode
    df.fillna(df.mode().iloc[0], inplace=True)

    # convert columns to appropriate data types
    try:
        df['CRASH_DATE'] = pd.to_datetime(df['CRASH_DATE'], format='%m/%d/%Y')
    except:
        pass

    try:
        df['POSTED_SPEED_LIMIT'] = df ['POSTED_SPEED_LIMIT'].astype('int32')
    except:
        pass

    # # merge the three dataframes into a single dataframe
    # merge_01_df = pd.merge(df, df2, on='CRASH_RECORD_ID')
    # all_data_df = pd.merge(merge_01_df, df3, on='CRASH_RECORD_ID')
    #
    # # drop unnecessary columns
    # df = df[['CRASH_UNIT_ID', 'CRASH_ID', 'CRASH_DATE', 'VEHICLE_ID', 'VEHICLE_MAKE', 'VEHICLE_MODEL',
    #          'VEHICLE_YEAR', 'VEHICLE_TYPE', 'PERSON_ID', 'PERSON_TYPE', 'PERSON_SEX', 'PERSON_AGE',
    #          'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH', 'DATE_POLICE_NOTIFIED']]
    return df