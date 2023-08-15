# Import modules
import pandas as pd
from chapter_08.etl.transform import transform_data

# Transform Crash Data
def transform_crash_data(crashes_df):
    crashes_df['CRASH_DATE'] = pd.to_datetime(crashes_df['CRASH_DATE'])
    crashes_df = crashes_df[crashes_df['CRASH_DATE_EST_I'] != 'Y']
    crashes_df = crashes_df[crashes_df['LATITUDE'].notnull() & crashes_df['LONGITUDE'].notnull()]
    crashes_df = crashes_df.drop(columns=['CRASH_DATE_EST_I'])
    crashes_df = transform_data(crashes_df)
    return crashes_df

# Transform Vehicle Data
def transform_vehicle_data(vehicles_df):
    vehicles_df['VEHICLE_MAKE'] = vehicles_df['VEHICLE_MAKE'].str.upper()
    vehicles_df['VEHICLE_MODEL'] = vehicles_df['VEHICLE_MODEL'].str.upper()
    vehicles_df = vehicles_df[vehicles_df['VEHICLE_YEAR'].notnull()]
    vehicles_df = transform_data(vehicles_df)
    return vehicles_df

# Transform People Data
def transform_people_data(people_df):
    people_df = people_df[people_df['PERSON_TYPE'].isin(['DRIVER', 'PASSENGER', 'PEDESTRIAN', 'BICYCLE', 'OTHER'])]
    people_df = people_df[people_df['PERSON_AGE'].notnull()]
    people_df = transform_data(people_df)
    return people_df
