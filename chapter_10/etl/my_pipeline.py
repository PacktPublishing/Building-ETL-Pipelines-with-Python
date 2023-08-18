
# Use Bonobo to define the pipeline workflow and dependencies: 

@use("extract") 

@use("transform") 

@use("load") 

 

 

# Define the functions for data extraction, transformation, and loading using #Bonobo syntax: 

 

def extract(): 

    crashes_df = pd.read_csv("s3://my-bucket-name/traffic/traffic_crashes.csv") 

  ## repeat for vehicles_df and people_df 

    yield crashes_df, vehicles_df, people_df 

 

def transform(crashes_df, vehicles_df, people_df): 

    # Merge the dataframes on the appropriate columns 

    crash_vehicles_df = pd.merge(crashes_df, vehicles_df, on='CRASH_RECORD_ID', how='left') 

    merged_all_df = pd.merge(crash_vehicles_df, people_df, on='CRASH_RECORD_ID', how='left') 

    # Filter the columns to only include the ones needed for the database tables 

    crash_df = merged_all_df[['CRASH_UNIT_ID_x', 'CRASH_RECORD_ID', 'CRASH_DATE_x', 'PERSON_ID', 'VEHICLE_ID', 'NUM_UNITS', 'TOTAL_INJURIES']] 

    vehicle_df = merged_all_df[['CRASH_UNIT_ID_x', 'CRASH_RECORD_ID', 'CRASH_DATE_x', 'VEHICLE_ID', 'VEHICLE_MAKE', 'VEHICLE_MODEL', 'VEHICLE_YEAR', 'VEHICLE_TYPE']] 

    person_df =  merged_all_df[['CRASH_RECORD_ID', 'CRASH_DATE_x', 'PERSON_ID', 'PERSON_TYPE', 'VEHICLE_ID', 'PERSON_SEX', 'PERSON_AGE']] 

    yield crash_df, vehicle_df, person_df 

 

def load(crash_df, vehicle_df, person_df): 

    # Connect to the database 

    conn = psycopg2.connect( 

        host="mydbinstance.cmyxyz123.us-east-1.rds.amazonaws.com", 

        port=5432, 

        user= <YOUR POSTGRE USERNAME>,

        password= <YOUR POSTGRE PASSWORD>,

        database= <YOUR POSTGRE DATABASE>

    ) 

    cur = conn.cursor() 

    # Load the data into the appropriate tables 

    crash_df.to_sql('CRASH', conn, if_exists='append', index=False) 

     ## repeat for vehicles_df and people_df 

    cur.close() 

    conn.close() 

    yield NOT_MODIFIED 

# Use Bonobo to define the pipeline workflow and dependencies: 

@use("extract") 

@use("transform") 

@use("load") 

def graph(extract, transform, load): 

    extract_crashes, extract_vehicles, extract_people = extract() 

    transform_crash, transform_vehicle, transform
