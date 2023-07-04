# import relevant modules
import psycopg2

def load_data(data, table_name):

    # establish connection to the Postgresql database
    conn = psycopg2.connect(
        database="your_database_name",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    # create a cursor object for running SQL queries
    cur = conn.cursor()

    # chicago_dmv.Vehicle PSQL
    insert_query_vehicle = '''INSERT INTO chicago_dmv.Vehicle       
                            (CRASH_UNIT_ID,  
                            CRASH_ID,  
                            CRASH_DATE,  
                            VEHICLE_ID,  
                            VEHICLE_MAKE,  
                            VEHICLE_MODEL,  
                            VEHICLE_YEAR,  
                            VEHICLE_TYPE) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''

    # chicago_dmv.Person PSQL
    insert_query_person = '''INSERT INTO chicago_dmv.Person (PERSON_ID,  
                            CRASH_ID,  
                            CRASH_DATE,  
                            PERSON_TYPE,  
                            VEHICLE_ID,  
                            PERSON_SEX,  
                            PERSON_AGE) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s);'''

    # chicago_dmv.Crash PSQL
    insert_query_crash = '''INSERT INTO chicago_dmv.Crash (CRASH_UNIT_ID,  
                            CRASH_ID,  
                            PERSON_ID,  
                            VEHICLE_ID,  
                            NUM_UNITS,  
                            TOTAL_INJURIES) 
                            VALUES (%s, %s, %s, %s, %s, %s);'''

    for index, row in df.iterrows():
        # vehicles
        values_vehicle = (row['CRASH_UNIT_ID'],
                          row['CRASH_ID'],
                          row['CRASH_DATE'],
                          row['VEHICLE_ID'],
                          row['VEHICLE_MAKE'],
                          row['VEHICLE_MODEL'],
                          row['VEHICLE_YEAR'],
                          row['VEHICLE_TYPE'])

        # Insert data int
        cur.execute(insert_query_vehicle, values_vehicle)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and database connection
    cur.close()
    conn.close()