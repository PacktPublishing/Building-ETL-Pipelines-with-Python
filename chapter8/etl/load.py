# import relevant modules
import psycopg2

def init_conn():
    """
    Initialize Postgre connection
    :return: postgre cursor object
    """
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
    print('successful creation of cursor object.')

    return conn, cur

# suggested continued learning: this function can be modified to be fully dynamic
def load_data(cur, df, postgre_table, postgre_schema):
    """
    Load transformed data into respective PostgreSQL Table
    :param cur: posgre cursor object
    :return: cursor object
    """
    insert_query = f"INSERT INTO {postgre_table} {postgre_schema};"

    # insert transformed data into PostgreSQL table
    # TODO: REFACTOR TO MAKE SENSE - VERY SLOW / POOR USE OF CPUs
    for index, row in df.iterrows():

        if postgre_table == 'chicago_dmv.Crash':
            insert_values = (row['CRASH_UNIT_ID'],
                              row['CRASH_ID'],
                              row['PERSON_ID'],
                              row['VEHICLE_ID'],
                              row['NUM_UNITS'],
                              row['TOTAL_INJURIES'])

        elif postgre_table == 'chicago_dmv.Vehicle':
            insert_values = (row['CRASH_UNIT_ID'],
                              row['CRASH_ID'],
                              row['CRASH_DATE'],
                              row['VEHICLE_ID'],
                              row['VEHICLE_MAKE'],
                              row['VEHICLE_MODEL'],
                              row['VEHICLE_YEAR'],
                              row['VEHICLE_TYPE'])

        elif postgre_table == 'chicago_dmv.Person':
            insert_values = (row['PERSON_ID'],
                              row['CRASH_ID'],
                              row['CRASH_DATE'],
                              row['PERSON_TYPE'],
                              row['VEHICLE_ID'],
                              row['PERSON_SEX'],
                              row['PERSON_AGE'])

        else:
            raise ValueError(f'Postgre Data Table {postgre_table} does not exist in this pipeline.')

        # Insert data int
        cur.execute(insert_query, insert_values)

    # Commit all changes to the database
    conn.commit()

    # Close the cursor and database connection
    cur.close()
    conn.close()

def close_conn(cur):
    """
    Closing Postgre connection
    :param cur: posgre cursor object
    :return: none
    """

    # Close the cursor and database connection
    cur.close()
    print('successful closing of cursor object.')
