# Import necessary libraries
from sqlalchemy import create_engine

# suggested continued learning: this function can be modified to be fully dynamic
def load_data(ecommerce_df: object):
    """
    Load transformed data into respective PostgreSQL Table
    :param cur: posgre cursor object
    :return: n/a
    """
    # Create a connection to the PostgreSQL database
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')

    # Load the DataFrame into the database as a new table
    ecommerce_df.to_sql('EcommerceData', engine, if_exists='replace', index=False)
    print('Data successfully written to PSQL.')


