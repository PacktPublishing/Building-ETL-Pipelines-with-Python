# Import modules
import psycopg2
import configparser
import yaml

# Import database configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load process as a Bonobo graph
def load_data(df: object, create_PSQL: str, insert_PSQL: str) -> object:

    config = configparser.ConfigParser()
    config.read('config.ini')
    conn = psycopg2.connect(
        host=config['POSTGRESQL']['host'],
        port=config['POSTGRESQL']['port'],
        dbname=config['POSTGRESQL']['database'],
        user=config['POSTGRESQL']['user'],
        password=config['POSTGRESQL']['password']
    )
    cursor = conn.cursor()
    cursor.execute(create_PSQL)
    conn.commit()

    for row in df.itertuples(index=False):
        cursor.execute(insert_PSQL, row)

    conn.commit()
    cursor.close()
    conn.close()
