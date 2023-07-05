import luigi
import pandas as pd
import psycopg2
import configparser


class ExtractCrashes(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/traffic_crashes.csv')

    def run(self):
        df_crashes = pd.read_csv(self.input().path)
        df_crashes = df_crashes[
            ['CRASH_RECORD_ID', 'CRASH_DATE_EST_I', 'CRASH_DATE', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH',
             'LATITUDE', 'LONGITUDE', 'NUM_UNITS', 'INJURIES_TOTAL']]
        df_crashes = df_crashes.rename(columns={
            'CRASH_RECORD_ID': 'CRASH_ID',
            'INJURIES_TOTAL': 'TOTAL_INJURIES'
        })
        df_crashes.to_csv(self.output().path, index=False)


class ExtractVehicles(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/traffic_crash_vehicle.csv')

    def run(self):
        df_vehicles = pd.read_csv(self.input().path)
        df_vehicles = df_vehicles[
            ['CRASH_UNIT_ID', 'CRASH_RECORD_ID', 'CRASH_DATE', 'VEHICLE_ID', 'MAKE', 'MODEL', 'YEAR', 'VEHICLE_TYPE']]

        df_vehicles = df_vehicles.rename(columns={
            'CRASH_RECORD_ID': 'CRASH_ID',
            'MAKE': 'VEHICLE_MAKE',
            'MODEL': 'VEHICLE_MODEL',
            'YEAR': 'VEHICLE_YEAR',
            'VEHICLE_TYPE': 'VEHICLE_TYPE'
        })
        df_vehicles.to_csv(self.output().path, index=False)


class ExtractPeople(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/traffic_crash_people.csv')

    def run(self):
        df_people = pd.read_csv(self.input().path)
        df_people = df_people[['PERSON_ID', 'CRASH_RECORD_ID', 'CRASH_DATE', 'PERSON_TYPE', 'VEHICLE_ID', 'SEX', 'AGE']]
        df_people = df_people.rename(columns={
            'CRASH_RECORD_ID': 'CRASH_ID',
            'PERSON_TYPE': 'PERSON_TYPE',
            'SEX': 'PERSON_SEX',
            'AGE': 'PERSON_AGE'
        })
        df_people.to_csv(self.output().path, index=False)


class TransformCrashes(luigi.Task):

    def requires(self):
        return ExtractCrashes()

    def output(self):
        return luigi.LocalTarget('data/transformed_crashes.csv')

    def run(self):
        df_crashes = pd.read_csv(self.input().path)
        df_crashes['CRASH_DATE'] = pd.to_datetime(df_crashes['CRASH_DATE'])
        df_crashes = df_crashes[df_crashes['CRASH_DATE_EST_I'] != 'Y']
        df_crashes = df_crashes[df_crashes['LATITUDE'].notnull() & df_crashes['LONGITUDE'].notnull()]
        df_crashes = df_crashes.drop(columns=['CRASH_DATE_EST_I'])
        df_crashes.to_csv(self.output().path, index=False)


class TransformVehicles(luigi.Task):
    def requires(self):
        return ExtractVehicles()

    def output(self):
        return luigi.LocalTarget('data/transformed_vehicles.csv')

    def run(self):
        df_vehicles = pd.read_csv(self.input().path)
        df_vehicles['VEHICLE_MAKE'] = df_vehicles['VEHICLE_MAKE'].str.upper()
        df_vehicles['VEHICLE_MODEL'] = df_vehicles['VEHICLE_MODEL'].str.upper()
        df_vehicles = df_vehicles[df_vehicles['VEHICLE_YEAR'].notnull()]
        df_vehicles.to_csv(self.output().path, index=False)


class TransformPeople(luigi.Task):

    def requires(self):
        return ExtractPeople()

    def output(self):
        return luigi.LocalTarget('data/transformed_people.csv')

    def run(self):
        df_people = pd.read_csv(self.input().path)
        df_people = df_people[df_people['PERSON_TYPE'].isin(['DRIVER', 'PASSENGER', 'PEDESTRIAN', 'BICYCLE', 'OTHER'])]
        df_people = df_people[df_people['PERSON_AGE'].notnull()]
        df_people.to_csv(self.output().path, index=False)


class LoadCrashes(luigi.Task):

    def requires(self):
        return TransformCrashes()

    def run(self):
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
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS chicago_dmv.CRASH (CRASH_ID TEXT, CRASH_DATE TIMESTAMP, CRASH_HOUR INTEGER, CRASH_DAY_OF_WEEK INTEGER, CRASH_MONTH INTEGER, LATITUDE NUMERIC, LONGITUDE NUMERIC, NUM_UNITS INTEGER, TOTAL_INJURIES INTEGER)')
        conn.commit()
        df_crashes = pd.read_csv(self.input().path)
        for row in df_crashes.itertuples(index=False):
            cursor.execute('INSERT INTO chicago_dmv.CRASH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
        conn.commit()
        cursor.close()
        conn.close()


class LoadVehicles(luigi.Task):

    def requires(self):
        return TransformVehicles()

    def run(self):
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
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS chicago_dmv.VEHICLE (CRASH_UNIT_ID INTEGER, CRASH_ID TEXT, CRASH_DATE TIMESTAMP, VEHICLE_ID INTEGER, VEHICLE_MAKE TEXT, VEHICLE_MODEL TEXT, VEHICLE_YEAR INTEGER, VEHICLE_TYPE TEXT)')
        conn.commit()
        df_vehicles = pd.read_csv(self.input().path)
        for row in df_vehicles.itertuples(index=False):
            cursor.execute('INSERT INTO chicago_dmv.VEHICLE VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', row)
        conn.commit()
        cursor.close()
        conn.close()


class LoadPeople(luigi.Task):

    def requires(self):
        return TransformPeople()

    def run(self):
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
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS chicago_dmv.PERSON (PERSON_ID TEXT, CRASH_ID TEXT, CRASH_DATE TIMESTAMP, PERSON_TYPE TEXT, VEHICLE_ID INTEGER, PERSON_SEX TEXT, PERSON_AGE INTEGER)')
        conn.commit()
        df_people = pd.read_csv(self.input().path)
        for row in df_people.itertuples(index=False):
            cursor.execute('INSERT INTO chicago_dmv.PERSON VALUES (%s, %s, %s, %s, %s, %s, %s)', row)

        conn.commit()
        cursor.close()
        conn.close()


class ChicagoDMV(luigi.Task):
    def requires(self):
        return [LoadCrashes(), LoadVehicles(), LoadPeople()]


if __name__ == '__main__':
    luigi.run()
