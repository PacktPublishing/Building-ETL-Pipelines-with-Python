import luigi
import psycopg2
import configparser
import pandas as pd
from chapter_08.etl.extract import extract_data
from chapter_08.etl.transform import (
    transform_crash_data,
    transform_vehicle_data,
    transform_people_data
)

import yaml

# import pipeline configuration
with open('../../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


class ExtractCrashes(luigi.Task):

    def output(self):
        return luigi.LocalTarget(config_data['crash_filepath'])

    def run(self):
        crashes_df = extract_data(filepath=config_data['crash_filepath'],
                                  select_cols=config_data['crash_columns_list'],
                                  rename_cols=config_data['crash_columns_rename_dict'])
        crashes_df.to_csv(self.output().path, index=False)


class ExtractVehicles(luigi.Task):

    def output(self):
        return luigi.LocalTarget(config_data['vehicle_filepath'])

    def run(self):
        vehicle_df = extract_data(filepath=config_data['vehicle_filepath'],
                                  select_cols=config_data['vehicle_columns_list'],
                                  rename_cols=config_data['vehicle_columns_rename_dict'])
        vehicle_df.to_csv(self.output().path, index=False)


class ExtractPeople(luigi.Task):

    def output(self):
        return luigi.LocalTarget(config_data['people_filepath'])

    def run(self):
        people_df = extract_data(filepath=config_data['people_filepath'],
                                 select_cols=config_data['people_columns_list'],
                                 rename_cols=config_data['people_columns_rename_dict'])
        people_df.to_csv(self.output().path, index=False)


class TransformCrashes(luigi.Task):

    def requires(self):
        return ExtractCrashes()

    def output(self):
        return luigi.LocalTarget('data/transformed_crashes.csv')

    def run(self):
        crash_df = pd.read_csv(self.input().path)
        transformed_crashes_df = transform_crash_data(crash_df)
        transformed_crashes_df.to_csv(self.output().path, index=False)


class TransformVehicles(luigi.Task):
    def requires(self):
        return ExtractVehicles()

    def output(self):
        return luigi.LocalTarget('data/transformed_vehicles.csv')

    def run(self):
        vehicle_df = pd.read_csv(self.input().path)
        transformed_vehicle_df = transform_vehicle_data(vehicle_df)
        transformed_vehicle_df.to_csv(self.output().path, index=False)


class TransformPeople(luigi.Task):

    def requires(self):
        return ExtractPeople()

    def output(self):
        return luigi.LocalTarget('data/transformed_people.csv')

    def run(self):
        people_df = pd.read_csv(self.input().path)
        transformed_people_df = transform_people_data(people_df)
        transformed_people_df.to_csv(self.output().path, index=False)


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
