# Import Modules
import luigi
import pandas as pd
from Chapters.chapter_08 import extract_data
from Chapters.chapter_08 import (
    transform_crash_data,
    transform_vehicle_data,
    transform_people_data
)
from Chapters.chapter_08.etl.load import load_data

import yaml

# import pipeline configuration
with open('../config.yaml', 'r') as file:
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
        crash_df = pd.read_csv(self.input().path)
        load_data(df=crash_df,
                  create_PSQL=config_data['crash_create_PSQL'],
                  insert_PSQL=config_data['crash_insert_PSQL'])


class LoadVehicles(luigi.Task):

    def requires(self):
        return TransformVehicles()

    def run(self):
        people_df = pd.read_csv(self.input().path)
        load_data(df=people_df,
                  create_PSQL=config_data['people_create_PSQL'],
                  insert_PSQL=config_data['people_insert_PSQL'])


class LoadPeople(luigi.Task):

    def requires(self):
        return TransformPeople()

    def run(self):
        vehicle_df = pd.read_csv(self.input().path)
        load_data(df=vehicle_df,
                  create_PSQL=config_data['vehicle_create_PSQL'],
                  insert_PSQL=config_data['vehicle_insert_PSQL'])


class ChicagoDMV(luigi.Task):
    def requires(self):
        return [LoadCrashes(), LoadVehicles(), LoadPeople()]


if __name__ == '__main__':
    luigi.run()
