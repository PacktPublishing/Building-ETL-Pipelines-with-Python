# Make sure below packages have been installed 
# pip install pyarrow
# pip install certifi

import urllib3
from urllib3 import request
import certifi
import json
import sqlite3
import pandas as pd
import logging

import logging

# define top level module logger
logger = logging.getLogger(__name__)

def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        logger.info(f'{parquet_file_name} : extracted {df_parquet.shape[0]} records from the parguet file')
    except Exception as e:
        logger.exception( f'{parquet_file_name} : - exception {e} encountered while extracting the parguet file')
        df_parquet = pd.DataFrame()
    return df_parquet


def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
        logger.info(f'{csv_file_name} : extracted {df_csv.shape[0]} records from the csv file')
    except Exception as e:
        logger.exception(f'{csv_file_name} : - exception {e} encountered while extracting the csv_file_name file')
        df_csv = pd.DataFrame()
    return df_csv

def source_data_from_api(api_endpoint):
    try:
        # Check if API is available to retrive the data
        # Sometimes we get certificate error . We should never silence this error as this may cause a securirty threat.
        # Create a Pool manager that can be used to read the API response 
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        api_response = http.request('GET', api_endpoint)
        apt_status = api_response.status
        if apt_status == 200:
            logger.info(f'{apt_status} - ok : while invoking the api {api_endpoint}')
            data = json.loads(api_response.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
            logger.info(f'{apt_status}- extracted {df_api.shape[0]} records from the csv file')
        else:
            logger.error(f'{apt_status}- error : while invoking the api {api_endpoint}')
            df_api = pd.Dataframe()
    except Exception as e:
        logger.exception(f'{apt_status} : - exception {e} encountered while reading data from the api')
        df_api = pd.DataFrame()
    return df_api

def source_data_from_table(db_name, table_name):
    try:
        # Read sqlite query results into a pandas DataFrame
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql(f"SELECT * from {table_name}", conn)
            logger.info(f'{db_name}- read {df_table.shape[0]} records from the table: {table_name}')
    except Exception as e:
        logger.exception(f'{db_name} : - exception {e} encountered while reading data from the table: {table_name}')                 
        df_table = pd.DataFrame()
    return df_table


def source_data_from_webpage(web_page_url,matching_keyword):
    try:
        # Read webpage table into a pandas DataFrame
        df_html = pd.read_html(web_page_url,match = matching_keyword)
        df_html = df_html[0]
        logger.info(f'{web_page_url}- read {df_html.shape[0]} records from the page: {web_page_url}')
    except Exception as e:
        logger.exception(f'{web_page_url} : - exception {e} encountered while reading data from the page: {web_page_url}')
        df_html = pd.DataFrame()
    return df_html

def extractd_data():
        parquet_file_name = "yellow_tripdata_2022-01.parquet"
        csv_file_name = "h9gi-nx95.csv"
        api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
        db_name = "movies.sqlite"
        table_name = "movies"
        web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
        matching_keyword = "by country"

        # Extract data from all source systems
        # Now these dataframes are available for loading data into eithter VSA table, PSA table or to be consumed in 
        # transfromation pipeline.

        df_parquet,df_csv,df_api,df_table,df_html = (source_data_from_parquet(parquet_file_name),
                                                    source_data_from_csv(csv_file_name),
                                                    source_data_from_api(api_endpoint),
                                                    source_data_from_table(db_name, table_name),
                                                    source_data_from_webpage(web_page_url,matching_keyword))
        return df_parquet,df_csv,df_api,df_table,df_html
