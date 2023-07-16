# !/usr/bin/env python
# coding: utf-8
# # Creating a Data Extraction pipeline using Python

# Import modules
import json
import sqlite3

import certifi
import pandas as pd
import urllib3


# Function to Import Parquet Data
def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
    except Exception as e:
        df_parquet = pd.DataFrame()
    return df_parquet


# Function to Import CSV Data
def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
    except Exception as e:
        df_csv = pd.DataFrame()
    return df_csv


# Function to Import API Data
def source_data_from_api(api_endpoint):
    try:
        # Create a Pool manager that can be used to read the API response
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        api_response = http.request('GET', api_endpoint)
        apt_status = api_response.status

        if apt_status == 200: # Check if API is available to retrive the data
            data = json.loads(api_response.data.decode('utf-8'))
            df_api = pd.json_normalize(data)

        else: # Sometimes we get certificate error.
            df_api = pd.Dataframe()

    except Exception as e: #We should never silence certificate errors as this may cause a security threat.
        df_api = pd.DataFrame()
    return df_api


# Function to Import SQL lite Data
def source_data_from_table(db_name, table_name):
    try:
        # Read sqlite query results into a pandas DataFrame
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql(f"SELECT * from {table_name}", conn)

    except Exception as e:
        df_table = pd.DataFrame()
    return df_table


# Function to Import Webpage Data
def source_data_from_webpage(web_page_url, matching_keyword):
    try:
        # Read webpage table into a pandas DataFrame
        df_html = pd.read_html(web_page_url, match=matching_keyword)
        df_html = df_html[0]

    except Exception as e:
        df_html = pd.DataFrame()
    return df_html


# Function to Import All Data Sources
def extracted_data_files():
    """
    Extract data from all source systems for loading data into VSA(Virtual Staging Area)
    :return: example dataframes of relevant input data sources
    """

    # import all data types
    parquet_file_name = "data/yellow_tripdata_2022-01.parquet"
    csv_file_name = "data/h9gi-nx95.csv"
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    db_name = "data/movies.sqlite"
    table_name = "movies"
    web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    matching_keyword = "by country"

    df_parquet, df_csv, df_api, df_table, df_html = (source_data_from_parquet(parquet_file_name),

                                                     source_data_from_csv(csv_file_name),

                                                     source_data_from_api(api_endpoint),

                                                     source_data_from_table(db_name, table_name),

                                                     source_data_from_webpage(web_page_url, matching_keyword))

    return df_parquet, df_csv, df_api, df_table, df_html
