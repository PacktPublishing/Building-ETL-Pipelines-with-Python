# create a command-line runnable pipeline
from etl.extract import extract_data
from etl.transform import clean_data, transform_data
import etl.load as load

import yaml

# import pipeline configuration
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


def run_pipeline():
    # Step 1: Extract data
    orders_df = extract_data(config_data['orders_filepath'])
    products_df = extract_data(config_data['products_filepath'])
    customers_df = extract_data(config_data['customers_filepath'])

    # Step 2: Transform data
    ## clean data pre-transform
    clean_orders_df = clean_data(df=orders_df)
    clean_products_df = clean_data(df=products_df)
    clean_customers_df = clean_data(df=customers_df)

    ## merge data to create ecommerce data
    ecommerce_df = transform_data(orders_df=clean_orders_df,
                                  products_df=clean_products_df,
                                  customers_df=clean_customers_df)

    # Step 3: Load data
    load.load_data(df=ecommerce_df)


if __name__ == "__main__":
    run_pipeline()
