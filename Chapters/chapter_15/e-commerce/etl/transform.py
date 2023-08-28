# import modules
import pandas as pd

# clean data
def clean_data(df: object) -> object:
    """
       ECommerce Transformation Function in Python with Error Handling
       :param df: pandas dataframe, extracted ecommerce data 1
       :output: pandas dataframe, transformed data
    """

    # drop duplicate rows
    df = df.drop_duplicates()

    # replace missing values in numeric columns with the mean
    df = df.fillna(df.mean(), inplace=True)

    # drop rows with any remaining null values
    df = df.dropna()

    return df

# transform data
def transform_data(orders_df: object, products_df: object, customers_df: object) -> object:
    """
       ECommerce Transformation Function in Python with Error Handling
       :param orders_df: pandas dataframe, extracted ecommerce data 1
       :param products_df: pandas dataframe, extracted ecommerce data 1
       :param customers_df: pandas dataframe, extracted ecommerce data 1
       :output: pandas dataframe, transformed data
    """

    # Merge the orders and products DataFrames
    product_orders_df = pd.merge(orders_df, products_df, on='product_id')

    # Calculate the total price for each order
    product_orders_df['total_price'] = product_orders_df['quantity'] * product_orders_df['price']

    # Merge the resulting DataFrame with the customers DataFrame
    ecommerce_df = pd.merge(product_orders_df, customers_df, on='customer_id')

    return ecommerce_df