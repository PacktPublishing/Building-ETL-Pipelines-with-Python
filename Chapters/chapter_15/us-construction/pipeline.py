# Import Methods
# <-- your code here -->

# Lazy - Import of Paths
# TODO: Create config.yaml file
s3_BUCKET_NAME = "your_s3_bucket_name"
CMDW_FILE_KEY = "your_filename"
LOCAL_PATH = "data/us_construction_extract.csv"
REDSHIFT_TABLE = "your_redshift_table"
REDSHIFT_CONN_STR = "your_redshift_conn_str"

def run_etl_pipeline(bucket_name, file_key, local_path, table_name, redshift_conn_str):
    extract_data(bucket_name, file_key, local_path)
    df = transform_data(local_path)
    load_data(df, table_name, redshift_conn_str)

if __name__ == '__main__':
    run_etl_pipeline(bucket_name=s3_BUCKET_NAME,
                     file_key=CMDW_FILE_KEY,
                     local_path=LOCAL_PATH,
                     table_name=REDSHIFT_TABLE,
                     redshift_conn_str=REDSHIFT_CONN_STR)
