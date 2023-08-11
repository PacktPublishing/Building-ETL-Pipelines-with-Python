# Import Methods
# <-- your code here -->

def run_etl_pipeline(bucket_name, file_key, local_path, table_name, redshift_conn_str):
    extract_data(bucket_name, file_key, local_path)
    df = transform_data(local_path)
    load_data(df, table_name, redshift_conn_str)
