from sqlalchemy import create_engine

def load_data(df, table_name, redshift_conn_str):
    engine = create_engine(redshift_conn_str)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded data into {table_name}")
