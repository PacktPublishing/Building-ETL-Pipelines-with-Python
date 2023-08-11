import pandas as pd

def transform_data(local_path):
    df = pd.read_csv(local_path)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df['duration'] = df['end_date'] - df['start_date']
    df.loc[df['end_date'].isna(), 'duration'] = pd.Timestamp.today() - df['start_date']
    df['duration'] = df['duration'].dt.days
    print(f"Transformed data from {local_path}")
    return df
