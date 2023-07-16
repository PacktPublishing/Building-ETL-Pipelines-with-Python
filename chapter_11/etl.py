# import modules
# import pandas as pd
import numpy as np

def extract():
    """
    Generates a DataFrame with random data.
    """
    date_range = pd.date_range(start='2023-01-01', end='2023-06-30')
    data = pd.DataFrame({
        'date': date_range,
        'sales': np.random.randint(1, 100, size=len(date_range)),
        'cost': np.random.randint(1, 50, size=len(date_range))
    })
    return data

def transform(data):
    """
    Transforms the data by calculating profit and sorting by date.
    """
    data['profit'] = data['sales'] - data['cost']
    data = data.sort_values('date')
    return data

def load(data):
    """
    Writes the data into a CSV file.
    """
    data.to_csv('transformed_data.csv', index=False)

def main():
    """
    Orchestrates the ETL process.
    """
    # Extract
    data = extract()

    # Transform
    data = transform(data)

    # Load
    load(data)

if __name__ == "__main__":
    main()

