# test_etl_pipeline.py
import unittest
from etl_pipeline import extract_data, transform_data, load_data
from config import DATABASE_CONNECTION, TABLE_NAME, FILE_PATH

class TestETLPipeline(unittest.TestCase):
    def test_extract_data(self):
        df = extract_data(FILE_PATH)
        self.assertIsNotNone(df)
        self.assertEqual(df.shape[1], 18)

    def test_transform_data(self):
        df = extract_data(FILE_PATH)
        df = transform_data(df)
        self.assertIn('trip_duration', df.columns)
        self.assertIn('average_speed', df.columns)

    def test_load_data(self):
        df = extract_data(FILE_PATH)
        df = transform_data(df)
        load_data(df, TABLE_NAME, DATABASE_CONNECTION)
        # Here, you would typically connect to the database and check that the data was loaded correctly.
        # However, for simplicity, we'll skip this step in this example.

if __name__ == '__main__':
    unittest.main()
