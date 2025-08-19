"""
Tests for data loader module.
"""
import os
import unittest
from unittest.mock import patch, MagicMock, mock_open

import pandas as pd
from pyspark.sql import SparkSession

from src.data.loader import DataLoader


class TestDataLoader(unittest.TestCase):
    """
    Test cases for DataLoader class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)

        # Configure mock DataFrame
        self.mock_df = MagicMock()
        self.mock_df.count.return_value = 10

        # Configure mock SparkSession
        self.mock_spark.read.csv.return_value = self.mock_df
        self.mock_spark.read.json.return_value = self.mock_df
        self.mock_spark.read.parquet.return_value = self.mock_df
        self.mock_spark.createDataFrame.return_value = self.mock_df

        # Configure mock sample
        self.mock_df.sample.return_value = self.mock_df

        # Create DataLoader instance
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock config
            mock_config.get.side_effect = lambda section, option, fallback=None: {
                ('DATA', 'data_url'): 'https://example.com/data.csv',
                ('DATA', 'data_dir'): 'test_data',
                ('DATA', 'data_format'): 'csv'
            }.get((section, option), fallback)

            mock_config.getint.side_effect = lambda section, option, fallback=None: {
                ('DATA', 'sample_size'): 100,
                ('DEFAULT', 'random_seed'): 42
            }.get((section, option), fallback)

            self.data_loader = DataLoader(self.mock_spark)

    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_init(self, mock_makedirs, mock_exists):
        """
        Test initialization.
        """
        # Configure mock
        mock_exists.return_value = False

        # Create DataLoader
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock config
            mock_config.get.side_effect = lambda section, option, fallback=None: {
                ('DATA', 'data_url'): 'https://example.com/data.csv',
                ('DATA', 'data_dir'): 'test_data',
                ('DATA', 'data_format'): 'csv'
            }.get((section, option), fallback)

            mock_config.getint.side_effect = lambda section, option, fallback=None: {
                ('DATA', 'sample_size'): 100
            }.get((section, option), fallback)

            loader = DataLoader(self.mock_spark)

            # Check if directory was created
            mock_makedirs.assert_called_once_with('test_data')

    @patch('os.path.exists')
    @patch('urllib.request.urlretrieve')
    def test_download_data_existing(self, mock_urlretrieve, mock_exists):
        """
        Test download_data when file already exists.
        """
        # Configure mock
        mock_exists.return_value = True

        # Download data
        filepath = self.data_loader.download_data()

        # Check that urlretrieve was not called
        mock_urlretrieve.assert_not_called()

        # Check returned filepath
        self.assertEqual(filepath, os.path.join('test_data', 'data.csv'))

    @patch('os.path.exists')
    @patch('urllib.request.urlretrieve')
    def test_download_data_new(self, mock_urlretrieve, mock_exists):
        """
        Test download_data when file does not exist.
        """
        # Configure mock
        mock_exists.return_value = False

        # Download data
        filepath = self.data_loader.download_data()

        # Check that urlretrieve was called
        mock_urlretrieve.assert_called_once()

        # Check returned filepath
        self.assertEqual(filepath, os.path.join('test_data', 'data.csv'))

    @patch('os.path.exists')
    def test_load_data_csv(self, mock_exists):
        """
        Test load_data with CSV file.
        """
        # Configure mock
        mock_exists.return_value = True

        # Load data
        df = self.data_loader.load_data('test.csv')

        # Check that read.csv was called
        self.mock_spark.read.csv.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(df, self.mock_df)

    @patch('os.path.exists')
    def test_load_data_json(self, mock_exists):
        """
        Test load_data with JSON file.
        """
        # Configure mock
        mock_exists.return_value = True

        # Load data
        df = self.data_loader.load_data('test.json')

        # Check that read.json was called
        self.mock_spark.read.json.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(df, self.mock_df)

    @patch('os.path.exists')
    def test_load_data_parquet(self, mock_exists):
        """
        Test load_data with Parquet file.
        """
        # Configure mock
        mock_exists.return_value = True

        # Load data
        df = self.data_loader.load_data('test.parquet')

        # Check that read.parquet was called
        self.mock_spark.read.parquet.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(df, self.mock_df)

    @patch('os.path.exists')
    def test_load_data_unsupported(self, mock_exists):
        """
        Test load_data with unsupported file format.
        """
        # Configure mock
        mock_exists.return_value = True

        # Load data with unsupported format
        with self.assertRaises(ValueError):
            self.data_loader.load_data('test.txt')

    @patch('pandas.DataFrame')
    def test_load_sample_data(self, mock_pandas_df):
        """
        Test load_sample_data.
        """
        # Configure mock
        mock_pandas_df.return_value = pd.DataFrame()

        # Load sample data
        df = self.data_loader.load_sample_data()

        # Check that createDataFrame was called
        self.mock_spark.createDataFrame.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(df, self.mock_df)


if __name__ == '__main__':
    unittest.main()
